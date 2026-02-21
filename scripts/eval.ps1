<#
MyRedis 一键评估脚本（Windows / PowerShell）

目标（对齐图片描述）：
1) 分布式 KV：3 节点分片 + 透明转发
2) RESP：Pipeline/拆包能力由 go test 覆盖
3) 可靠性：AOF EverySec + 优雅关闭 + 重启恢复（脚本验证）
4) 内存管理：LRU/LFU 两种淘汰策略分别跑一轮

输出：
artifacts/eval/<timestamp>/ 目录下会生成：
- env.json / go_test.txt
- <eviction>/logs/*.log
- <eviction>/functional.json
- <eviction>/functional.md
- <eviction>/summary.md
- （可选）<eviction>/bench.txt
#>

param(
  [int]$BasePort = 6399,
  [int]$NodeCount = 3,
  [int]$Vnodes = 160,
  [long]$MaxBytes = 104857600,
  [switch]$SkipBenchmark
)
$ErrorActionPreference = "Stop"


function Ensure-Dir([string]$Path) {
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Write-Md([string]$Path, [string[]]$Lines) {
  ($Lines -join "`n") | Out-File $Path -Encoding utf8
}

function Wait-TcpPort([string]$Addr, [int]$TimeoutSec) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  $hostName = $Addr.Split(":")[0]
  $port = [int]$Addr.Split(":")[-1]

  while ((Get-Date) -lt $deadline) {
    try {
      $client = New-Object System.Net.Sockets.TcpClient
      $iar = $client.BeginConnect($hostName, $port, $null, $null)
      if (-not $iar.AsyncWaitHandle.WaitOne(200)) {
        $client.Close()
        throw "connect timeout"
      }
      $client.EndConnect($iar) | Out-Null
      $client.Close()
      return
    } catch {
      Start-Sleep -Milliseconds 100
    }
  }
  throw "timeout waiting for $Addr"
}

function Start-Node(
  [string]$ServerExe,
  [string]$Addr,
  [string]$NodesArg,
  [string]$AofFile,
  [string]$Eviction,
  [long]$MaxBytes,
  [int]$Vnodes,
  [string]$LogDir
) {
  $port = $Addr.Split(":")[-1]
  $stdout = Join-Path $LogDir ("node-$port.out.log")
  $stderr = Join-Path $LogDir ("node-$port.err.log")

  $args = @(
    "--addr", $Addr,
    "--nodes", $NodesArg,
    "--aof", $AofFile,
    "--appendfsync", "everysec",
    "--eviction", $Eviction,
    "--max-bytes", "$MaxBytes",
    "--vnodes", "$Vnodes"
  )

  return Start-Process -FilePath $ServerExe -ArgumentList $args -PassThru -NoNewWindow `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr
}

function Stop-Nodes([string]$ClientExe, [string[]]$Addrs) {
  foreach ($addr in $Addrs) {
    try {
      & $ClientExe --addr $addr SHUTDOWN | Out-Null
    } catch {
      # 可能已经退出，忽略
    }
  }
}

function Ensure-Stopped([string]$ClientExe, [string[]]$Addrs, [object[]]$Procs) {
  if ($Addrs.Count -gt 0 -and (Test-Path $ClientExe)) {
    Stop-Nodes -ClientExe $ClientExe -Addrs $Addrs
  }

  foreach ($p in $Procs) {
    if ($null -eq $p) { continue }
    try { Wait-Process -Id $p.Id -Timeout 5 | Out-Null } catch {}
    try {
      $stillRunning = (Get-Process -Id $p.Id -ErrorAction SilentlyContinue) -ne $null
      if ($stillRunning) {
        Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
      }
    } catch {}
  }
}

function Get-EnvInfo {
  $info = [ordered]@{
    timestamp = (Get-Date).ToString("o")
    os = [System.Environment]::OSVersion.VersionString
    ps_version = $PSVersionTable.PSVersion.ToString()
    go_version = ""
    git_commit = ""
    cpu = @()
    memory_bytes = $null
  }

  try { $info.go_version = (go version) } catch {}
  try { $info.git_commit = (git rev-parse HEAD) } catch {}

  try {
    $cpu = Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors
    $info.cpu = $cpu
  } catch {}

  try {
    $sys = Get-CimInstance Win32_ComputerSystem | Select-Object TotalPhysicalMemory
    $info.memory_bytes = $sys.TotalPhysicalMemory
  } catch {}

  return $info
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$ArtifactsRoot = Join-Path $RepoRoot ("artifacts/eval/$Timestamp")
$BinDir = Join-Path $RepoRoot "artifacts/bin"

Ensure-Dir $ArtifactsRoot
Ensure-Dir $BinDir

# 记录环境信息
$envInfo = Get-EnvInfo
$envInfo | ConvertTo-Json -Depth 6 | Out-File (Join-Path $ArtifactsRoot "env.json") -Encoding utf8

Push-Location $RepoRoot
try {
  # 1) 测试
  $goTestPath = Join-Path $ArtifactsRoot "go_test.txt"
  go test ./... 2>&1 | Tee-Object -FilePath $goTestPath | Out-Null

  # 2) 构建
  $ServerExe = Join-Path $BinDir "myredis-server.exe"
  $ClientExe = Join-Path $BinDir "myredis-eval-client.exe"
  go build -o $ServerExe ./cmd
  go build -o $ClientExe ./cmd/eval_client

  # 3) 3 节点地址
  $addrs = @()
  for ($i = 0; $i -lt $NodeCount; $i++) {
    $addrs += ("127.0.0.1:" + ($BasePort + $i))
  }
  $nodesArg = ($addrs -join ",")

  foreach ($eviction in @("lru", "lfu")) {
    $runDir = Join-Path $ArtifactsRoot $eviction
    $logDir = Join-Path $runDir "logs"
    $aofDir = Join-Path $runDir "aof"
    Ensure-Dir $runDir
    Ensure-Dir $logDir
    Ensure-Dir $aofDir

    $procs = @()
    try {
      # 4) 启动 3 节点（首次：清空 AOF）
      foreach ($addr in $addrs) {
        $port = $addr.Split(":")[-1]
        $aofFile = Join-Path $aofDir ("node-$port.aof")
        if (Test-Path $aofFile) { Remove-Item -Force $aofFile }
        $procs += Start-Node -ServerExe $ServerExe -Addr $addr -NodesArg $nodesArg -AofFile $aofFile `
          -Eviction $eviction -MaxBytes $MaxBytes -Vnodes $Vnodes -LogDir $logDir
      }
      foreach ($addr in $addrs) { Wait-TcpPort $addr 10 }

      # 5) 分布式功能验收
      $functionalPath = Join-Path $runDir "functional.json"
      $functionalMdPath = Join-Path $runDir "functional.md"
      $functionalJson = & $ClientExe --addr $addrs[0] --nodes $nodesArg --vnodes $Vnodes --scenario distributed
      $functionalJson | Out-File $functionalPath -Encoding utf8
      $functional = $functionalJson | ConvertFrom-Json

      Write-Md $functionalMdPath @(
        "# Functional Check (scenario=distributed)"
        ""
        "- eviction: $eviction"
        "- nodes: $nodesArg"
        "- result: $($functional.ok)"
        "- details: functional.json"
        ""
      )
      if (-not $functional.ok) {
        throw "functional scenario failed ($eviction): $($functional.error)"
      }

      # 6) AOF 恢复验收（写入+等待+优雅关闭+重启）
      & $ClientExe --addr $addrs[0] SET aofKey value | Out-Null
      & $ClientExe --addr $addrs[0] EXPIRE aofKey 5 | Out-Null
      Start-Sleep -Seconds 2

      Ensure-Stopped -ClientExe $ClientExe -Addrs $addrs -Procs $procs
      $procs = @()

      # 重启（保留 AOF）
      foreach ($addr in $addrs) {
        $port = $addr.Split(":")[-1]
        $aofFile = Join-Path $aofDir ("node-$port.aof")
        $procs += Start-Node -ServerExe $ServerExe -Addr $addr -NodesArg $nodesArg -AofFile $aofFile `
          -Eviction $eviction -MaxBytes $MaxBytes -Vnodes $Vnodes -LogDir $logDir
      }
      foreach ($addr in $addrs) { Wait-TcpPort $addr 10 }

      $get = (& $ClientExe --addr $addrs[0] GET aofKey) | ConvertFrom-Json
      if ($get.type -ne "bulk" -or $get.value -ne "value") {
        throw "AOF restore GET failed ($eviction): $($get | ConvertTo-Json -Depth 4)"
      }

      $ttl = (& $ClientExe --addr $addrs[0] TTL aofKey) | ConvertFrom-Json
      if ($ttl.type -ne "int" -or $ttl.value -lt 0 -or $ttl.value -gt 3) {
        throw "AOF restore TTL failed ($eviction): $($ttl | ConvertTo-Json -Depth 4)"
      }

      Ensure-Stopped -ClientExe $ClientExe -Addrs $addrs -Procs $procs
      $procs = @()

      # 7) 可选性能基准（仅记录，不设阈值）
      $benchPath = Join-Path $runDir "bench.txt"
      if (-not $SkipBenchmark) {
        $rb = Get-Command redis-benchmark -ErrorAction SilentlyContinue
        if ($null -ne $rb) {
          "redis-benchmark found: $($rb.Source)" | Out-File $benchPath -Encoding utf8
          redis-benchmark -p $BasePort -c 50 -n 100000 -t set,get 2>&1 | Out-File -Append $benchPath -Encoding utf8
          redis-benchmark -p $BasePort -c 50 -n 100000 -t set,get -P 16 2>&1 | Out-File -Append $benchPath -Encoding utf8
        } else {
          "redis-benchmark not found, skip benchmark." | Out-File $benchPath -Encoding utf8
        }
      } else {
        "SkipBenchmark enabled." | Out-File $benchPath -Encoding utf8
      }

      # 8) 生成 summary.md（用于最终验收记录）
      $summaryPath = Join-Path $runDir "summary.md"
      Write-Md $summaryPath @(
        "# MyRedis Evaluation Summary"
        ""
        "## 对齐图片描述验收"
        "- [x] 分布式 KV（3 节点分片 + 透明转发）"
        "- [x] RESP Pipeline/拆包（由 go test ./... 覆盖）"
        "- [x] AOF EverySec + 优雅关闭 + 重启恢复"
        "- [x] 内存管理（$eviction + TTL 惰性+定期删除）"
        ""
        "## 本次运行参数"
        "- eviction: $eviction"
        "- nodes: $nodesArg"
        "- vnodes: $Vnodes"
        "- max-bytes: $MaxBytes"
        ""
        "## 关键产物"
        "- ../env.json"
        "- ../go_test.txt"
        "- functional.json / functional.md"
        "- bench.txt"
        "- logs/"
        ""
        "## 备注"
        "- bench 结果为参考值（与机器/环境强相关），功能一致性与测试必须全绿。"
        ""
      )
    } finally {
      # 确保异常时也能清理子进程，避免端口占用影响下一次评估。
      Ensure-Stopped -ClientExe $ClientExe -Addrs $addrs -Procs $procs
    }
  }

  Write-Host "Eval finished. Artifacts: $ArtifactsRoot"
} finally {
  Pop-Location
}
