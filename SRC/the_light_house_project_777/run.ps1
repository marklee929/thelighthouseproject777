param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Rest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
if (-not $Rest) { $Rest = @() }

$ngrokMode = $false
$xPostTestMode = $false
if ($Rest.Count -gt 0) {
  $filteredArgs = New-Object System.Collections.Generic.List[string]
  foreach ($arg in $Rest) {
    if ($arg -eq "--ikeolbot" -or $arg -eq "-i") {
      continue
    }
    if ($arg -eq "--ngrok" -or $arg -eq "-n") {
      $ngrokMode = $true
      continue
    }
    if ($arg -eq "--x-post-test" -or $arg -eq "-x") {
      $xPostTestMode = $true
      continue
    }
    $filteredArgs.Add($arg)
  }
  $Rest = $filteredArgs.ToArray()
}

$projectRoot = Resolve-Path $PSScriptRoot
Set-Location $projectRoot

$workspaceRoot = Resolve-Path (Join-Path $projectRoot "..\..")
$venvPath = Join-Path $workspaceRoot ".venv_lh777"
$venvPython = Join-Path $venvPath "Scripts\python.exe"
$venvActivate = Join-Path $venvPath "Scripts\Activate.ps1"

if (-not (Test-Path $venvPython)) {
  Write-Host "[INFO] Creating virtual environment..."
  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "[ERR] Python not found. Install Python 3.8+"
    exit 1
  }
  $pythonVersion = & python --version
  Write-Host "[INFO] Using $pythonVersion"
  & python -m venv $venvPath
  if (-not $?) {
    Write-Host "[ERR] Failed to create venv"
    exit 1
  }
}

Write-Host "[INFO] Activating venv..."
& $venvActivate
$venvPython = Join-Path $venvPath "Scripts\python.exe"

$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'
$env:OLLAMA_HOST = '0.0.0.0'
$env:OLLAMA_BASE_URL = 'http://127.0.0.1:11434'

$envPath = Join-Path $projectRoot ".env"
if (Test-Path $envPath) {
  Get-Content $envPath | ForEach-Object {
    if ($_ -match '^\s*#') { return }
    if ($_ -match '^\s*$') { return }
    $parts = $_ -split '=', 2
    if ($parts.Length -eq 2) {
      Set-Item -Path "env:$($parts[0].Trim())" -Value $parts[1].Trim()
    }
  }
}

function Start-NgrokOAuthTunnel {
  param(
    [string]$ProjectRoot,
    [int]$LocalPort = 8080
  )

  Write-Host "[INFO] ngrok OAuth mode enabled. Starting ngrok tunnel for port $LocalPort..."
  $configuredPublicUrl = [string]$env:IKEOLBOT_NGROK_URL
  if ($null -eq $configuredPublicUrl) { $configuredPublicUrl = "" }
  $configuredPublicUrl = $configuredPublicUrl.Trim()
  if (-not $configuredPublicUrl) {
    $configuredDomain = [string]$env:IKEOLBOT_NGROK_DOMAIN
    if ($null -eq $configuredDomain) { $configuredDomain = "" }
    $configuredDomain = $configuredDomain.Trim()
    if ($configuredDomain) {
      if ($configuredDomain -match '^https?://') {
        $configuredPublicUrl = $configuredDomain.TrimEnd('/')
      }
      else {
        $configuredPublicUrl = "https://$($configuredDomain.TrimEnd('/'))"
      }
    }
  }
  if ($configuredPublicUrl) {
    Write-Host "[INFO] Configured ngrok URL: $configuredPublicUrl"
  }
  $existingApi = $null
  try {
    $existingApi = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2
  }
  catch {
    $existingApi = $null
  }

  $publicUrl = $null
  if ($existingApi -and $existingApi.tunnels) {
    foreach ($tunnel in $existingApi.tunnels) {
      if ($tunnel.public_url -and $tunnel.public_url -match '^https://') {
        $publicUrl = [string]$tunnel.public_url
        break
      }
    }
  }

  if (-not $publicUrl) {
    try {
      $ngrokArgs = @("/c", "ngrok", "http")
      if ($configuredPublicUrl) {
        $ngrokArgs += @("--url", $configuredPublicUrl)
      }
      $ngrokArgs += @("$LocalPort")
      Start-Process -FilePath "cmd.exe" -ArgumentList $ngrokArgs -WindowStyle Minimized | Out-Null
    }
    catch {
      throw "Failed to start ngrok via cmd.exe: $($_.Exception.Message)"
    }

    $maxAttempts = 20
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
      Start-Sleep -Milliseconds 750
      try {
        $api = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2
        foreach ($tunnel in $api.tunnels) {
          if (
            $tunnel.public_url -and
            $tunnel.public_url -match '^https://' -and
            (
              (-not $configuredPublicUrl) -or
              ([string]$tunnel.public_url).TrimEnd('/') -eq $configuredPublicUrl.TrimEnd('/')
            )
          ) {
            $publicUrl = [string]$tunnel.public_url
            break
          }
        }
      }
      catch {
        $publicUrl = $null
      }
      if ($publicUrl) { break }
    }
  }

  if (-not $publicUrl) {
    throw "ngrok public HTTPS URL could not be resolved from http://127.0.0.1:4040/api/tunnels"
  }

  $publicUrl = $publicUrl.TrimEnd('/')
  $env:X_CALLBACK_URL = "$publicUrl/api/crew/social/x/oauth/callback"
  $env:IKEOLBOT_PUBLIC_URL = $publicUrl
  Write-Host "[INFO] ngrok public URL: $publicUrl"
  Write-Host "[INFO] X_CALLBACK_URL override: $($env:X_CALLBACK_URL)"
  Write-Host "[INFO] Open the_light_house_project+777 via: $publicUrl/"
}

$crewaiConfigDir = Join-Path $projectRoot ".config/crewai"
$crewaiConfigPath = Join-Path $crewaiConfigDir "settings.json"
if (-not (Test-Path $crewaiConfigDir)) {
  New-Item -ItemType Directory -Path $crewaiConfigDir -Force | Out-Null
}
if (-not (Test-Path $crewaiConfigPath)) {
  "{}" | Out-File -Encoding UTF8 -FilePath $crewaiConfigPath
}
$env:CREWAI_CONFIG_PATH = $crewaiConfigPath
$env:XDG_CONFIG_HOME = (Join-Path $projectRoot ".config")

if ($ngrokMode) {
  Start-NgrokOAuthTunnel -ProjectRoot $projectRoot -LocalPort 8080
}

if (Test-Path '.\requirements.txt') {
  Write-Host "[INFO] Installing dependencies..."
  & $venvPython -m pip install -r .\requirements.txt
  Write-Host "[INFO] Pinning Flask / Werkzeug versions..."
  & $venvPython -m pip install "flask==2.2.5" "werkzeug==2.2.3"
}

if ($xPostTestMode) {
  $testScript = Join-Path $projectRoot "test_x_post.py"
  if (-not (Test-Path $testScript)) {
    throw "X post test script not found: $testScript"
  }
  Write-Host "[INFO] Running one-shot X OAuth1 post test..."
  & $venvPython $testScript @Rest
  exit $LASTEXITCODE
}

function Get-PortPids {
  param([int]$Port)
  try {
    $connections = netstat -ano | Select-String ":$Port\s"
    $pids = @()
    foreach ($line in $connections) {
      if ($line.Line -match '\s+(\d+)\s*$') {
        $pids += [int]$Matches[1]
      }
    }
    return $pids | Select-Object -Unique
  }
  catch {
    return @()
  }
}

$ollamaPort = 11434
$pidList = @(Get-PortPids -Port $ollamaPort)

if ($pidList.Count -gt 0) {
  Write-Warning "WARNING: Port $ollamaPort is in use by PID(s): $($pidList -join ', ')"
}
else {
  Write-Host "[INFO] Ollama not detected. Starting Ollama..."
  $startOllamaScript = Join-Path $projectRoot "startOllama.bat"

  if (Test-Path $startOllamaScript) {
    Start-Process -FilePath $startOllamaScript -WindowStyle Hidden
    Write-Host "[INFO] Waiting for Ollama to start..."
    Start-Sleep -Seconds 5
  }
  else {
    Write-Warning "[WARN] startOllama.bat not found at: $startOllamaScript"
    Write-Warning "[WARN] Please start Ollama manually."
  }
}

$modelsToCheck = @("qwen3:8b", "qwen3-coder", "deepseek-r1:8b")
foreach ($model in $modelsToCheck) {
  Write-Host "[INFO] Checking for model $model..."
  $result = & ollama list 2>$null | Select-String -Pattern $model -Quiet
  if ($result) {
    Write-Host "[INFO] Model $model is available."
  }
  else {
    Write-Host "[WARN] Model $model is missing."
  }
}

$tempDir = Join-Path $projectRoot "temp"
if (-not (Test-Path $tempDir)) {
  New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
}

$preferredAppPortRaw = [string]$env:APP_PORT
if ([string]::IsNullOrWhiteSpace($preferredAppPortRaw)) {
  $preferredAppPortRaw = "8080"
}
try {
  $preferredAppPort = [int]$preferredAppPortRaw
}
catch {
  $preferredAppPort = 8080
}
$appPort = $preferredAppPort
for ($offset = 0; $offset -lt 20; $offset++) {
  $candidate = $preferredAppPort + $offset
  $ownerPids = @(Get-PortPids -Port $candidate)
  if ($ownerPids.Count -eq 0) {
    $appPort = $candidate
    break
  }
}
if ($appPort -ne $preferredAppPort) {
  Write-Warning "[WARN] Port $preferredAppPort is already in use. Switching to port $appPort."
}
$env:APP_PORT = [string]$appPort
$env:PORT = [string]$appPort

$mainPy = Join-Path $projectRoot "main.py"
$logFile = Join-Path $tempDir "crew_app.log"
$errFile = Join-Path $tempDir "crew_app_err.log"

$pythonArgs = @($mainPy) + $Rest

Write-Host "[INFO] Starting application... (log: $logFile)"
Write-Host "[INFO] Python args: $($pythonArgs -join ' ')"
Write-Host "[INFO] App URL: http://127.0.0.1:$appPort"

$pathEntries = @(
  [System.Environment]::GetEnvironmentVariables().GetEnumerator() |
    Where-Object { [string]$_.Key -ieq 'PATH' }
)
if ($pathEntries.Count -gt 1) {
  $canonicalPathEntry = $pathEntries | Where-Object { [string]$_.Key -ceq 'Path' } | Select-Object -First 1
  if ($null -eq $canonicalPathEntry) {
    $canonicalPathEntry = $pathEntries | Select-Object -First 1
  }

  # PowerShell Start-Process on Windows treats PATH/Path as the same key.
  Set-Item -Path Env:Path -Value ([string]$canonicalPathEntry.Value)
  Remove-Item -Path Env:PATH -ErrorAction SilentlyContinue
}

$proc = Start-Process `
  -FilePath $venvPython `
  -ArgumentList $pythonArgs `
  -NoNewWindow `
  -PassThru `
  -RedirectStandardOutput $logFile `
  -RedirectStandardError $errFile

Start-Sleep -Seconds 2
if ($proc.HasExited) {
  Write-Warning "[INFO] Server exited with code $($proc.ExitCode)"
  Write-Host "[INFO] Error log:"
  Get-Content $errFile -ErrorAction SilentlyContinue
  exit $proc.ExitCode
}
else {
  Write-Host "[INFO] Server running (PID: $($proc.Id)). Press Ctrl+C to stop."
  Wait-Process -Id $proc.Id
}
