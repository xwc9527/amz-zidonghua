param(
    [Parameter(Mandatory=$true)][string]$Python,
    [Parameter(Mandatory=$true)][string]$ModelRoot,
    [string]$DependencyPath = '',
    [string]$PgEnvScript = '',
    [int]$WebPort = 8093
)
$ErrorActionPreference = 'Stop'
if ($PgEnvScript) { . $PgEnvScript }
if (-not $env:PG_DSN) { throw 'Load the existing PG_DSN configuration first.' }
$env:TESTING = '0'
$env:QAC_LOCAL_TRANSLATOR_URL = 'http://127.0.0.1:18191'
$env:PYTHONPATH = "$PSScriptRoot;$DependencyPath"
$runtime = Join-Path $ModelRoot 'llama-b12032/llama-server.exe'
$model = Join-Path $ModelRoot 'Qwen3-4B-Instruct-2507-Q4_K_M.gguf'
if (-not (Test-Path -LiteralPath $runtime) -or -not (Test-Path -LiteralPath $model)) {
    throw 'Run setup_qac_translation.py before starting the research page.'
}
$logRoot = Join-Path $ModelRoot 'logs'
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
function Test-LocalHttp([string]$Url) {
    try { return (Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 2).StatusCode -eq 200 }
    catch { return $false }
}
if (-not (Test-LocalHttp 'http://127.0.0.1:18191/health')) {
    $modelArgs = "-m `"$model`" --host 127.0.0.1 --port 18191 --alias qac-qwen3-4b-2507 -c 16384 -ngl 99 -np 4 -fa on --jinja --no-webui"
    Start-Process -FilePath $runtime -ArgumentList $modelArgs -WindowStyle Hidden -WorkingDirectory $ModelRoot -RedirectStandardOutput (Join-Path $logRoot 'model-out.log') -RedirectStandardError (Join-Path $logRoot 'model-error.log') | Out-Null
    $deadline = (Get-Date).AddSeconds(90)
    while (-not (Test-LocalHttp 'http://127.0.0.1:18191/health')) {
        if ((Get-Date) -gt $deadline) { throw 'Local model startup failed; inspect the model log.' }
        Start-Sleep -Seconds 1
    }
}
if (-not (Test-LocalHttp "http://127.0.0.1:$WebPort/research")) {
    $webArgs = "-m uvicorn amazonqac_search:app --host 127.0.0.1 --port $WebPort --no-access-log"
    Start-Process -FilePath $Python -ArgumentList $webArgs -WindowStyle Hidden -WorkingDirectory $PSScriptRoot -RedirectStandardOutput (Join-Path $logRoot 'web-out.log') -RedirectStandardError (Join-Path $logRoot 'web-error.log') | Out-Null
    $deadline = (Get-Date).AddSeconds(30)
    while (-not (Test-LocalHttp "http://127.0.0.1:$WebPort/research")) {
        if ((Get-Date) -gt $deadline) { throw 'Research page startup failed; inspect the web log.' }
        Start-Sleep -Seconds 1
    }
}
Write-Host "AmazonQAC research page: http://127.0.0.1:$WebPort/research"
Write-Host 'The existing PostgreSQL instance must remain running. Logs and model files are persistent.'
