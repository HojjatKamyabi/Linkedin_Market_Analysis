param (
    [switch]$Force
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$envPath = Join-Path $repoRoot ".env"
$envExamplePath = Join-Path $repoRoot ".env.example"

if (-not $Force -and (Test-Path $envPath)) {
    Write-Error ".env already exists. Use -Force to overwrite."
    exit 1
}

Copy-Item $envExamplePath $envPath -Force

$chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
$dbPass = -join ((1..24) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })

$bytes = New-Object byte[] 32
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $rng.GetBytes($bytes)
} finally {
    $rng.Dispose()
}
$fernetKey = [Convert]::ToBase64String($bytes) -replace '\+', '-' -replace '/', '_'

$adminPass = -join ((1..24) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })

$envContent = Get-Content $envPath
$envContent = $envContent -replace '^DB_PASSWORD=$', "DB_PASSWORD=$dbPass"
$envContent = $envContent -replace '^DB_PASSWORD_URI=$', "DB_PASSWORD_URI=$dbPass"
$envContent = $envContent -replace '^FERNET_KEY=$', "FERNET_KEY=$fernetKey"
$envContent = $envContent -replace '^AIRFLOW_ADMIN_PASSWORD=$', "AIRFLOW_ADMIN_PASSWORD=$adminPass"

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllLines($envPath, $envContent, $utf8NoBom)

New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "data/raw") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "data/processed") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "airflow/logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $repoRoot "airflow/plugins") | Out-Null

Write-Host "Created .env"
Write-Host "Generated local database password: true"
Write-Host "Generated Fernet key: true"
Write-Host "Generated Airflow admin password: true"
Write-Host "LLM_API_KEY still requires user input"
