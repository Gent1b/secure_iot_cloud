<#
1_pull_updates.ps1
Pulls the latest code from GitHub on all VMs and (optionally) deploys the local .env.

Usage:
    .\scripts\1_pull_updates.ps1
    .\scripts\1_pull_updates.ps1 -Branch edge-branch-refined
    .\scripts\1_pull_updates.ps1 -SkipEnv
#>

[CmdletBinding()]
param(
        [string[]]$Hosts = @("mqtt", "monitor", "cloud", "devices"),
        [string]$RepoDir = "/root/secure_iot_cloud",
        [string]$Branch = "edge-branch-refined",
    [switch]$SkipEnv,
        [string]$EnvPath = ".env",
        [string]$RemoteEnvDir = "/opt/iot"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " 1. GIT PULL (All Nodes)"
Write-Host "========================================" -ForegroundColor Cyan

foreach ($H in $Hosts) {
    Write-Host ">>> Updating $H..." -ForegroundColor Yellow
    
    # Reset to remote branch (handles force-pushes and divergent branches)
    ssh $H "cd $RepoDir; git fetch origin; git checkout $Branch; git reset --hard origin/$Branch"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK  $H updated." -ForegroundColor Green
    } else {
        Write-Host "ERR $H update failed." -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " 2. DEPLOY ENVIRONMENT FILE"
Write-Host "========================================" -ForegroundColor Cyan

if ($SkipEnv) {
    Write-Host "(Skipping .env deploy - requested via -SkipEnv)" -ForegroundColor DarkGray
    return
}

if (-not (Test-Path -LiteralPath $EnvPath)) {
    Write-Host "ERR Env file not found: $EnvPath" -ForegroundColor Red
    Write-Host "    Tip: create it locally or pass -EnvPath PATH" -ForegroundColor DarkGray
    exit 1
}

# Deploy .env from local machine to ALL nodes via SCP
Write-Host ">>> Deploying $EnvPath to all nodes..." -ForegroundColor Yellow
foreach ($H in $Hosts) {
    ssh $H "mkdir -p $RemoteEnvDir"
    scp $EnvPath ${H}:$RemoteEnvDir/.env

    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK  $H .env deployed." -ForegroundColor Green
    } else {
        Write-Host "ERR $H .env deploy failed." -ForegroundColor Red
    }
}

Write-Host "OK  All nodes updated; env deploy finished." -ForegroundColor Green
