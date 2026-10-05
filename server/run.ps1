# Build (if needed) and start Own-Redis-Java.
#   .\run.ps1                 -> build if the jar is missing, then run
#   .\run.ps1 -Rebuild        -> always rebuild first
#   .\run.ps1 -- --port 6380  -> pass everything after -- to the server
param(
    [switch]$Rebuild,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ServerArgs
)

$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

$jar = Join-Path $PSScriptRoot 'target\own-redis-java.jar'

if ($Rebuild -or -not (Test-Path $jar)) {
    Write-Host 'Building...' -ForegroundColor Cyan
    mvn -q package
    if ($LASTEXITCODE -ne 0) { throw 'build failed' }
}

java -jar $jar @ServerArgs
