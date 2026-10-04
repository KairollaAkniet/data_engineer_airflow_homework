param(
    [ValidateSet('test', 'verify', 'init', 'webserver', 'scheduler')]
    [string]$Mode = 'test'
)

$ErrorActionPreference = 'Stop'
$taskLinuxProject = (& wsl -d Ubuntu -- wslpath -a $PSScriptRoot).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the project path in WSL' }
& wsl -d Ubuntu -- bash "$taskLinuxProject/scripts/run.sh" $Mode
if ($LASTEXITCODE -ne 0) { throw "Airflow command exited with code $LASTEXITCODE" }
