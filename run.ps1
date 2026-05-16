$ErrorActionPreference = 'Stop'
$Root = $PSScriptRoot
$py = & (Join-Path $Root 'scripts\Resolve-ProjectPython.ps1')
Set-Location -LiteralPath $Root
& $py (Join-Path $Root 'run.py')
