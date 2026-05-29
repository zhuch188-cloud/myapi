<#
.SYNOPSIS
  将本仓库代码同步到 Git 提交目录（默认 C:\Users\zhuxd\myapi），保留目标端 .git。

.DESCRIPTION
  开发在 financial-product-bs（工作区）完成；推送到 GitHub / Render 前运行本脚本，
  再在 myapi 目录内 git add / commit / push。

.EXAMPLE
  .\scripts\Sync-ToMyapi.ps1
  cd C:\Users\zhuxd\myapi
  git status
#>
[CmdletBinding()]
param(
    [string]$Destination = "C:\Users\zhuxd\myapi"
)

$ErrorActionPreference = "Stop"
$Source = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Dest = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Destination)

if (-not (Test-Path -LiteralPath $Dest)) {
    throw "目标目录不存在: $Dest"
}
if (-not (Test-Path -LiteralPath (Join-Path $Dest ".git"))) {
    Write-Warning "目标目录未发现 .git，请确认 myapi 路径正确: $Dest"
}

Write-Host "同步: $Source"
Write-Host "  -> $Dest"
Write-Host ""

# robocopy: 0-7 成功或仅多余文件；>=8 失败
$robocopyArgs = @(
    $Source, $Dest,
    "/E", "/XO",
    "/XD", ".git", ".venv", "__pycache__", ".pytest_cache", ".turso",
    "server-data", ".cursor", "node_modules", "logs",
    "/XF", ".env",
    "/NFL", "/NDL", "/NJH", "/NJS"
)
& robocopy @robocopyArgs | Out-Null
$rc = $LASTEXITCODE
if ($rc -ge 8) {
    throw "robocopy 失败，退出码 $rc"
}

Write-Host "同步完成。助手将在 myapi 内 add/commit；你只需 push:"
Write-Host "  cd $Dest"
Write-Host "  git push origin main"
# robocopy 1-7 也视为成功
exit 0
