# 将当前 Git 提交标记为可回滚的基线版本（附注标签）
# 用法（在要标记的仓库根目录）：
#   .\scripts\tag-baseline.ps1
#   .\scripts\tag-baseline.ps1 -Name "baseline-2026-05-16"
#   .\scripts\tag-baseline.ps1 -Message "Turso+Wind 稳定，策略列表完整"
#
# 推送到 GitHub 后可在任意机器回滚：
#   git fetch --tags
#   git checkout baseline-2026-05-16
# 或新建分支从基线继续： git checkout -b hotfix-from-baseline baseline-2026-05-16

param(
    [string]$Name = "",
    [string]$Message = "Application baseline for rollback"
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Set-Location $root
if (-not (Test-Path ".git")) {
    Write-Error "当前目录不是 Git 仓库: $root"
}

if (-not $Name) {
    $Name = "baseline-" + (Get-Date -Format "yyyy-MM-dd")
}

$commit = (git rev-parse --short HEAD).Trim()
$fullMsg = "$Message`ncommit: $commit`n$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') 北京时间"

git tag -a $Name -m $fullMsg
Write-Host "已创建附注标签: $Name -> $commit"
Write-Host ""
Write-Host "下一步（在部署用的 myapi 仓库执行同样命令，或 push 后在该仓库 fetch tag）："
Write-Host "  git push origin $Name"
Write-Host ""
Write-Host "回滚到该版本："
Write-Host "  git fetch origin tag $Name"
Write-Host "  git checkout $Name"
Write-Host "  # 或保留分支名: git checkout -b restore/$Name $Name"
