<#
.SYNOPSIS
  Print full path to python.exe for this repo (one line, no extra output).

.DESCRIPTION
  Resolution order:
  1) $env:FINANCIAL_PRODUCT_BS_PYTHON
  2) python.exe under Windows PythonCore registry (HKCU then HKLM)
  3) the literal string "python" (PATH fallback)

  Examples:
    $py = & "$PSScriptRoot\Resolve-ProjectPython.ps1"
    & $py -m pip install -r "$PSScriptRoot\..\requirements.txt"
#>
[CmdletBinding()]
param()

function Get-PythonCorePairs {
  foreach ($hive in @('HKCU', 'HKLM')) {
    $base = "${hive}:\SOFTWARE\Python\PythonCore"
    if (-not (Test-Path -LiteralPath $base)) { continue }
    Get-ChildItem -LiteralPath $base -ErrorAction SilentlyContinue | ForEach-Object {
      $verKey = $_.PSChildName
      $literalInstall = "${hive}:\SOFTWARE\Python\PythonCore\${verKey}\InstallPath"
      if (-not (Test-Path -LiteralPath $literalInstall)) { return }
      $dir = (Get-ItemProperty -LiteralPath $literalInstall -ErrorAction SilentlyContinue).'(default)'
      if ([string]::IsNullOrWhiteSpace($dir)) { return }
      [PSCustomObject]@{
        VersionKey = $verKey
        InstallDir = $dir.TrimEnd('\')
      }
    }
  }
}

function Get-SortKeyFromVersionKey {
  param([string]$versionKey)
  $vk = ($versionKey -split '-')[0]
  $nums = $vk.Split('.')
  $maj = if ($nums.Count -gt 0) { try { [int]$nums[0] } catch { 0 } } else { 0 }
  $min = if ($nums.Count -gt 1) { try { [int]$nums[1] } catch { 0 } } else { 0 }
  $pat = if ($nums.Count -gt 2) { try { [int]$nums[2] } catch { 0 } } else { 0 }
  return ($maj * 1000000 + $min * 1000 + $pat)
}

$custom = $env:FINANCIAL_PRODUCT_BS_PYTHON
if ($custom) {
  $c = $custom.Trim().Trim('"')
  if (Test-Path -LiteralPath $c) {
    Write-Output ((Resolve-Path -LiteralPath $c).Path)
    exit 0
  }
}

$candidates = @()
foreach ($row in Get-PythonCorePairs) {
  $exe = Join-Path $row.InstallDir 'python.exe'
  if (Test-Path -LiteralPath $exe) {
    $candidates += [PSCustomObject]@{
      Exe        = $exe
      SortKey    = (Get-SortKeyFromVersionKey $row.VersionKey)
      VersionKey = $row.VersionKey
    }
  }
}

if ($candidates.Count -gt 0) {
  $picked = $candidates | Sort-Object -Property SortKey -Descending | Select-Object -First 1
  Write-Output ($picked.Exe)
  exit 0
}

Write-Output 'python'
exit 0
