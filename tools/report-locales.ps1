# 多语言翻译覆盖报告：每个语言的键数、缺失键、与英文原文相同的未翻译项
#
# 用法：
#   pwsh -File tools/report-locales.ps1
#   pwsh -File tools/report-locales.ps1 -RequireTranslated
#   pwsh -File tools/report-locales.ps1 -LocaleDir NewLife.MySql/Locales
param(
    [String]$LocaleDir = "",
    [String]$Baseline = "zh-Hans.yml",
    [Switch]$RequireTranslated,
    [String]$AllowIdentical = "tools/locales-identical.allow.json"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$pattern = '^\s*"((?:[^"\\]|\\.)*)"\s*:\s*"((?:[^"\\]|\\.)*)"\s*$'

function Unescape-Yaml([string]$text) {
    return [regex]::Replace($text, '\\(u[0-9a-fA-F]{4}|.)', {
        param($m)
        $v = $m.Groups[1].Value
        if ($v.Length -eq 5) { return [string][char][Convert]::ToInt32($v.Substring(1), 16) }
        switch -CaseSensitive ($v) {
            'n' { return "`n" }
            'r' { return "`r" }
            't' { return "`t" }
            '\\' { return '\' }
            '"' { return '"' }
            default { return $m.Value }
        }
    })
}

function Read-Locale([String]$path) {
    $entries = [ordered]@{}
    foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8) {
        if ($line.Trim().Length -eq 0 -or $line.TrimStart().StartsWith('#')) { continue }
        $m = [regex]::Match($line, $pattern)
        if (!$m.Success) { continue }
        $entries[(Unescape-Yaml $m.Groups[1].Value)] = Unescape-Yaml $m.Groups[2].Value
    }
    return $entries
}

$allowSet = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
if ($AllowIdentical) {
    $allowPath = if ([IO.Path]::IsPathRooted($AllowIdentical)) { $AllowIdentical } else { Join-Path $root $AllowIdentical }
    if (Test-Path -LiteralPath $allowPath) {
        $data = Get-Content -LiteralPath $allowPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $keys = if ($data -is [Array]) { $data } elseif ($data.keys) { $data.keys } else { @() }
        foreach ($k in $keys) { [void]$allowSet.Add([String]$k) }
    }
}

function Test-OneDir([String]$dir) {
    $baselinePath = Join-Path $dir $Baseline
    if (!(Test-Path -LiteralPath $baselinePath)) { return $false }

    $reference = Read-Locale $baselinePath
    $failed = $false
    Write-Host "`n[$dir]" -ForegroundColor Cyan
    Write-Host ("{0,-10} {1,6} {2,6} {3,8} {4,8}" -f '语言', '键数', '缺失', '未翻译', '已翻译')

    foreach ($file in Get-ChildItem $dir -Filter *.yml | Sort-Object Name) {
        if ($file.Name -eq $Baseline -or $file.Name -eq 'en.yml') { continue }
        $locale = Read-Locale $file.FullName
        $missing = 0
        $identical = 0
        foreach ($key in $reference.Keys) {
            if (!$locale.Contains($key)) { $missing++; continue }
            if ($locale[$key] -ceq $key -and !$allowSet.Contains($key)) { $identical++ }
        }
        $translated = $reference.Count - $missing - $identical
        Write-Host ("{0,-10} {1,6} {2,6} {3,8} {4,8}" -f $file.BaseName, $reference.Count, $missing, $identical, $translated)

        if ($missing -gt 0 -or $identical -gt 0) {
            $failed = $true
            if ($RequireTranslated) {
                foreach ($key in $reference.Keys) {
                    if (!$locale.Contains($key)) {
                        Write-Host "  缺失：$key" -ForegroundColor Red
                    }
                    elseif ($locale[$key] -ceq $key -and !$allowSet.Contains($key)) {
                        Write-Host "  未翻译：$key" -ForegroundColor Yellow
                    }
                }
            }
        }
    }

    return !$failed
}

$dirs = @()
if ($LocaleDir) {
    $dir = if ([IO.Path]::IsPathRooted($LocaleDir)) { $LocaleDir } else { Join-Path $root $LocaleDir }
    $dirs = @(Get-Item -LiteralPath $dir)
}
else {
    $dirs = @(Get-ChildItem -Path $root -Recurse -Directory -Filter Locales -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' -and (Test-Path (Join-Path $_.FullName $Baseline)) } |
        Sort-Object FullName)
}

if ($dirs.Count -eq 0) {
    Write-Host "未找到任何包含 $Baseline 的 Locales 目录" -ForegroundColor Red
    exit 1
}

$allOk = $true
foreach ($dir in $dirs) {
    if (!(Test-OneDir $dir.FullName)) { $allOk = $false }
}

if ($RequireTranslated -and !$allOk) {
    Write-Host "`n翻译覆盖检查未通过" -ForegroundColor Red
    exit 1
}

Write-Host "`n翻译覆盖检查完成" -ForegroundColor Green
