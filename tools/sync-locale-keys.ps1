# 补齐词表缺失键：以基准表为源，在所有语言文件末尾追加英文占位
#
# 用途：新增消息时先保证所有语言键集合一致，避免 check-locales 因缺键失败；
# 非中英文的正式译文由后续 low 模式子代理统一补齐。
#
# 用法：
#   pwsh -File tools/sync-locale-keys.ps1
#   pwsh -File tools/sync-locale-keys.ps1 -Check
#   pwsh -File tools/sync-locale-keys.ps1 -LocaleDir NewLife.MySql/Locales
param(
    [String]$LocaleDir = "",
    [String]$Baseline = "zh-Hans.yml",
    [Switch]$Check
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$pattern = '^\s*"((?:[^"\\]|\\.)*)"\s*:'

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

function Escape-Yaml([string]$text) {
    return $text.Replace('\', '\\').Replace('"', '\"').Replace("`r", '\r').Replace("`n", '\n').Replace("`t", '\t')
}

function Sync-OneDir([String]$dir) {
    $baselinePath = Join-Path $dir $Baseline
    if (!(Test-Path -LiteralPath $baselinePath)) { return $true }

    $keys = [System.Collections.Generic.List[String]]::new()
    foreach ($line in Get-Content -LiteralPath $baselinePath -Encoding UTF8) {
        if ($line.Trim().Length -eq 0 -or $line.TrimStart().StartsWith('#')) { continue }
        $m = [regex]::Match($line, $pattern)
        if ($m.Success) { $keys.Add((Unescape-Yaml $m.Groups[1].Value)) }
    }

    $changed = 0
    foreach ($file in Get-ChildItem $dir -Filter *.yml | Sort-Object Name) {
        if ($file.Name -eq $Baseline -or $file.Name -eq 'en.yml') { continue }

        $existing = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $text = [IO.File]::ReadAllText($file.FullName)
        foreach ($line in ($text -split "`r?`n")) {
            if ($line.Trim().Length -eq 0 -or $line.TrimStart().StartsWith('#')) { continue }
            $m = [regex]::Match($line, $pattern)
            if ($m.Success) { [void]$existing.Add((Unescape-Yaml $m.Groups[1].Value)) }
        }

        $missing = @($keys | Where-Object { !$existing.Contains($_) })
        if ($missing.Count -eq 0) { continue }

        $changed++
        if ($Check) {
            Write-Host "✗ $($file.Name)：缺少 $($missing.Count) 个键" -ForegroundColor Red
            $missing | Select-Object -First 5 | ForEach-Object { Write-Host "  $_" }
            continue
        }

        $newline = if ($text.Contains("`r`n")) { "`r`n" } else { "`n" }
        if (!$text.EndsWith($newline)) { $text += $newline }
        $append = [System.Text.StringBuilder]::new()
        foreach ($key in $missing) {
            $escaped = Escape-Yaml $key
            [void]$append.Append('"').Append($escaped).Append('": "').Append($escaped).Append('"').Append($newline)
        }
        [IO.File]::WriteAllText($file.FullName, $text + $append.ToString(), (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "✓ $($file.Name)：追加 $($missing.Count) 个英文占位" -ForegroundColor Green
    }

    return ($changed -eq 0)
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

$failed = $false
foreach ($dir in $dirs) {
    if (!(Sync-OneDir $dir.FullName)) { $failed = $true }
}

if ($Check -and $failed) { exit 1 }
