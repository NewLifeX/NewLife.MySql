# 程序集元数据本地化检查：AssemblyTitle / Description 必须是英文源文，且进入本项目基准词表
#
# 约定：csproj 的 AssemblyTitle/Description 写英文源文，同一个字符串作为 key 写入 Locales/zh-Hans.yml。
# 运行时由展示层调用 Localization.Translate 取得译文；NuGet 画廊字段本身无法按语言切换。
#
# 用法：
#   pwsh -File tools/check-assembly-metadata.ps1
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [String]$Baseline = "zh-Hans.yml"
)

$ErrorActionPreference = "Stop"

$cjkPattern = '[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]'
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

function Read-LocaleKeys([String]$path) {
    $keys = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    if (!(Test-Path -LiteralPath $path)) { return ,$keys }
    foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8) {
        if ($line.Trim().Length -eq 0 -or $line.TrimStart().StartsWith('#')) { continue }
        $m = [regex]::Match($line, $pattern)
        if ($m.Success) { [void]$keys.Add((Unescape-Yaml $m.Groups[1].Value)) }
    }
    return ,$keys
}

$errors = 0
$checked = 0
$projects = Get-ChildItem -Path $Root -Recurse -Filter *.csproj -File |
    Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' }

foreach ($project in $projects) {
    [xml]$xml = [IO.File]::ReadAllText($project.FullName)
    $titles = @($xml.SelectNodes('//AssemblyTitle') | ForEach-Object { $_.InnerText.Trim() } | Where-Object { $_ })
    $descriptions = @($xml.SelectNodes('//Description') | ForEach-Object { $_.InnerText.Trim() } | Where-Object { $_ })
    if ($titles.Count -eq 0 -and $descriptions.Count -eq 0) { continue }

    $localePath = Join-Path $project.DirectoryName "Locales\$Baseline"
    if (!(Test-Path -LiteralPath $localePath)) {
        Write-Host "✗ $($project.FullName)：缺少 $localePath" -ForegroundColor Red
        $errors++
        continue
    }

    $keys = Read-LocaleKeys $localePath
    foreach ($value in @($titles + $descriptions)) {
        if ($value -match $cjkPattern) {
            Write-Host "✗ $($project.Name)：元数据含中文 → $value" -ForegroundColor Red
            $errors++
        }
        if (!$keys.Contains($value)) {
            Write-Host "✗ $($project.Name)：元数据未进入 $Baseline → $value" -ForegroundColor Red
            $errors++
        }
    }
    $checked++
}

if ($errors -gt 0) {
    Write-Host "`n程序集元数据检查未通过：$errors 项" -ForegroundColor Red
    exit 1
}

Write-Host "程序集元数据检查通过：$checked 个工程" -ForegroundColor Green
