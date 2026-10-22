# 多语言词表一致性校验：键集合、占位符、格式说明符、未翻译项
# 以基准语言文件（默认 zh-Hans.yml）为准，所有语言必须提供完全相同的键集合
#
# 用法：
#   pwsh -File tools/check-locales.ps1                                      # 自动发现仓库内所有 Locales 目录
#   pwsh -File tools/check-locales.ps1 -LocaleDir NewLife.MySql/Locales -SourceDir NewLife.MySql
#   pwsh -File tools/check-locales.ps1 -RequireTranslated                   # 额外要求所有语言都有真实译文
#   pwsh -File tools/check-locales.ps1 -LocaleDir ... -SkipOrphan            # 只校验词表内部一致性
param(
    [String]$LocaleDir = "",
    [String]$Baseline = "zh-Hans.yml",
    [String[]]$SourceDir = @(),
    [Switch]$SkipOrphan,
    [Switch]$RequireTranslated,
    [String]$AllowIdentical = "tools/locales-identical.allow.json"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

# 未指定目录时自动发现全部 Locales，逐个目录递归执行
if (!$LocaleDir) {
    $discovered = @(Get-ChildItem -Path $root -Recurse -Directory -Filter Locales -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' -and (Test-Path (Join-Path $_.FullName $Baseline)) } |
        Sort-Object FullName)
    if ($discovered.Count -eq 0) {
        Write-Host "未找到任何包含 $Baseline 的 Locales 目录" -ForegroundColor Red
        exit 1
    }

    $failed = $false
    foreach ($dir in $discovered) {
        $projectDir = Split-Path -Parent $dir.FullName
        & pwsh -NoProfile -File $PSCommandPath -LocaleDir $dir.FullName -Baseline $Baseline -SourceDir $projectDir -SkipOrphan:$SkipOrphan -RequireTranslated:$RequireTranslated -AllowIdentical $AllowIdentical
        if ($LASTEXITCODE -ne 0) { $failed = $true }
    }
    if ($failed) { exit 1 }
    exit 0
}

$localeDir = if ([IO.Path]::IsPathRooted($LocaleDir)) { $LocaleDir } else { Join-Path $root $LocaleDir }
if (!(Test-Path $localeDir)) {
    Write-Host "未找到语言目录：$localeDir" -ForegroundColor Red
    exit 1
}

if (!$SourceDir -or $SourceDir.Count -eq 0) {
    $SourceDir = @(Split-Path -Parent $localeDir)
}

$allowSet = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
if ($RequireTranslated -and $AllowIdentical) {
    $allowPath = if ([IO.Path]::IsPathRooted($AllowIdentical)) { $AllowIdentical } else { Join-Path $root $AllowIdentical }
    if (Test-Path -LiteralPath $allowPath) {
        $allowData = Get-Content -LiteralPath $allowPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $allowKeys = if ($allowData -is [Array]) { $allowData } elseif ($allowData.keys) { $allowData.keys } else { @() }
        foreach ($k in $allowKeys) { [void]$allowSet.Add([String]$k) }
    }
}

$pattern = '^\s*"((?:[^"\\]|\\.)*)"\s*:\s*"((?:[^"\\]|\\.)*)"\s*$'
$placeholder = '\{(\d+)(?:,-?\d+)?(:[^}]*)?\}'

function Unescape-Yaml([string]$text) {
    # 与源生成器保持一致：还原 \\ \" \n \r \t \uXXXX
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

function Get-Placeholders([string]$text) {
    $set = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($m in [regex]::Matches($text, $placeholder)) {
        [void]$set.Add([int]$m.Groups[1].Value)
    }
    return ,$set
}

function Get-PlaceholderSpecs([string]$text) {
    $map = @{}
    foreach ($m in [regex]::Matches($text, $placeholder)) {
        $index = [int]$m.Groups[1].Value
        if (!$map.ContainsKey($index)) { $map[$index] = $m.Groups[2].Value }
    }
    return $map
}

function Get-SourceMessageKeys([string]$text) {
    # 提取参与本地化的源码消息：L.T、Localization.Translate、带前缀日志助手、常用日志入口与 XTrace 日志
    $keys = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $patterns = @(
        'L\.T\(\s*"((?:[^"\\]|\\.)*)"',
        'Localization\.Translate(?:ForLog)?\(\s*(?:[A-Za-z_][\w\.]*\s*,\s*)?"((?:[^"\\]|\\.)*)"',
        '(?:InfoPrefixed|ErrorPrefixed|DebugPrefixed|WarnPrefixed)\([^,]+,\s*"((?:[^"\\]|\\.)*)"',
        'WritePrefixed\([^,]+,[^,]+,\s*"((?:[^"\\]|\\.)*)"',
        'Log\??\.(?:Info|Error|Warn|Debug|Trace|Write)\(\s*"((?:[^"\\]|\\.)*)"',
        'XTrace\.Write(?:Line)?\(\s*"((?:[^"\\]|\\.)*)"'
    )
    foreach ($p in $patterns) {
        foreach ($m in [regex]::Matches($text, $p)) {
            [void]$keys.Add((Unescape-Yaml $m.Groups[1].Value))
        }
    }
    return ,$keys
}

function Read-Locale([string]$path) {
    $entries = [ordered]@{}
    $errors = @()
    $lineNo = 0
    foreach ($line in Get-Content -LiteralPath $path -Encoding UTF8) {
        $lineNo++
        if ($line.Trim().Length -eq 0 -or $line.TrimStart().StartsWith("#")) { continue }
        $m = [regex]::Match($line, $pattern)
        if (!$m.Success) {
            $errors += "第 $lineNo 行格式非法：$line"
            continue
        }
        $key = Unescape-Yaml $m.Groups[1].Value
        if ($entries.Contains($key)) {
            $errors += "第 $lineNo 行键重复：$key"
            continue
        }
        $entries[$key] = Unescape-Yaml $m.Groups[2].Value
    }
    return @{ Entries = $entries; Errors = $errors }
}

$referencePath = Join-Path $localeDir $Baseline
if (!(Test-Path -LiteralPath $referencePath)) {
    Write-Host "未找到基准语言文件：$referencePath" -ForegroundColor Red
    exit 1
}
$reference = Read-Locale $referencePath
$allErrors = @($reference.Errors | ForEach-Object { "$($Baseline): $_" })
$allWarnings = @()

foreach ($file in Get-ChildItem $localeDir -Filter *.yml | Sort-Object Name) {
    if ($file.Name -eq $Baseline) { continue }
    $locale = Read-Locale $file.FullName
    foreach ($e in $locale.Errors) { $allErrors += "$($file.Name): $e" }

    foreach ($key in $reference.Entries.Keys) {
        if (!$locale.Entries.Contains($key)) {
            $allErrors += "$($file.Name): 缺少键 $key"
        }
    }
    foreach ($key in $locale.Entries.Keys) {
        if (!$reference.Entries.Contains($key)) {
            $allErrors += "$($file.Name): 多余键 $key"
            continue
        }

        $value = $locale.Entries[$key]
        $expected = Get-Placeholders $key
        $actual = Get-Placeholders $value
        if (!$expected.SetEquals($actual)) {
            $allErrors += "$($file.Name): 占位符不一致 → $key"
            continue
        }

        # 格式说明符只在译文保留该占位符时比对，缺失时仅告警
        $enSpecs = Get-PlaceholderSpecs $key
        $trSpecs = Get-PlaceholderSpecs $value
        foreach ($index in $enSpecs.Keys) {
            if ($trSpecs.ContainsKey($index) -and $trSpecs[$index] -ne $enSpecs[$index]) {
                $allWarnings += "$($file.Name): 格式说明符不一致 → $key"
                break
            }
        }

        # 严格模式：译文不得与英文原文完全相同（品牌/专有名词走白名单）
        if ($RequireTranslated -and $file.Name -ne 'en.yml' -and $value -ceq $key -and !$allowSet.Contains($key)) {
            $allErrors += "$($file.Name): 未翻译（与英文原文相同）→ $key"
        }
    }
}

# 孤儿键：基准表中的键在源码里完全找不到（改文案后遗留的死条目）
$orphans = @()
if (!$SkipOrphan) {
    $sourceDirs = @($SourceDir | ForEach-Object {
        if ([IO.Path]::IsPathRooted($_)) { $_ } else { Join-Path $root $_ }
    } | Where-Object { Test-Path $_ })

    $sourceText = ($sourceDirs | ForEach-Object { Get-ChildItem $_ -Recurse -Include *.cs,*.csproj -File } |
        Where-Object { $_.FullName -notmatch '\\(obj|bin|Locales)\\' } |
        ForEach-Object { [IO.File]::ReadAllText($_.FullName) }) -join "`n"

    foreach ($key in $reference.Entries.Keys) {
        # 源码中的字面量形式：制表符写作 \t，引号写作 \"
        $escaped = $key.Replace('\', '\\').Replace('"', '\"').Replace("`t", '\t').Replace("`r", '\r').Replace("`n", '\n')
        if (!$sourceText.Contains($key) -and !$sourceText.Contains($escaped)) {
            $orphans += $key
        }
    }
    foreach ($o in $orphans) { $allErrors += "孤儿键（源码中找不到）：$o" }

    # 反向检查：源码中参与本地化的消息必须存在于基准表，避免新增消息漏词表
    foreach ($key in (Get-SourceMessageKeys $sourceText)) {
        # [MySqlPacket] 为结构化诊断日志，按约定保持英文
        if ($key.StartsWith('[MySqlPacket]')) { continue }
        if (!$reference.Entries.Contains($key)) {
            $allErrors += "源码消息缺少词表键：$key"
        }
    }
}

foreach ($e in $allErrors) { Write-Host $e -ForegroundColor Red }
foreach ($w in $allWarnings) { Write-Host $w -ForegroundColor Yellow }

$count = (Get-ChildItem $localeDir -Filter *.yml).Count
if ($allErrors.Count -gt 0) {
    Write-Host "多语言词表校验未通过：$count 个语言，$($allErrors.Count) 个错误" -ForegroundColor Red
    exit 1
}

Write-Host "多语言词表校验通过：$count 个语言，基准 $($reference.Entries.Count) 条" -ForegroundColor Green
