# README 多语言完整度校验：标题结构、代码块、表格与占位标记
#
# 以 README.md 为唯一基线，检查每份 README.<lang>.md 是否为全量翻译，
# 而不是只翻译前几节的摘要页。代码块内容必须与英文完全一致，防止代码被翻译。
#
# 用法：
#   pwsh -File tools/check-readme-coverage.ps1
#   pwsh -File tools/check-readme-coverage.ps1 -RequireFull
#   pwsh -File tools/check-readme-coverage.ps1 -Languages zh-CN,zh-TW -RequireFull
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [String]$Config = "readme-languages.json",
    [String[]]$Languages = @(),
    [Switch]$RequireFull
)

$ErrorActionPreference = "Stop"

$configPath = Join-Path $Root $Config
if (!(Test-Path -LiteralPath $configPath)) { throw "未找到语言配置：$configPath" }

$configData = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
$langs = @($configData.languages | Where-Object { $_.code -ne 'en' })
if ($Languages.Count -gt 0) {
    $langs = @($langs | Where-Object { $Languages -contains $_.code })
    if ($langs.Count -ne $Languages.Count) {
        $known = @($configData.languages.code)
        $missing = @($Languages | Where-Object { $known -notcontains $_ })
        throw "配置中不存在这些语言：$($missing -join ', ')"
    }
}

$basePath = Join-Path $Root 'README.md'
if (!(Test-Path -LiteralPath $basePath)) { throw "未找到英文基线：$basePath" }

function Get-ReadmeShape([String]$text) {
    $lines = $text -split "`r?`n"
    $headings = [System.Collections.Generic.List[String]]::new()
    $blocks = [System.Collections.Generic.List[String]]::new()
    $inFence = $false
    $fenceLines = [System.Collections.Generic.List[String]]::new()
    $tableSeparators = 0

    foreach ($line in $lines) {
        if ($line -match '^\s*(```|~~~)') {
            if ($inFence) {
                $blocks.Add(($fenceLines -join "`n").Trim())
                $fenceLines.Clear()
            }
            $inFence = !$inFence
            continue
        }
        if ($inFence) {
            $fenceLines.Add($line)
            continue
        }

        $m = [regex]::Match($line, '^(#{1,6})\s+(.+?)\s*$')
        if ($m.Success) { $headings.Add($m.Groups[1].Value) }

        if ($line -match '^\s*\|?[\s:|-]*-[\s:|-]*\|[\s:|-]*$') { $tableSeparators++ }
    }

    return @{
        Headings = $headings.ToArray()
        Blocks = $blocks.ToArray()
        TableSeparators = $tableSeparators
    }
}

$base = Get-ReadmeShape ([IO.File]::ReadAllText($basePath))
$errors = 0
$placeholder = '(?i)(translation may lag|可能滞后|可能落後|待翻译|待翻譯|translation pending|\bTBD\b|\bTODO\b)'

foreach ($lang in $langs) {
    $path = Join-Path $Root $lang.file
    if (!(Test-Path -LiteralPath $path)) {
        Write-Host "✗ $($lang.file)：文件不存在" -ForegroundColor Red
        $errors++
        continue
    }

    $text = [IO.File]::ReadAllText($path)
    $shape = Get-ReadmeShape $text
    $localErrors = 0

    $baseHeadingKey = $base.Headings -join ','
    $targetHeadingKey = $shape.Headings -join ','
    if ($baseHeadingKey -ne $targetHeadingKey) {
        Write-Host "✗ $($lang.file)：标题结构不一致（基线 $($base.Headings.Count) 个，实际 $($shape.Headings.Count) 个）" -ForegroundColor Red
        $localErrors++
    }

    if ($base.Blocks.Count -ne $shape.Blocks.Count) {
        Write-Host "✗ $($lang.file)：代码块数量不一致（基线 $($base.Blocks.Count)，实际 $($shape.Blocks.Count)）" -ForegroundColor Red
        $localErrors++
    }
    else {
        for ($i = 0; $i -lt $base.Blocks.Count; $i++) {
            if ($base.Blocks[$i] -cne $shape.Blocks[$i]) {
                Write-Host "✗ $($lang.file)：第 $($i + 1) 个代码块内容与英文不一致" -ForegroundColor Red
                $localErrors++
                break
            }
        }
    }

    if ($base.TableSeparators -ne $shape.TableSeparators) {
        Write-Host "✗ $($lang.file)：表格数量不一致（基线 $($base.TableSeparators)，实际 $($shape.TableSeparators)）" -ForegroundColor Red
        $localErrors++
    }

    $m = [regex]::Match($text, $placeholder)
    if ($m.Success) {
        Write-Host "✗ $($lang.file)：仍含未完成标记「$($m.Value)」" -ForegroundColor Red
        $localErrors++
    }

    if ($localErrors -eq 0) {
        Write-Host "✓ $($lang.file)：标题 $($shape.Headings.Count)，代码块 $($shape.Blocks.Count)，表格 $($shape.TableSeparators)" -ForegroundColor Green
    }
    else {
        $errors += $localErrors
    }
}

if ($errors -gt 0) {
    Write-Host "`nREADME 完整度校验未通过：$errors 项" -ForegroundColor Red
    if ($RequireFull) { exit 1 }
}
else {
    Write-Host "`nREADME 完整度校验通过：$($langs.Count) 个语言" -ForegroundColor Green
}
