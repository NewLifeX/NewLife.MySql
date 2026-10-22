# 配置类元数据检查：DisplayName / Description / Category 必须是英文源文
#
# 约定：配置类与配置项的可见文案写英文源文，注释保持中文；展示层用 Localization.Translate
# 或 Core 的配置注释本地化机制取得译文。协议字段（如 ConnectAttributes 的 _client_*）不在
# 用户可见文案范围，通过 tools/config-metadata.allow.json 豁免。
#
# 用法：
#   pwsh -File tools/check-config-metadata.ps1
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [String]$AllowFile = "tools/config-metadata.allow.json"
)

$ErrorActionPreference = "Stop"

$cjkPattern = '[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]'
$attrPattern = '\[(?:DisplayName|Description|Category)\(\s*"((?:[^"\\]|\\.)*)"\s*\)\]'

$allow = @()
if ($AllowFile) {
    $allowPath = if ([IO.Path]::IsPathRooted($AllowFile)) { $AllowFile } else { Join-Path $Root $AllowFile }
    if (Test-Path -LiteralPath $allowPath) {
        $data = Get-Content -LiteralPath $allowPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $allow = @($data.items)
    }
}

$skip = @('bin', 'obj', '.git', 'Bin', '.tmp')
$skipPrefix = '^(UnitTest|Test|Benchmark|Samples|AotSmoke|Bin)(\.|\\)'
$errors = 0
$files = Get-ChildItem -Path $Root -Recurse -File -Filter *.cs |
    Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' } |
    Where-Object {
        $relative = $_.FullName.Substring($Root.Length).TrimStart('\')
        $segments = $relative.Split('\')
        ($relative -notmatch $skipPrefix) -and -not ($segments | Where-Object { $skip -contains $_ })
    }

foreach ($file in $files) {
    $relative = $file.FullName.Substring($Root.Length).TrimStart('\')
    $lineNo = 0
    foreach ($line in Get-Content -LiteralPath $file.FullName -Encoding UTF8) {
        $lineNo++
        foreach ($m in [regex]::Matches($line, $attrPattern)) {
            $value = $m.Groups[1].Value
            if ($value -notmatch $cjkPattern) { continue }

            $allowed = @($allow | Where-Object {
                ($_.file -eq $relative) -and ($_.value -eq $value)
            }).Count -gt 0
            if ($allowed) { continue }

            Write-Host "✗ ${relative}:${lineNo}: 配置元数据含中文 → $value" -ForegroundColor Red
            $errors++
        }
    }
}

if ($errors -gt 0) {
    Write-Host "`n配置元数据检查未通过：$errors 项" -ForegroundColor Red
    exit 1
}

Write-Host "配置元数据检查通过：未发现中文 DisplayName/Description/Category" -ForegroundColor Green
