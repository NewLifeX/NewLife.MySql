# 运行时消息英文检查：异常、日志、控制台输出与 L.T 的源文不得包含 CJK
#
# 约定：源码消息用英文（词表 key），注释与 XML 文档可以保持中文。
# 服务器返回的 MySqlException 原文、[MySqlPacket] 结构化追踪不在此脚本范围内。
#
# 用法：
#   pwsh -File tools/check-english-exceptions.ps1
#   pwsh -File tools/check-english-exceptions.ps1 -Dir NewLife.MySql -List
#   pwsh -File tools/check-english-exceptions.ps1 -Csv hits.csv
param(
    [String]$Dir = "",
    [Switch]$List,
    [String]$Csv = "",
    [String[]]$Exclude = @()
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
if (!$Dir) { $Dir = $root }
elseif (![IO.Path]::IsPathRooted($Dir)) { $Dir = Join-Path $root $Dir }

$skip = @('bin', 'obj', '.git', 'Locales', 'Bin', '.tmp')
$skipPrefix = '^(UnitTest|Test|Benchmark|Samples|AotSmoke|Bin)(\.|\\)'
if ($Exclude.Count -gt 0) { $skip += $Exclude }

$callPattern = '(?:throw\s+new\s+\w+Exception|L\.T|Localization\.Translate(?:ForLog)?|XTrace\.Write(?:Line)?|Log\??\.(?:Info|Error|Warn|Debug|Trace|Write)|WriteLog|Console\.Write(?:Line)?)\s*\(\s*"((?:[^"\\]|\\.)*)"'
$cjkPattern = '[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]'

$hits = @()
$files = Get-ChildItem -Path $Dir -Recurse -File -Filter *.cs |
    Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' } |
    Where-Object {
        $relative = $_.FullName.Substring($Dir.Length).TrimStart('\')
        $segments = $relative.Split('\')
        ($relative -notmatch $skipPrefix) -and -not ($segments | Where-Object { $skip -contains $_ })
    }

foreach ($file in $files) {
    $lineNo = 0
    foreach ($line in Get-Content -LiteralPath $file.FullName -Encoding UTF8) {
        $lineNo++
        foreach ($m in [regex]::Matches($line, $callPattern)) {
            $text = $m.Groups[1].Value
            if ($text -match $cjkPattern) {
                $hits += [pscustomobject]@{
                    File = $file.FullName.Substring($root.Length).TrimStart('\')
                    Line = $lineNo
                    Message = $text
                }
            }
        }
    }
}

if ($Csv) {
    $csvPath = if ([IO.Path]::IsPathRooted($Csv)) { $Csv } else { Join-Path $root $Csv }
    $hits | Export-Csv -LiteralPath $csvPath -NoTypeInformation -Encoding UTF8
    Write-Host "已导出：$csvPath"
}

foreach ($hit in $hits) {
    Write-Host "$($hit.File):$($hit.Line): $($hit.Message)"
}

if ($hits.Count -gt 0) {
    Write-Host "运行时消息英文检查未通过：$($hits.Count) 处 CJK 源文" -ForegroundColor Red
    if (!$List) { exit 1 }
}
else {
    Write-Host "运行时消息英文检查通过：未发现 CJK 源文" -ForegroundColor Green
}
