# 包切片所有权检查：SpanReader 不释放构造时传入的数据包
#
# 约定：不要把 OwnerPacket.Slice(...) 直接传给 SpanReader 构造函数。
# 切片会新增一份引用计数，而 SpanReader 不拥有/不释放它，池化缓冲会无法回池。
# 正确写法：直接传原包并 Advance(offset)，或使用原包的 GetSpan()[offset..]。
#
# 用法：
#   pwsh -File tools/check-packet-slices.ps1
#   pwsh -File tools/check-packet-slices.ps1 -Dir NewLife.MySql
param(
    [String]$Dir = ""
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
if (!$Dir) { $Dir = Join-Path $root "NewLife.MySql" }
elseif (![IO.Path]::IsPathRooted($Dir)) { $Dir = Join-Path $root $Dir }

$pattern = 'new\s+SpanReader\s*\([^)]*\.Slice\s*\('
$files = Get-ChildItem -Path $Dir -Recurse -File -Filter *.cs |
    Where-Object { $_.FullName -notmatch '[\\/](bin|obj|\.git)[\\/]' }

$hits = @()
foreach ($file in $files) {
    $matches = Select-String -Path $file.FullName -Pattern $pattern
    foreach ($m in $matches) {
        $line = $m.Line.Trim()
        # 允许显式注释说明为历史兼容写法
        if ($line -match '//\s*packet-slice-allowed') { continue }
        $relative = $file.FullName.Substring($root.Length).TrimStart('\', '/')
        $hits += "${relative}:$($m.LineNumber): $line"
    }
}

if ($hits.Count -gt 0) {
    Write-Host "发现 SpanReader 直接接收 OwnerPacket.Slice 的写法（切片句柄不会释放）：" -ForegroundColor Red
    $hits | ForEach-Object { Write-Host "  $_" }
    Write-Host "请改为直接传原包并 Advance(offset)，或使用 GetSpan()[offset..]。" -ForegroundColor Yellow
    exit 1
}

Write-Host "包切片所有权检查通过：未发现 SpanReader 接收 Slice 的写法"
