# NuGet 消费端冒烟：打包本仓库，在临时工程引用本地 nupkg，验证词表已编入程序集
#
# 验证点：
#   1. Localization.Translate 在 zh-Hans / ja 生效
#   2. 未命中语言回退英文
#   3. 程序集标题可通过 Localization.Translate 取得译文
#
# 用法：
#   pwsh -File tools/check-package-consumer.ps1
#   pwsh -File tools/check-package-consumer.ps1 -Keep
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [Switch]$Keep
)

$ErrorActionPreference = "Stop"

$work = Join-Path $Root ".tmp\package-consumer"
if (Test-Path -LiteralPath $work) { Remove-Item -LiteralPath $work -Recurse -Force }
New-Item -ItemType Directory -Path $work | Out-Null

$packDir = Join-Path $work "packages"
New-Item -ItemType Directory -Path $packDir | Out-Null

Write-Host "打包 NewLife.MySql ..."
& dotnet pack (Join-Path $Root "NewLife.MySql\NewLife.MySql.csproj") -c Release -o $packDir --nologo
if ($LASTEXITCODE -ne 0) { throw "dotnet pack 失败" }

$nupkg = Get-ChildItem $packDir -Filter "NewLife.MySql.*.nupkg" | Sort-Object Name | Select-Object -Last 1
if (!$nupkg) { throw "未找到 nupkg" }
$version = $nupkg.BaseName.Substring("NewLife.MySql.".Length)
Write-Host "包版本：$version"

$consumer = Join-Path $work "Consumer"
& dotnet new console -n Consumer -o $consumer --framework net10.0 --force --nologo | Out-Null
if ($LASTEXITCODE -ne 0) { throw "创建消费工程失败" }

$nugetConfig = @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local" value="$packDir" />
    <add key="nuget" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
"@
[IO.File]::WriteAllText((Join-Path $consumer "NuGet.config"), $nugetConfig, (New-Object System.Text.UTF8Encoding($false)))

& dotnet add (Join-Path $consumer "Consumer.csproj") package NewLife.MySql --version $version --nologo
if ($LASTEXITCODE -ne 0) { throw "添加本地包失败" }

$program = @'
using System.Globalization;
using System.Reflection;
using NewLife;
using NewLife.MySql;

var assembly = typeof(MySqlConnection).Assembly;
var title = assembly.GetCustomAttribute<AssemblyTitleAttribute>()?.Title ?? "";
if (String.IsNullOrWhiteSpace(title)) throw new Exception("AssemblyTitle is empty");

Localization.Culture = CultureInfo.GetCultureInfo("zh-Hans");
var zh = Localization.Translate("Connection is not open.");
if (zh == "Connection is not open.") throw new Exception("zh-Hans translation missing");
var zhTitle = Localization.Translate(title);
if (zhTitle == title) throw new Exception("zh-Hans assembly title translation missing");

Localization.Culture = CultureInfo.GetCultureInfo("ja");
var ja = Localization.Translate("Connection is not open.");
if (ja == "Connection is not open.") throw new Exception("ja translation missing");

Localization.Culture = CultureInfo.GetCultureInfo("sw-KE");
var fallback = Localization.Translate("Connection is not open.");
if (fallback != "Connection is not open.") throw new Exception("English fallback broken");

Console.WriteLine("OK: " + zh + " | " + ja + " | " + zhTitle);
'@
[IO.File]::WriteAllText((Join-Path $consumer "Program.cs"), $program, (New-Object System.Text.UTF8Encoding($false)))

Push-Location $consumer
try {
    & dotnet run -c Release --nologo
    if ($LASTEXITCODE -ne 0) { throw "消费端运行失败" }
}
finally {
    Pop-Location
}

if (!$Keep) {
    Remove-Item -LiteralPath $work -Recurse -Force
    Write-Host "已清理临时工程（-Keep 可保留）"
}
Write-Host "NuGet 消费端冒烟通过" -ForegroundColor Green
