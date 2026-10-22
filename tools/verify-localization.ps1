# 本地化统一验证：构建、测试、词表、英文消息、元数据、README、包消费
#
# 用法：
#   pwsh -File tools/verify-localization.ps1
#   pwsh -File tools/verify-localization.ps1 -RequireTranslated
#   pwsh -File tools/verify-localization.ps1 -SkipBuild -SkipTests -SkipPackage
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [Switch]$RequireTranslated,
    [Switch]$SkipBuild,
    [Switch]$SkipTests,
    [Switch]$SkipPackage
)

$ErrorActionPreference = "Stop"

function Invoke-Step([String]$Name, [ScriptBlock]$Action) {
    Write-Host "`n== $Name ==" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -ne 0) { throw "$Name 失败（exit $LASTEXITCODE）" }
}

$oldLang = $env:DOTNET_CLI_UI_LANGUAGE
$env:DOTNET_CLI_UI_LANGUAGE = 'en'
try {
    if (!$SkipBuild) {
        Invoke-Step "构建" { dotnet build (Join-Path $Root "NewLife.MySql.slnx") -c Release --nologo -v q }
    }

    if (!$SkipTests) {
        Invoke-Step "主库测试" { dotnet test (Join-Path $Root "UnitTest\UnitTest.csproj") -c Release --no-build --nologo --logger "console;verbosity=minimal" }
        Invoke-Step "EF6 测试" { dotnet test (Join-Path $Root "UnitTest.EntityFramework\UnitTest.EntityFramework.csproj") -c Release --no-build --nologo --logger "console;verbosity=minimal" }
        Invoke-Step "EF Core 测试" { dotnet test (Join-Path $Root "UnitTest.EntityFrameworkCore\UnitTest.EntityFrameworkCore.csproj") -c Release --no-build --nologo --logger "console;verbosity=minimal" }
    }

    $localeArgs = @{}
    if ($RequireTranslated) { $localeArgs['RequireTranslated'] = $true }
    Invoke-Step "词表一致性" { & (Join-Path $Root "tools\check-locales.ps1") @localeArgs }
    Invoke-Step "翻译覆盖" { & (Join-Path $Root "tools\report-locales.ps1") @localeArgs }
    Invoke-Step "运行时消息英文" { & (Join-Path $Root "tools\check-english-exceptions.ps1") }
    Invoke-Step "程序集元数据" { & (Join-Path $Root "tools\check-assembly-metadata.ps1") }
    Invoke-Step "配置元数据" { & (Join-Path $Root "tools\check-config-metadata.ps1") }

    $readmeArgs = @{}
    if ($RequireTranslated) { $readmeArgs['RequireFull'] = $true }
    Invoke-Step "README 完整度" { & (Join-Path $Root "tools\check-readme-coverage.ps1") @readmeArgs }
    Invoke-Step "README 语言切换行" { & (Join-Path $Root "tools\sync-readme-languages.ps1") -Check }
    Invoke-Step "README 包版" { & (Join-Path $Root "tools\sync-readme-package.ps1") -Check }

    if (!$SkipPackage) {
        Invoke-Step "NuGet 消费端" { & (Join-Path $Root "tools\check-package-consumer.ps1") }
    }

    Write-Host "`n本地化验证全部通过" -ForegroundColor Green
}
finally {
    if ($null -eq $oldLang) { Remove-Item Env:DOTNET_CLI_UI_LANGUAGE -ErrorAction SilentlyContinue }
    else { $env:DOTNET_CLI_UI_LANGUAGE = $oldLang }
}
