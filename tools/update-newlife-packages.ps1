# NewLife.* 依赖升级：查询 NuGet 最新 beta，并可选写回 csproj
#
# 用法：
#   pwsh -File tools/update-newlife-packages.ps1            # 只报告
#   pwsh -File tools/update-newlife-packages.ps1 -Check     # 有落后项时退出 1（CI）
#   pwsh -File tools/update-newlife-packages.ps1 -Update    # 写回 csproj
param(
    [String]$Root = (Split-Path -Parent $PSScriptRoot),
    [Switch]$Check,
    [Switch]$Update
)

$ErrorActionPreference = "Stop"

$projects = @(Get-ChildItem -Path $Root -Recurse -Filter *.csproj -File |
    Where-Object { $_.FullName -notmatch '\\(bin|obj|\.git)\\' })
$refs = @()
foreach ($project in $projects) {
    $text = [IO.File]::ReadAllText($project.FullName)
    foreach ($m in [regex]::Matches($text, '<PackageReference\s+Include="(NewLife\.[^"]+)"\s+Version="([^"]+)"')) {
        $refs += [pscustomobject]@{
            Project = $project.FullName.Substring($Root.Length).TrimStart('\')
            Package = $m.Groups[1].Value
            Current = $m.Groups[2].Value
        }
    }
}

if ($refs.Count -eq 0) {
    Write-Host "未发现 NewLife.* PackageReference"
    exit 0
}

$cache = @{}
$outdated = @()
foreach ($group in ($refs | Group-Object Package)) {
    $package = $group.Name
    if (!$cache.ContainsKey($package)) {
        $url = "https://api.nuget.org/v3-flatcontainer/$($package.ToLower())/index.json"
        try {
            $data = Invoke-RestMethod -Uri $url -MaximumRedirection 5 -TimeoutSec 30
            $beta = @($data.versions | Where-Object { $_ -match '-' })
            $latest = if ($beta.Count -gt 0) { $beta[-1] } else { $data.versions[-1] }
            $cache[$package] = $latest
        }
        catch {
            Write-Host "✗ 查询 $package 失败：$($_.Exception.Message)" -ForegroundColor Red
            $cache[$package] = $null
        }
    }

    $latest = $cache[$package]
    foreach ($ref in $group.Group) {
        if (!$latest) { continue }
        $state = if ($ref.Current -eq $latest) { '最新' } else { '可升级' }
        Write-Host ("{0,-40} {1,-28} → {2,-28} {3}" -f $ref.Project, $ref.Current, $latest, $state)
        if ($ref.Current -ne $latest) {
            $outdated += $ref
            if ($Update) {
                $path = Join-Path $Root $ref.Project
                $text = [IO.File]::ReadAllText($path)
                $old = '<PackageReference Include="' + $ref.Package + '" Version="' + $ref.Current + '"'
                $new = '<PackageReference Include="' + $ref.Package + '" Version="' + $latest + '"'
                if ($text.Contains($old)) {
                    [IO.File]::WriteAllText($path, $text.Replace($old, $new), (New-Object System.Text.UTF8Encoding($false)))
                    Write-Host "  已更新 $($ref.Project)" -ForegroundColor Green
                }
            }
        }
    }
}

if ($Check -and $outdated.Count -gt 0) {
    Write-Host "`n存在 $($outdated.Count) 处可升级的 NewLife.* 依赖" -ForegroundColor Yellow
    exit 1
}
