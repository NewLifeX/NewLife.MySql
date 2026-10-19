# NewLife.MySql - 純受控高效能 MySQL 驅動

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.MySql?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.MySql?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.MySql?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.MySql?logo=github)](https://github.com/NewLifeX/NewLife.MySql/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> 本翻譯可能落後，以簡體中文版為準。

NewLife.MySql 是純 C# 實作的 MySQL 協定驅動，相容 ADO.NET、Entity Framework Core 與 XCode，支援連線集區、交易、預處理和非同步操作。

## 安裝

```bash
dotnet add package NewLife.MySql
```

## 快速開始

```csharp
using var conn = new MySqlConnection("Server=127.0.0.1;Database=test;Uid=root;Pwd=***");
conn.Open();
```

## 核心特性

- 高吞吐、低延遲，已在生產環境大規模驗證
- 連線集區、交易與非同步 API 完整支援
- 多目標框架 net45 ~ net10.0，舊系統可平滑接入

## 文件

> 📖 完整文件（中文）見 [儲存庫文件](Doc)

## 授權

MIT License © 2002-2026 新生命開發團隊

[GitHub](https://github.com/NewLifeX/NewLife.MySql) | [Website](https://newlifex.com)
