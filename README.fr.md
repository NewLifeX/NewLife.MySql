# NewLife.MySql - Pilote MySQL hautes performances entièrement managé

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.MySql?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.MySql?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.MySql?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml)
[![locales](https://img.shields.io/badge/locales-11-2ea44f?logo=dotnet)](Doc/多语言本地化.md)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS%20%7C%20Android%20%7C%20iOS-blue)
![arch](https://img.shields.io/badge/arch-x64%20%7C%20arm64%20%7C%20loongarch64%20%7C%20riscv64%20%7C%20s390x-orange)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.MySql?logo=github)](https://github.com/NewLifeX/NewLife.MySql/graphs/contributors)

<!-- lang:start -->
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> ⚠️ Cette traduction peut être en retard ; la version de référence est le [README anglais](README.md).

NewLife.MySql est un pilote MySQL en C# pur compatible ADO.NET, Entity Framework Core et XCode : pooling, transactions, requêtes préparées et asynchrone.

## Installation

```bash
dotnet add package NewLife.MySql
```

## Démarrage rapide

```csharp
using var conn = new MySqlConnection("Server=127.0.0.1;Database=test;Uid=root;Pwd=***");
conn.Open();
```

## Fonctionnalités

- Haut débit et faible latence, éprouvés à l'échelle de la production
- Pooling de connexions, transactions et API asynchrones complets
- Cibles net45 à net10.0 pour une adoption progressive

## Documentation

> 📖 Documentation complète (chinois) : [documents du dépôt](Doc)

## Licence

MIT License © 2002-2026 Équipe NewLife

[GitHub](https://github.com/NewLifeX/NewLife.MySql) | [Website](https://newlifex.com)
