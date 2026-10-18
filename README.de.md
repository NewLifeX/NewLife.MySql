# NewLife.MySql - Rein verwalteter Hochleistungs-MySQL-Treiber

![GitHub License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql?logo=github)
![NuGet Downloads](https://img.shields.io/nuget/dt/NewLife.MySql?logo=nuget)
![NuGet](https://img.shields.io/nuget/v/NewLife.MySql?logo=nuget)
![NuGet prerelease](https://img.shields.io/nuget/vpre/NewLife.MySql?label=dev%20nuget&logo=nuget)
[![test](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/test.yml)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)
[![contributors](https://img.shields.io/github/contributors/NewLifeX/NewLife.MySql?logo=github)](https://github.com/NewLifeX/NewLife.MySql/graphs/contributors)

<!-- lang:start -->
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> Diese Übersetzung kann hinter der chinesischen Version zurückliegen.

NewLife.MySql ist ein reiner C#-MySQL-Protokolltreiber, kompatibel mit ADO.NET, Entity Framework Core und XCode, mit Pooling, Transaktionen, Prepared Statements und Async.

## Installation

```bash
dotnet add package NewLife.MySql
```

## Schnellstart

```csharp
using var conn = new MySqlConnection("Server=127.0.0.1;Database=test;Uid=root;Pwd=***");
conn.Open();
```

## Funktionen

- Hoher Durchsatz und niedrige Latenz, im Produktivbetrieb erprobt
- Vollständiges Connection-Pooling, Transaktionen und Async-APIs
- Ziele von net45 bis net10.0 für die reibungslose Integration in Altsysteme

## Dokumentation

> 📖 Vollständige Dokumentation (Chinesisch): [Repository-Dokumente](Doc)

## Lizenz

MIT License © 2002-2026 NewLife-Team

[GitHub](https://github.com/NewLifeX/NewLife.MySql) | [Website](https://newlifex.com)
