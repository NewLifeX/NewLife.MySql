# NewLife.MySql - برنامج تشغيل MySQL عالي الأداء مُدار بالكامل

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
🌐 [中文](Readme.MD) | [繁體中文](README.zh-TW.md) | [English](README.en.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md)
<!-- lang:end -->

> قد تتأخر هذه الترجمة عن النسخة الصينية.

NewLife.MySql برنامج تشغيل بروتوكول MySQL بـ C# خالص متوافق مع ADO.NET وEntity Framework Core وXCode، يدعم التجميع والمعاملات والاستعلامات المُعدّة وغير المتزامن.

## التثبيت

```bash
dotnet add package NewLife.MySql
```

## البدء السريع

```csharp
using var conn = new MySqlConnection("Server=127.0.0.1;Database=test;Uid=root;Pwd=***");
conn.Open();
```

## الميزات الرئيسية

- إنتاجية عالية وزمن استجابة منخفض، مُثبت على نطاق الإنتاج
- دعم كامل لتجميع الاتصالات والمعاملات وواجهات غير المتزامنة
- يدعم net45 إلى net10.0 لسهولة التبني في الأنظمة القديمة

## التوثيق

> 📖 التوثيق الكامل (بالصينية): [مستندات المستودع](Doc)

## الترخيص

MIT License © 2002-2026 فريق NewLife

[GitHub](https://github.com/NewLifeX/NewLife.MySql) | [Website](https://newlifex.com)
