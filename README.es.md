# NewLife.MySql — Driver MySQL de Alto Rendimiento en C# Puro

[![AOT](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml/badge.svg?label=AOT)](https://github.com/NewLifeX/NewLife.MySql/actions/workflows/aot.yml)


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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

> ⚠️ Esta traducción puede quedar desactualizada; la versión autorizada es el [README en inglés](README.md).

**NewLife.MySql** es un driver ADO.NET para MySQL en C# puro desarrollado por el equipo NewLife. Implementa el protocolo de cable MySQL (Protocol Version 10) directamente sobre TCP, con **cero dependencias de terceros**, **soporte async/await completo** y **licencia MIT** para uso comercial sin preocupaciones.

Su innovadora **ejecución por lotes en pipeline** ofrece un rendimiento DML por lotes **2–3× más rápido** comparado con la ejecución fila por fila.

---

## Comparativa de funciones

| Funcionalidad | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|---------|:---:|:---:|:---:|
| Licencia | **MIT** ✅ | MIT ✅ | GPLv2 ⚠️ |
| Dependencias | **0** (solo NewLife.Core) | 1 | 6 |
| IO Asíncrono real | ✅ | ✅ | ❌ |
| Lotes en pipeline | ✅ | ❌ | ❌ |
| Lotes por array | ✅ | ❌ | ❌ |
| Lotes por diccionario | ✅ | ❌ | ❌ |
| MySqlBulkCopy | ✅ | ✅ | ✅ |
| Compresión (zlib) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| Autenticación WebAuthn | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ Detección automática | ❌ | ❌ |
| Frameworks soportados | net45 ~ net10 | net462+ | net462+ |
| NativeAOT (net8.0+) | ✅ | ✅ | ❌ |
| Trazado OpenTelemetry | ✅ | ✅ | ✅ (8.1+) |

---

## Rendimiento

> Entorno de prueba: .NET 10 + MySQL 8.0.39 localhost (Windows), TCP 127.0.0.1:3306

### DML por lotes: 10,000 filas (ms, menor es mejor)

| Operación | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | **899** | 1,927 | 1,906 |
| UPDATE | **710** | 2,265 | 2,041 |
| DELETE | **661** | 1,961 | 1,767 |

### Aceleración con pipeline (1,000 filas)

| Escenario | Fila a fila | Pipeline + Tx | Aceleración |
|----------|----------:|-------------:|-------:|
| INSERT | 437ms | 54ms | **8.1×** |

---

## Inicio rápido

### Instalación

```shell
dotnet add package NewLife.MySql
```

### Conexión

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();
```

### CRUD básico

```csharp
// Consulta
using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}, {reader.GetInt64(2)}");
}

// Escalar
var count = (Int64)cmd.ExecuteScalar("SELECT COUNT(*) FROM users");

// No consulta
var rows = cmd.ExecuteNonQuery("INSERT INTO users(name, age) VALUES('Tom', 25)");
```

### Consultas parametrizadas

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);
using var reader = cmd.ExecuteReader();
```

### Transacciones

```csharp
using var tr = conn.BeginTransaction();
try
{
    conn.ExecuteNonQuery("INSERT INTO orders(product, qty) VALUES('Widget', 10)");
    conn.ExecuteNonQuery("UPDATE inventory SET qty = qty - 10 WHERE product = 'Widget'");
    tr.Commit();
}
catch
{
    tr.Rollback();
    throw;
}
```

### Operaciones por lotes (Pipeline)

```csharp
var connStr = "Server=localhost;Database=mydb;...;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);  // ¡2~3× más rápido que la competencia!
```

### API asíncrona

```csharp
await conn.OpenAsync();
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## Parámetros de cadena de conexión

| Parámetro | Alias | Predeterminado | Descripción |
|-----------|---------|---------|-------------|
| Server | DataSource, Data Source | — | Dirección del servidor |
| Port | — | 3306 | Número de puerto |
| Database | — | — | Nombre de la base de datos |
| UserID | Uid, User Id, User | — | Nombre de usuario |
| Password | Pass, Pwd | — | Contraseña |
| ConnectionTimeout | Connection Timeout | 15 | Tiempo de espera en segundos |
| SslMode | Ssl Mode | None | None / Preferred / Required |
| UseServerPrepare | Use Server Prepare | false | Preparación en servidor |
| Pipeline | Pipelining | false | Ejecución por lotes en pipeline |
| MinPoolSize | Min Pool Size | 0 | Tamaño mínimo del pool |
| MaxPoolSize | Max Pool Size | 100 | Tamaño máximo del pool |

---

## Compatibilidad de frameworks

| TFM | Estado |
|-----|--------|
| `net45` | ✅ El único driver MySQL moderno compatible con .NET 4.5 |
| `net461` | ✅ |
| `netstandard2.0` | ✅ |
| `netstandard2.1` | ✅ |
| `net6.0` | ✅ + API DbBatch |
| `net8.0` | ✅ |
| `net10.0` | ✅ Último .NET |

---

## Compatibilidad de bases de datos

| Base de datos | Detección | Notas |
|----------|:---------:|-------|
| MySQL 5.x ~ 9.0+ | Automática | 3 métodos de autenticación |
| OceanBase | Detección automática | CRUD completo + transacciones |
| TiDB | Detección automática | CRUD completo + transacciones |
| Aurora / CloudSQL | Detección automática | Compatible con AWS Aurora / GCP Cloud SQL |
| MariaDB | Básica | Sin soporte para `ed25519` |

---

## Mapa del ecosistema

NewLife es un ecosistema por capas de proyectos de código abierto con versionado independiente. El mapa siguiente se comparte literalmente en todos los repositorios de NewLife; cada proyecto enlaza a su repositorio y a su paquete principal de NuGet.

| Proyecto | Qué hace | Descargas |
| --- | --- | --- |
| **Fundación** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Registro de logs, configuración, caché, red, serialización, programación de tareas y seguimiento APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Middleware de datos: tablas únicas a escala de decenas de miles de millones, particionado automático, separación lectura/escritura y más de 10 bases de datos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Biblioteca de red de alto rendimiento: 1,77 M pps / 14,5 Gbps en una máquina (TCP 1KB, 2026-10), 4 M conexiones TCP persistentes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Framework de comunicación por protocolo y RPC para aplicaciones cliente/servidor y dispositivos IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Instala aplicaciones .NET como demonios del sistema operativo: servicios de Windows y systemd de Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Plantillas de proyecto `dotnet new` para consola, web, API, servicio, datos e IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Datos y mensajería** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Cliente Redis con latencia de microsegundos, millones de operaciones por segundo y colas de mensajes integradas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Cliente RocketMQ totalmente administrado para Apache RocketMQ y Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Cliente y servidor MQTT, incluida la conectividad con Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Base de datos híbrida en C#: embebida o en red, relacional, series temporales, cola de mensajes y KV en una sola. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Controlador y capa de ayuda de MySql construidos sobre la pila de datos de NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA y distribuido** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework rápido para plataformas de administración: permisos, SSO, servidor OAuth y andamiaje CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plataforma distribuida de servicios: gestión de nodos, APM, centro de configuración, registro y centro de despliegues. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plataforma de tareas distribuidas y programación de datos en tiempo real con particionado, reintentos y relleno histórico. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Cliente unificado para 46 proveedores de LLM más ChatAI, una aplicación web de chat lista para usar. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial e IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Biblioteca estándar de IoT que define contratos de protocolo y abstracciones de dispositivos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implementación de Modbus TCP/RTU/ASCII sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocolo para PLC Siemens sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocolo para PLC Omron sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocolo para PLC Mitsubishi Melsec sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Cliente OPC para el acceso a datos industriales. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Pila de protocolo BACnet para la automatización de edificios. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plataforma IoT de código abierto y ligera con gestión de pasarelas y dispositivos. | — |
| **Telemática vehicular** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocolo de posicionamiento vehicular JT/T 808: más de 70 cuerpos de mensaje, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocolo plataforma a plataforma JT/T 809 para autoridades de transporte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vídeo a bordo JT/T 1078: análisis RTP, remultiplexado FLV/fMP4 y transmisión en directo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Componentes de dominio** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Un solo paquete para Excel, Word, PPT, PDF y más de 15 formatos de documento. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Audio multiplataforma: códecs, E/S de dispositivos, DSP, contenedores y streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Códecs de vídeo, formatos de contenedor, procesamiento de fotogramas y streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interfaz unificada sobre Baidu, AMap, Tencent y Tianditu con geocodificación y conversión de coordenadas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Geolocalización IPv4 con una base de datos offline mapeada en memoria y una API en línea de respaldo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Herramientas y productos** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Caja de herramientas del desarrollador: red, puerto serie, criptografía, expresiones regulares, Modbus y MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Sistema de gestión de código fuente Git autoalojado. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Instala automáticamente el runtime de .NET correspondiente cuando se ejecuta desde un host más antiguo. | — |

> Las insignias muestran el paquete principal de cada proyecto; los paquetes de temas, extensiones y satélites figuran en sus páginas de NuGet.Los más de 30 paquetes no listados aquí (FTP, LoRa, extensiones de Office, controladores de protocolo, etc.) están disponibles en la [organización NewLifeX](https://github.com/NewLifeX) y en [newlifex.com](https://newlifex.com).

## Equipo

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife se fundó en 2002 y lleva más de dos décadas dedicado al ecosistema .NET e IoT, mantiene cerca de 100 proyectos de código abierto (34 de ellos principales) y ofrece soluciones integradas de software y hardware para los sectores de energía, logística, salud, control industrial e Internet. Nos guía el lema «en el aprendizaje no hay antigüedad: quien domina es maestro».

Los componentes de código abierto acumulan <!--nuget-total:start-->6.48M<!--nuget-total:end--> descargas en NuGet (todo el ecosistema, a fecha de 2026-10).

Sitio web: https://newlifex.com · Organización de código abierto: https://github.com/NewLifeX

<details>
<summary>Cuenta oficial de WeChat: 智能大石头 (escanea para seguir)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>

## Licencia

MIT License — uso gratuito para proyectos personales y comerciales.

Copyright © 2002-2026 [NewLife](https://newlifex.com)
