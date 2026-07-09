# NewLife.MySql — Driver MySQL de Alto Rendimiento en C# Puro

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)
[![Downloads](https://img.shields.io/nuget/dt/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)

> 🌐 [中文](Readme.MD) | [English](README.en.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | **Español** | [Português](README.pt.md)

**NewLife.MySql** es un driver ADO.NET para MySQL en C# puro desarrollado por el equipo NewLife. Implementa el protocolo de cable MySQL (Protocol Version 10) directamente sobre TCP, con **cero dependencias de terceros**, **soporte async/await completo** y **licencia MIT** para uso comercial sin preocupaciones.

Su innovadora **ejecución por lotes en pipeline** ofrece un rendimiento DML por lotes **2–3× más rápido** que los drivers de la competencia.

---

## Comparativa con otros drivers

| Funcionalidad | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|---------|:---:|:---:|:---:|
| Licencia | **MIT** ✅ | MIT ✅ | GPLv2 ⚠️ |
| Dependencias | **0** (solo NewLife.Core) | 1 | 6 |
| IO Asíncrono real | ✅ | ✅ | ❌ |
| Lotes en pipeline | ✅ **Exclusivo** | ❌ | ❌ |
| Lotes por array | ✅ | ❌ | ❌ |
| Lotes por diccionario | ✅ | ❌ | ❌ |
| MySqlBulkCopy | ✅ | ✅ | ✅ |
| Compresión (zlib/zstd) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| Autenticación WebAuthn | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ Detección automática | ❌ | ❌ |
| Frameworks soportados | net45 ~ net10 | net462+ | net462+ |

---

## Rendimiento

> Entorno de prueba: .NET 10 + MySQL 8.0.39 localhost (Windows), TCP 127.0.0.1:3306

### DML por lotes: 10,000 filas (ms, menor es mejor)

| Operación | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) | Aceleración |
|------|------:|------:|------:|------:|
| INSERT | **899** | 1,927 | 1,906 | **2.1×** |
| UPDATE | **710** | 2,265 | 2,041 | **2.9×** |
| DELETE | **661** | 1,961 | 1,767 | **2.7×** |

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

## Licencia

MIT License — uso gratuito para proyectos personales y comerciales.

Copyright © 2002-2026 [NewLife](https://newlifex.com)
