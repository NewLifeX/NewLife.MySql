# NewLife.MySql — Reiner C#-Hochleistungs-MySQL-Treiber

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** ist ein reiner C#-MySQL-Client-Treiber des NewLife-Teams. Auf den standardisierten ADO.NET-Schnittstellen aufbauend implementiert er die MySQL-Protokollschicht (Protocol Version 10) direkt über TCP. Er hängt ausschließlich von `NewLife.Core` ab und bietet **null Drittanbieter-Abhängigkeiten**, **echtes durchgängiges Async** und eine **MIT-Lizenz** für sorgenfreie kommerzielle Nutzung.

Seine innovative **pipelinegestützte Batch-Ausführung** (Pipeline) liefert bei Batch-Schreibvorgängen im Bereich von Zehntausenden Zeilen eine **2×–3×**-Beschleunigung (gegenüber der zeilenweisen Basislinie) und ist damit die ideale Wahl für Big-Data-Batch-Operationen und Szenarien der inländischen (信创) Ablösung.

---

## Designziele

| Ziel | Beschreibung |
|------|------|
| **Leichtgewichtig und minimal** | Nur standardkonforme ADO.NET-Funktionen: CRUD, Transaktionen, parametrisierte Abfragen, gespeicherte Prozeduren und Batch-Operationen |
| **Hohe Leistung** | Keine zusätzlichen Speicherzuweisungen (`ArrayPool` / `OwnerPacket`), echtes asynchrones I/O, pipelinegestützte Batch-Ausführung |
| **Reiner inländischer (chinesischer) Stack** | Der Kerncode ist vollständig eigenständig kontrolliert, ohne GPL-Lizenzrisiko, geeignet für 信创-Umgebungen |
| **Big-Data-freundlich** | Wörterbuch-Parametersätze / Array-Bindung / Pipelining / Mehrzeilen-VALUES / DbBatch – von Zehntausenden bis zu Millionen Zeilen |
| **Breite Kompatibilität** | `net45` ~ `net10` über alle Versionen, MySQL 5.x ~ 9.0+, kompatibel mit OceanBase / TiDB / Doris / Aurora / CloudSQL sowie NativeAOT-Unterstützung ab `net8.0+` |

## Kernfunktionen

| Funktion | Beschreibung |
|------|------|
| Verbindungsverwaltung | MySQL 5.x / 8.0 / 9.0+, mehrere Authentifizierungsverfahren: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Unterstützt lokale Verbindungen über Unix Domain Socket ohne TCP-Overhead |
| Komprimierungsprotokoll | Unterstützt zlib-komprimierten Transport (`UseCompression`) zur Verringerung des Netzwerkbandbreitenbedarfs; zstd ist wegen der Vorgabe „null Drittanbieter-Abhängigkeiten“ zurückgestellt |
| Abfrageausführung | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, vollständige ADO.NET-Schnittstellen |
| Parametrisierte Abfragen | Clientseitige Parametersubstitution mit der Syntax `@parameter-name` und automatischer Maskierung zum Schutz vor Injection |
| Prepared Statements | `Prepare()` nutzt das binäre Protokoll `COM_STMT_PREPARE`, mit optionalem globalem Schalter `UseServerPrepare` |
| Batch-Operationen | `ExecuteBatch` / `ExecuteArrayBatch` / Pipelining / Mehrzeilen-VALUES / DbBatch / `MySqlBulkCopy` |
| Pipelinegestützte Ausführung | `Pipeline=true` sendet gebündelt und empfängt gebündelt, mit nur einem Netzwerk-Roundtrip: 35× bei Tausenden Zeilen und nahezu 100× bei Zehntausenden Zeilen |
| Transaktionen | `BeginTransaction` / `Commit` / `Rollback`, vier Isolationsstufen, automatischer Rollback |
| Gespeicherte Prozeduren | `CommandType.StoredProcedure`, unterstützt IN-/OUT-Parameter |
| SSL/TLS | `SslMode=Preferred/Required`, unterstützt TLS 1.2/1.3 |
| Verbindungspool | Standardmäßig aktiviert, Pooling je Verbindungszeichenfolge, konfigurierbare Lebensdauer und Idle-Validierung, automatische Zustandsprüfung und Wiederverwertung |
| Asynchrone Methoden | Durchgängiges `async/await`: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` usw. |
| Schema-Abfragen | Metadaten wie Tables / Columns / Indexes / Databases sowie erweiterte Index- und Fremdschlüsselabfragen |
| DbDataSource | `MySqlDataSource` im Stil von .NET 7+, unterstützt Dependency Injection |
| Tracing | Integrierte `ActivitySource`-Instrumentierung (Quellname `NewLife.MySql`), die Open-/Execute-/Commit-/Rollback-Spans nach den stabilen semantischen Konventionen von OpenTelemetry ausgibt, ohne Overhead, wenn keine Abonnenten vorhanden sind |
| DbBatch | Standard-ADO.NET-Batch-API ab .NET 6 |
| Räumliche Daten | MySQL-Geometry-Typen, `MySqlGeometry`-WKB-Wrapper |
| EF Core | Unterstützt über das separate Paket `NewLife.MySql.EntityFrameworkCore` |
| XCode-Integration | Automatische Registrierung über `MySqlClientFactory` und nahtlose Einbindung in das XCode-ORM |

---

## Highlights

### 🚀 Pipelinegestützte Batch-Ausführung (Pipeline)

Ein innovativer Pipeline-Modus auf Basis des binären Protokolls `COM_STMT_PREPARE + COM_STMT_EXECUTE`:

- **Phase 1**: Alle EXECUTE-Pakete werden aufgebaut und gebündelt gesendet, wobei nur das letzte Paket geflusht wird
- **Phase 2**: Alle OK-/Error-Antworten werden der Reihe nach gebündelt gelesen und die betroffene Zeilenzahl aufsummiert
- Der TCP-Stack fasst kleine Pakete zusammen (Nagle-Algorithmus), sodass nur ein Netzwerk-Roundtrip entsteht

**Messdaten** (.NET 10 + MySQL 8.0.26 localhost, Pipelining + Transaktion gegenüber zeilenweiser Basislinie):

| Szenario | Zeilenweise Basislinie | Pipeline(tx) | Beschleunigung |
|------|------:|------:|------:|
| INSERT mit 100 Zeilen | 244ms | 20ms | 12,2× |
| INSERT mit 1.000 Zeilen | 2.473ms | 70ms | **35,3×** |
| INSERT mit 10.000 Zeilen | ~70s¹ | ~0,7s¹ | ~98× |

> ¹ Der zeilenweise Wert für 10.000 Zeilen ohne Transaktion ist extrapoliert (2,4ms/Zeile); der Pipeline-Wert ist ohne Transaktion gemessen. Mit Transaktion benötigen 10.000 Zeilen etwa 70ms.

**Leistungsdaten im Drei-Treiber-Vergleich** (Batch-Operationen mit 10.000 Zeilen):

| Operation | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1.927ms | 1.906ms |
| UPDATE | 710ms | 2.265ms | 2.041ms |
| DELETE | 661ms | 1.961ms | 1.767ms |

> Bereits Tausende Zeilen erreichen im lokalen LAN eine **35×**-Beschleunigung; über Rechenzentren hinweg ist der Gewinn noch größer. Detaillierte Daten finden Sie im [Leistungstestbericht](Doc/性能测试报告.md).

### 🔧 Sechs Optionen für Batch-Operationen

| Option | API | Einsatzfall |
|------|-----|---------|
| Wörterbuch-Parametersätze | `ExecuteBatch` | Dynamische Parameter, die von einer Auflistung geliefert werden |
| Wertearray-Bindung | `ExecuteBatchValues` | Massenimport (intern von `MySqlBulkCopy`), ohne Allokationen für Wörterbuch-/Parameterobjekte |
| Array-Bindung | `ExecuteArrayBatch` | DML in großem Umfang, Oracle-Stil |
| Pipelining | `Pipeline=true` | Zehntausende / Hunderttausende Zeilen, über Rechenzentren hinweg |
| Mehrzeilen-VALUES | Verkettetes SQL | Reine INSERT-Batches |
| DbBatch (.NET 6+) | `CreateBatch` | Gemischte Batches unterschiedlicher SQL-Anweisungen |

### 🛡️ Null Drittanbieter-Abhängigkeiten

Hängt ausschließlich von `NewLife.Core` ab (ebenfalls vom NewLife-Team), ohne GPL-Lizenzrisiko, mit MIT-Lizenz für sorgenfreie kommerzielle Nutzung und geeignet für 信创-Umgebungen sowie Szenarien mit strengen Sicherheitsaudits.

### ⚡ Extreme Leistungsoptimierungen

- `ArrayPool<T>` + `OwnerPacket` für null zusätzliche Allokationen
- Vollständig gepoolte `Pool.StringBuilder` / `Pool.MemoryStream`
- Durchgängiges `async/await` ohne das Antipattern `sync-over-async`
- Schlanke Protokollanalyse, die nur die notwendigen Felder liest

### 🌐 Breite Kompatibilität

- **Frameworks**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+` unterstützt Trimming und NativeAOT (`IsAotCompatible`, mit einem `AotSmoke`-Rauchtest und einer CI-Matrix gängiger RIDs zur Regressionsabsicherung; erforderlich sind win-x64 / linux-x64 / linux-arm64; LoongArch linux-la64 wird per QEMU-Emulation abgedeckt (Beobachtungsphase: läuft, Fehler blockieren nicht)), siehe [AOT-Kompatibilitätsbewertung](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Kompatible Datenbanken**: OceanBase, TiDB, Doris (automatische Erkennung), Aurora / CloudSQL (Cloud-Datenbanken), MariaDB (grundlegend)
- **Bedingte Kompilierung**: wählt je Framework automatisch die optimale Implementierung

---

## Funktionsvergleich

| Funktion | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **Lizenz** | **MIT** | MIT | GPL (kostenpflichtig für kommerzielle Nutzung) |
| **Inländische (chinesische) Autonomie** | ✅ | ❌ | ❌ |
| **Drittanbieter-Abhängigkeiten** | **Keine** | Keine | Keine |
| **Echtes asynchrones I/O** | ✅ | ✅ | ❌ |
| **Pipelinegestützte Batch-Ausführung** | ✅ | ❌ | ❌ |
| **Array-gebundener Batch** | ✅ | ❌ | ❌ |
| **Batch mit Wörterbuch-Parametersätzen** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode-ORM-Integration** | ✅ | ❌ | ❌ |
| **Zielframeworks** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **OpenTelemetry-Tracing** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Automatische Erkennung | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Komprimierungsprotokoll (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn-Authentifizierung** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> Die vollständige Funktions- und Architekturanalyse finden Sie unter [Architektur](Doc/架构设计.md).

---

## Installation

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Schnellstart

### Verbindungszeichenfolge

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Parameter | Aliase | Standard | Beschreibung |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Serveradresse |
| Port | - | 3306 | Portnummer |
| Database | - | - | Datenbankname |
| UserID | Uid, User Id, User | - | Benutzername |
| Password | Pass, Pwd | - | Kennwort |
| ConnectionTimeout | Connection Timeout | 15 | Verbindungstimeout (Sekunden) |
| CommandTimeout | Default Command Timeout | 30 | Befehlstimeout (Sekunden) |
| SslMode | Ssl Mode | None | SSL-Modus (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Globaler Schalter für serverseitiges Prepare |
| Pipeline | Pipelining | false | Schalter für pipelinegestützte Ausführung |

### Verbindung öffnen

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Resultset abfragen

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();

while (reader.Read())
{
    var id = reader.GetInt64(0);
    var name = reader.GetString(1);
    var age = reader.GetInt64(2);
    Console.WriteLine($"{id}: {name}, {age}");
}
```

### Skalarwert abfragen

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### DML ausführen

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Parametrisierte Abfragen

Verwenden Sie die Syntax `@parameter-name`, um Parameter zu binden; die Werte werden automatisch maskiert, um Injection zu verhindern:

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);

using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine(reader.GetString(0));
}
```

```csharp
// Parameterized insert
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age, created) VALUES(@name, @age, @dt)");
cmd.Parameters.AddWithValue("name", "Alice");
cmd.Parameters.AddWithValue("age", 30);
cmd.Parameters.AddWithValue("dt", DateTime.Now);
cmd.ExecuteNonQuery();
```

```csharp
// Passing NULL values
cmd.Parameters.AddWithValue("email", DBNull.Value);
```

**Unterstützte Parametertypen**:

| .NET-Typ | SQL-Literal-Beispiel |
|-----------|---------------|
| `String` | `'hello'` (Sonderzeichen werden automatisch maskiert) |
| `Int32` / `Int64` und andere numerische Typen | `42` |
| `Boolean` | `1` oder `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | Geometriedaten im WKB-Format |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Wird in eine Zahl umgewandelt |
| `null` / `DBNull.Value` | `NULL` |

---

## Transaktionen

```csharp
using var conn = new MySqlConnection(connStr);
conn.Open();

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

Vier Isolationsstufen werden unterstützt: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (Standard) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

Wird eine Transaktion verworfen, ohne dass sie bestätigt oder zurückgerollt wurde, wird sie automatisch zurückgerollt.

---

## Gespeicherte Prozeduren

```csharp
using var cmd = new MySqlCommand { Connection = conn, CommandType = CommandType.StoredProcedure };
cmd.CommandText = "my_proc";
cmd.Parameters.AddWithValue("p_id", 1);

var outParam = new MySqlParameter { ParameterName = "p_result", Direction = ParameterDirection.Output };
cmd.Parameters.Add(outParam);

cmd.ExecuteNonQuery();
var result = outParam.Value; // Output parameter value
```

---

## Prepared Statements

Mit `Prepare()` wird das SQL auf dem Server kompiliert; nachfolgende Ausführungen nutzen das binäre Protokoll:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 25);

cmd.Prepare(); // Server-side prepare

cmd.ExecuteNonQuery(); // First execution

// Execute again after changing parameters (no recompilation needed)
cmd.Parameters[0].Value = "Jerry";
cmd.Parameters[1].Value = 30;
cmd.ExecuteNonQuery();
```

Sie können es auch global über die Verbindungszeichenfolge aktivieren, sodass jede parametrisierte Abfrage automatisch das binäre Protokoll verwendet:

```
UseServerPrepare=true;
```

---

## Batch-Operationen

### Option A: Wörterbuch-Parametersätze

Dieselbe SQL-Anweisung mit mehreren gebundenen Parametersätzen ausführen; intern wird automatisch ein Prepare durchgeführt:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", "");
cmd.Parameters.AddWithValue("age", 0);

var paramSets = new List<IDictionary<String, Object?>>
{
    new Dictionary<String, Object?> { ["name"] = "Alice", ["age"] = 25 },
    new Dictionary<String, Object?> { ["name"] = "Bob", ["age"] = 30 },
    new Dictionary<String, Object?> { ["name"] = "Charlie", ["age"] = 22 },
};
var totalAffected = cmd.ExecuteBatch(paramSets);
// or await cmd.ExecuteBatchAsync(paramSets);
```

### Option B: Array-Bindung (Oracle-Stil)

Legen Sie die Parameterwerte als Arrays an und geben Sie die Ausführungsanzahl an:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Option C: Pipelinegestützte Ausführung

Aktivieren Sie `Pipeline=true` in der Verbindungszeichenfolge, um gebündelt zu senden und gebündelt zu empfangen, mit nur einem Netzwerk-Roundtrip:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

Pipelining funktioniert für INSERT / UPDATE / DELETE und lässt sich mit Wörterbuch-Parametersätzen oder Array-Bindung kombinieren. Je höher die Netzwerklatenz (etwa über Rechenzentren hinweg), desto größer der Gewinn.

### Option D: Mehrzeilen-VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Option E: DbBatch-API (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core-Unterstützung

Das Paket `NewLife.MySql.EntityFrameworkCore` stellt einen EF Core-Datenbankanbieter für .NET 6 / .NET 8 / .NET 10 bereit.

```shell
dotnet add package NewLife.MySql.EntityFrameworkCore
```

```csharp
public class AppDbContext : DbContext
{
    public DbSet<User> Users { get; set; } = null!;

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>(entity =>
        {
            entity.ToTable("users");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Name).HasMaxLength(100).IsRequired();
        });
    }
}

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseMySql("Server=localhost;Database=mydb;User Id=root;******;")
    .Options;

using var context = new AppDbContext(options);
```

**Unterstützte Funktionen**:

| Funktion | Beschreibung |
|------|------|
| Typzuordnung | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Abfrageübersetzung | LINQ → MySQL-Syntax (Backticks, LIMIT/OFFSET) |
| Methodenübersetzung | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Memberübersetzung | DateTime.Year / Month / Day / Hour / Minute / Second |
| Migrationsunterstützung | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Strategie zur Wertgenerierung | AUTO_INCREMENT-Identitätsschlüssel |

---

## SSL/TLS-verschlüsselte Verbindungen

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Verhalten |
|---------|------|
| `None` / `Disabled` | Kein SSL verwenden (Standard) |
| `Preferred` | Verschlüsselung bevorzugen, bei fehlender Unterstützung auf Klartext zurückfallen |
| `Required` | Verschlüsselung erzwingen |

---

## Abfragen von Schema-Informationen

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Asynchrone Methoden

Alle Kernoperationen unterstützen asynchrone Varianten:

```csharp
using var conn = new MySqlConnection(connStr);
await conn.OpenAsync();

using var cmd = new MySqlCommand(conn, "SELECT * FROM users");
using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## Tracing (OpenTelemetry)

Der Treiber bringt eine integrierte `ActivitySource`-Instrumentierung mit (Quellname `NewLife.MySql`) und gibt vier Arten von Spans aus: `Open` (Verbindung), `Execute` (Befehl, umfasst die Lebensdauer des Readers), `Commit` / `Rollback` (Transaktionen). Die Attribute folgen den stabilen semantischen Datenbankkonventionen von OpenTelemetry (`db.system.name` / `db.query.text` / `server.address` / `error.type` usw.). Das OpenTelemetry-SDK wird vom Nutzer bereitgestellt; der Treiber fügt keine Abhängigkeiten hinzu:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Geltungsbereich**: `net6.0` und höher; auf niedrigeren Versionen und ohne Abonnenten ist es ein No-Op ohne Overhead
- **Hinweis zum Datenschutz**: `db.query.text` zeichnet den Befehlstext auf; bevorzugen Sie parametrisierte Abfragen, damit keine Literale in die Telemetrie gelangen

---

## Verbindungspool

Der Verbindungspool ist standardmäßig aktiviert und erfordert keine zusätzliche Konfiguration. Jede eindeutige Verbindungszeichenfolge wird einem eigenen unabhängigen Pool zugeordnet.

- Beim Öffnen werden Verbindungen automatisch aus dem Pool entnommen
- Beim Schließen werden Verbindungen automatisch zurückgegeben (die TCP-Verbindung wird nicht getrennt)
- Ungültige Verbindungen werden automatisch entfernt
- Neue Verbindungen werden bei Bedarf erstellt
- **Automatische Wiederverbindung nach Netzwerkunterbrechung**: Bei erkannt getrennter Verbindung wird automatisch erneut versucht, was die Stabilität in langlebigen Szenarien erhöht

### Parameter des Verbindungspools

| Parameter | Standard | Beschreibung |
|------|--------|------|
| MinPoolSize | 0 | Minimale Anzahl von Verbindungen |
| MaxPoolSize | 100 | Maximale Anzahl von Verbindungen |
| ConnectionLifeTime | 0 | Lebensdauer einer Verbindung (Sekunden); 0 bedeutet unbegrenzt |
| ConnectionIdleTime | 300 | Idle-Timeout (Sekunden); danach werden Verbindungen zurückgewonnen |
| ValidationInterval | 30 | Intervall der Idle-Validierung (Sekunden) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode-ORM-Integration

Wird über `MySqlClientFactory` automatisch als MySQL-Treiber für XCode registriert:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Datenbank wechseln

### ChangeDatabase (empfohlen)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ Ein Wechsel mitten in einer Transaktion wird nicht unterstützt (die Transaktion geht verloren); verwenden Sie für jede Datenbank eine eigene Verbindung.

### SqlClient.SetDatabaseAsync (Low-Level-API)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

Schließt die Verbindung nicht; wechselt nur die aktuelle Datenbank auf dem Server, geeignet für temporäre Abfrageszenarien.

---

## OceanBase / TiDB-Unterstützung

Der Treiber erkennt den Datenbanktyp automatisch anhand der Versionszeichenfolge im Handshake-Paket; die Verbindungsherstellung ist genau dieselbe wie bei MySQL:

```csharp
// OceanBase
var connStr = "Server=oceanbase-host;Port=2881;Database=test;User Id=root;******;";
using var conn = new MySqlConnection(connStr);
conn.Open();
Console.WriteLine($"Database type: {conn.DatabaseType}");  // OceanBase

// TiDB
var connStr2 = "Server=tidb-host;Port=4000;Database=test;User Id=root;******;";
using var conn2 = new MySqlConnection(connStr2);
conn2.Open();
Console.WriteLine($"Database type: {conn2.DatabaseType}");  // TiDB
```

Einfaches SQL, Transaktionen, parametrisierte Abfragen und Batch-Operationen sind vollständig kompatibel.

---

## Zielframeworks

| Framework | Beschreibung |
|------|------|
| `net45` / `net461` | .NET Framework, kompatibel mit Legacy-Projekten |
| `netstandard2.0` / `netstandard2.1` | Plattformübergreifende Kompatibilität, unterstützt asynchrones Dispose |
| `net6.0` | Unterstützt zusätzlich die DbBatch-API |
| `net10.0` | Neuestes .NET, optimale Leistung |

---

## NativeAOT-Bereitstellung

Mit den Zielframeworks `net8.0+` hat der Treiber die echte NativeAOT-Publish-Validierung bestanden (lokale Messungen und CI unter win-x64 und linux-x64; alle 26 Rauchtest-Szenarien mit 0 ILC-Warnungen). Das Veröffentlichen nativer Binärdateien erfordert **keine Anpassungen auf der Aufruferseite**; die CI schützt kontinuierlich vor Regressionen über eine Matrix gängiger RIDs (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, plus einen QEMU-emulierten Rauchtest für LoongArch linux-la64 während der Beobachtungsphase, der nicht blockierend ist):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Punkt | Inhalt |
|------|------|
| Geltungsbereich | `net8.0+`; `net6.0` erfordert einen experimentellen Schalter, frühere Frameworks werden nicht unterstützt |
| Verifizierte Fähigkeiten | Verbindung/Authentifizierung (einschließlich Handshake-Verbindungsattributen), synchrone und asynchrone Abfragen, serverseitiges Prepare, Batch (Wörterbuch/Array/Wertearray/pipelinegestützt), Transaktionen, gespeicherte Prozeduren, Schema-Metadaten, DbDataSource, DataAdapter, MySqlBulkCopy, Komprimierungsprotokoll, binlog-Abonnement, Tracing |
| Hinweise | Die Familie `MySqlDataAdapter` Fill/FillSchema/Update trägt `RequiresUnreferencedCode`-Annotationen; getrimmte Anwendungen erklären die Erhaltungspflicht an der Aufrufstelle gemäß der System.Data-Semantik |
| Ausführliche Bewertung | [AOT-Kompatibilitätsbewertung](Doc/AOT兼容性评估.md) |

---

## Hinweise

1. **MySQL-Version**: 5.x und höher unterstützt, 8.0+ empfohlen
2. **Zeichenkodierung**: standardmäßig UTF-8
3. **ChangeDatabase**: über Close/Reopen implementiert; ein Wechsel mitten in einer Transaktion wird nicht unterstützt, verwenden Sie daher eine eigene Verbindung
4. **Pipelinegestützte Ausführung**: nur für Batch-DML-Operationen, nicht für SELECT-Abfragen

---

## Verwandte Dokumentation

| Dokument | Beschreibung |
|------|------|
| [Anforderungen](Doc/需求文档.md) | Vision, Kernziele, Funktionsrichtungen |
| [Funktionsliste](Doc/功能清单.md) | Dreidimensionale Statusverfolgung von Implementierung/Tests/Kommentaren |
| [Architektur](Doc/架构设计.md) | Architekturüberblick, Protokollimplementierung, Technologieauswahl, Designentscheidungen, Treibervergleich |
| [Leistungstestbericht](Doc/性能测试报告.md) | Detaillierte Benchmark-Daten für Batch-Operationen |
| [Changelog (中文)](Doc/ChangeLog.md) | Chinesisches Versionsprotokoll |
| [Changelog (EN)](Doc/ChangeLog.en.md) | Englisches Versionsprotokoll |
| [简体中文 README](README.zh-CN.md) | Chinesische Dokumentation, im selben Batch wie diese englische Version gepflegt |
| [Migrationsleitfaden: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Schritte und Hinweise zur Migration vom offiziellen Oracle-Treiber |
| [Migrationsleitfaden: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Schritte und Hinweise zur Migration vom gängigen Community-Treiber |

## Ökosystem-Karte

NewLife ist ein geschichtetes Ökosystem unabhängig versionierter Open-Source-Projekte. Die folgende Karte wird wortgleich in allen NewLife-Repositories verwendet; jedes Projekt verlinkt auf sein Repository und sein primäres NuGet-Paket.

| Projekt | Funktion | Downloads |
| --- | --- | --- |
| **Grundlage** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Protokollierung, Konfiguration, Caching, Netzwerk, Serialisierung, Zeitplanung und APM-Tracing. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Daten-Middleware: einzelne Tabellen in der Größenordnung von zig Milliarden Zeilen, automatisches Sharding, Lese-/Schreibtrennung, über 10 Datenbanken. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Hochperformante Netzwerkbibliothek: 1,77 Mio. pps / 14,5 Gbps auf einer Maschine (TCP 1KB, 2026-10), 4 Mio. langlebige TCP-Verbindungen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Protokollkommunikation und RPC-Framework für Client/Server-Anwendungen und IoT-Geräte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Installiert .NET-Anwendungen als Betriebssystem-Dienste: Windows-Dienste und Linux-systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new`-Projektvorlagen für Konsole, Web, API, Dienst, Daten und IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Daten & Messaging** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis-Client mit Mikrosekunden-Latenz, Millionen Operationen pro Sekunde und integrierten Nachrichtenwarteschlangen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Vollständig verwalteter RocketMQ-Client für Apache RocketMQ und Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT-Client und -Server, einschließlich Alibaba Cloud IoT-Anbindung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Hybriddatenbank in C#: eingebettet oder im Netzwerk, relational, Zeitreihen, Nachrichtenwarteschlange und KV in einem. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | MySql-Treiber und Hilfsschicht auf Basis des NewLife-Datenstacks. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, KI & verteilt** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Schnelles Framework für Administrationsplattformen: Berechtigungen, SSO, OAuth-Server und CRUD-Gerüst. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Verteilte Dienstplattform: Knotenverwaltung, APM, Konfigurationszentrum, Registry und Release-Center. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plattform für verteilte Aufgaben und Echtzeit-Datenplanung mit Sharding, Wiederholungen und Backfill. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Einheitlicher Client für 46 LLM-Anbieter plus ChatAI, eine sofort einsatzbereite Web-Chat-Anwendung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrie & IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | IoT-Standardbibliothek mit Protokollverträgen und Geräteabstraktionen. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Modbus-TCP/RTU/ASCII-Implementierung auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Siemens-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Omron-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Mitsubishi-Melsec-SPS-Protokoll auf Basis von NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | OPC-Client für industriellen Datenzugriff. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet-Protokollstack für die Gebäudeautomation. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Leichtgewichtige Open-Source-IoT-Plattform mit Gateway- und Geräteverwaltung. | — |
| **Fahrzeugtelematik** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T-808-Fahrzeugortungsprotokoll: über 70 Nachrichtenkörper, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T-809-Plattform-zu-Plattform-Protokoll für Verkehrsbehörden. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T-1078-Fahrzeugvideo: RTP-Parsing, FLV/fMP4-Remuxing und Live-Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Domänenkomponenten** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Ein Paket für Excel, Word, PPT, PDF und über 15 Dokumentformate. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Plattformübergreifendes Audio: Codecs, Geräte-I/O, DSP, Container und Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Videocodecs, Containerformate, Frame-Verarbeitung und Streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Einheitliche Schnittstelle für Baidu, AMap, Tencent und Tianditu mit Geokodierung und Koordinatenumrechnung. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4-Geolokalisierung mit einer offline speichergemappten Datenbank und einer Online-API als Fallback. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Werkzeuge & Produkte** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Entwickler-Toolbox: Netzwerk, serielle Schnittstelle, Krypto, Regex, Modbus und MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Selbst gehostetes Git-Quellcode-Verwaltungssystem. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Installiert automatisch die passende .NET-Laufzeit, wenn es von einem älteren Host gestartet wird. | — |

> Die Badges zeigen das primäre Paket jedes Projekts; Theme-, Erweiterungs- und Satellitenpakete sind auf den jeweiligen NuGet-Seiten aufgeführt. Die hier nicht aufgeführten über 30 Pakete (FTP, LoRa, Office-Erweiterungen, Protokolltreiber und mehr) sind in der [NewLifeX-Organisation](https://github.com/NewLifeX) und auf [newlifex.com](https://newlifex.com) verfügbar.

## Team

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Das NewLife-Team wurde 2002 gegründet und arbeitet seit mehr als zwei Jahrzehnten am .NET- und IoT-Ökosystem. Es betreut nahezu 100 Open-Source-Projekte (34 davon sind Kern dieses Ökosystems) und liefert integrierte Hard- und Softwarelösungen für die Bereiche Energie, Logistik, Gesundheitswesen, Industrieautomation und Internet. Unser Leitspruch lautet „Lernen kennt kein Dienstalter – wer es beherrscht, wird zum Lehrmeister“.

Die Open-Source-Komponenten wurden <!--nuget-total:start-->6.48M<!--nuget-total:end--> Mal von NuGet heruntergeladen (ökosystemweit, Stand 2026-10).

Website: https://newlifex.com · Open-Source-Organisation: https://github.com/NewLifeX

<details>
<summary>WeChat-Offizieller Account: 智能大石头 (zum Folgen scannen)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
