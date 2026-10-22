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

**NewLife.MySql** es un driver cliente de MySQL en C# puro del equipo NewLife. Construido sobre las interfaces estándar de ADO.NET, implementa la capa de protocolo de MySQL (Protocol Version 10) directamente sobre TCP. Solo depende de `NewLife.Core`, con **cero dependencias de terceros**, **asincronía real de extremo a extremo** y **licencia MIT** para un uso comercial sin preocupaciones.

Su innovadora **ejecución por lotes en pipeline** (Pipeline) ofrece una aceleración de **2×~3×** en escrituras por lotes del orden de decenas de miles de filas (frente a la línea base fila por fila), lo que la convierte en una opción ideal para operaciones por lotes de big data y escenarios de sustitución nacional (信创).

---

## Objetivos de diseño

| Objetivo | Descripción |
|------|------|
| **Ligero y minimalista** | Solo CRUD estándar de ADO.NET, transacciones, consultas parametrizadas, procedimientos almacenados y operaciones por lotes |
| **Alto rendimiento** | Cero asignaciones de memoria adicionales (`ArrayPool` / `OwnerPacket`), E/S asíncrona real, ejecución por lotes en pipeline |
| **Pila nacional china (信创)** | El código central es totalmente autocontrolado, sin riesgo de licencia GPL, apto para entornos 信创 |
| **Amigable para big data** | Conjuntos de parámetros por diccionario / enlace de arrays / pipeline / VALUES de varias filas / DbBatch, cubriendo desde decenas de miles hasta millones de filas |
| **Amplia compatibilidad** | `net45` ~ `net10` en todas las versiones, MySQL 5.x ~ 9.0+, compatible con OceanBase / TiDB / Doris / Aurora / CloudSQL, y soporte NativeAOT en `net8.0+` |

## Funciones principales

| Función | Descripción |
|------|------|
| Gestión de conexiones | MySQL 5.x / 8.0 / 9.0+, múltiples métodos de autenticación: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Admite conexiones locales mediante Unix Domain Socket con cero sobrecoste de TCP |
| Protocolo de compresión | Admite transporte comprimido con zlib (`UseCompression`) para reducir el consumo de ancho de banda; zstd queda aplazado por la restricción de cero dependencias de terceros |
| Ejecución de consultas | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, interfaces ADO.NET completas |
| Consultas parametrizadas | Sustitución de parámetros en el cliente con la sintaxis `@nombre-de-parámetro` y escape automático para prevenir inyecciones |
| Sentencias preparadas | `Prepare()` usa el protocolo binario `COM_STMT_PREPARE`, con un interruptor global opcional `UseServerPrepare` |
| Operaciones por lotes | `ExecuteBatch` / `ExecuteArrayBatch` / pipeline / VALUES de varias filas / DbBatch / `MySqlBulkCopy` |
| Ejecución en pipeline | `Pipeline=true` envía por lotes y recibe por lotes, con una sola ida y vuelta de red: 35× para miles de filas y casi 100× para decenas de miles de filas |
| Transacciones | `BeginTransaction` / `Commit` / `Rollback`, cuatro niveles de aislamiento, reversión automática |
| Procedimientos almacenados | `CommandType.StoredProcedure`, admite parámetros IN/OUT |
| SSL/TLS | `SslMode=Preferred/Required`, admite TLS 1.2/1.3 |
| Pool de conexiones | Habilitado por defecto, agrupado por cadena de conexión, con vida útil y validación en inactividad configurables, comprobación de estado y reciclaje automáticos |
| Métodos asíncronos | Cadena completa `async/await`: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync`, etc. |
| Consultas de esquema | Metadatos como Tables / Columns / Indexes / Databases, además de consultas ampliadas de índices y claves externas |
| DbDataSource | Estilo `MySqlDataSource` de .NET 7+, admite inyección de dependencias |
| Trazado | Instrumentación `ActivitySource` integrada (nombre de origen `NewLife.MySql`), que emite spans Open / Execute / Commit / Rollback siguiendo las convenciones semánticas estables de OpenTelemetry, con coste cero cuando no hay suscriptores |
| DbBatch | API estándar de lotes de ADO.NET de .NET 6+ |
| Datos espaciales | Tipos Geometry de MySQL, envoltorio WKB `MySqlGeometry` |
| EF Core | Compatible mediante el paquete independiente `NewLife.MySql.EntityFrameworkCore` |
| Integración con XCode | Registro automático mediante `MySqlClientFactory`, integración perfecta con el ORM XCode |

---

## Aspectos destacados

### 🚀 Ejecución por lotes en pipeline (Pipeline)

Un innovador modo de pipeline basado en el protocolo binario `COM_STMT_PREPARE + COM_STMT_EXECUTE`:

- **Fase 1**: Construir y enviar todos los paquetes EXECUTE por lotes, vaciando solo el último paquete
- **Fase 2**: Leer todas las respuestas OK/Error en orden y por lotes, acumulando el número de filas afectadas
- La pila TCP agrupa los paquetes pequeños (algoritmo de Nagle), por lo que solo hay una ida y vuelta de red

**Datos medidos** (.NET 10 + MySQL 8.0.26 localhost, pipeline + transacción frente a la línea base fila por fila):

| Escenario | Línea base fila por fila | Pipeline(tx) | Aceleración |
|------|------:|------:|------:|
| INSERT de 100 filas | 244ms | 20ms | 12,2× |
| INSERT de 1.000 filas | 2.473ms | 70ms | **35,3×** |
| INSERT de 10.000 filas | ~70s¹ | ~0,7s¹ | ~98× |

> ¹ La cifra de 10.000 filas fila por fila sin transacción es extrapolada (2,4ms/fila); la cifra de Pipeline se mide sin transacción. Con transacción, 10.000 filas tardan unos 70ms.

**Datos de rendimiento de tres drivers** (operaciones por lotes de 10.000 filas):

| Operación | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1.927ms | 1.906ms |
| UPDATE | 710ms | 2.265ms | 2.041ms |
| DELETE | 661ms | 1.961ms | 1.767ms |

> Miles de filas ya logran una aceleración de **35×** en una LAN local; el beneficio es aún mayor entre centros de datos. Consulta el [Informe de pruebas de rendimiento](Doc/性能测试报告.md) para ver los datos detallados.

### 🔧 Seis opciones de operaciones por lotes

| Opción | API | Caso de uso |
|------|-----|---------|
| Conjuntos de parámetros por diccionario | `ExecuteBatch` | Parámetros dinámicos proporcionados por una colección |
| Enlace de arrays de valores | `ExecuteBatchValues` | Importación masiva (interno de `MySqlBulkCopy`), cero asignaciones de diccionarios u objetos de parámetro |
| Enlace de arrays | `ExecuteArrayBatch` | DML de gran volumen, estilo Oracle |
| Pipeline | `Pipeline=true` | Decenas de miles / cientos de miles de filas, entre centros de datos |
| VALUES de varias filas | SQL concatenado | Lotes de solo INSERT |
| DbBatch (.NET 6+) | `CreateBatch` | Lotes mixtos de distintas sentencias SQL |

### 🛡️ Cero dependencias de terceros

Solo depende de `NewLife.Core` (también del equipo NewLife), sin riesgo de licencia GPL, con licencia MIT para un uso comercial sin preocupaciones y apto para entornos 信创 y escenarios con auditorías de seguridad estrictas.

### ⚡ Optimizaciones de rendimiento extremas

- `ArrayPool<T>` + `OwnerPacket` para cero asignaciones adicionales
- `Pool.StringBuilder` / `Pool.MemoryStream` totalmente agrupados en pool
- Cadena completa `async/await`, sin el antipatrón `sync-over-async`
- Análisis de protocolo ágil que lee solo los campos necesarios

### 🌐 Amplia compatibilidad

- **Frameworks**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+` admite recorte de código y NativeAOT (`IsAotCompatible`, con una prueba de humo `AotSmoke` y una matriz de RID principales en CI para proteger contra regresiones; win-x64 / linux-x64 / linux-arm64 obligatorios; linux-la64 de LoongArch está cubierto por emulación QEMU (periodo de observación: se ejecuta y los fallos no bloquean)), consulta la [Evaluación de compatibilidad AOT](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Bases de datos compatibles**: OceanBase, TiDB, Doris (detección automática), Aurora / CloudSQL (bases de datos en la nube), MariaDB (básico)
- **Compilación condicional**: selecciona automáticamente la implementación óptima para cada framework

---

## Comparativa de funciones

| Función | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **Licencia** | **MIT** | MIT | GPL (de pago para uso comercial) |
| **Autonomía nacional china** | ✅ | ❌ | ❌ |
| **Dependencias de terceros** | **Ninguna** | Ninguna | Ninguna |
| **E/S asíncrona real** | ✅ | ✅ | ❌ |
| **Ejecución por lotes en pipeline** | ✅ | ❌ | ❌ |
| **Lote con enlace de arrays** | ✅ | ❌ | ❌ |
| **Lote con conjuntos de parámetros por diccionario** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **Integración con XCode ORM** | ✅ | ❌ | ❌ |
| **Frameworks de destino** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **Trazado OpenTelemetry** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Detección automática | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Protocolo de compresión (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **Autenticación WebAuthn** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> Para el análisis completo de funciones y arquitectura, consulta la [Arquitectura](Doc/架构设计.md).

---

## Instalación

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Inicio rápido

### Cadena de conexión

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Parámetro | Alias | Valor predeterminado | Descripción |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Dirección del servidor |
| Port | - | 3306 | Número de puerto |
| Database | - | - | Nombre de la base de datos |
| UserID | Uid, User Id, User | - | Nombre de usuario |
| Password | Pass, Pwd | - | Contraseña |
| ConnectionTimeout | Connection Timeout | 15 | Tiempo de espera de conexión (segundos) |
| CommandTimeout | Default Command Timeout | 30 | Tiempo de espera de comando (segundos) |
| SslMode | Ssl Mode | None | Modo SSL (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Interruptor global de preparación en el servidor |
| Pipeline | Pipelining | false | Interruptor de ejecución en pipeline |

### Abrir una conexión

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Consultar un conjunto de resultados

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

### Consultar un escalar

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### Ejecutar DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Consultas parametrizadas

Usa la sintaxis `@nombre-de-parámetro` para enlazar parámetros; los valores se escapan automáticamente para evitar inyecciones:

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

**Tipos de parámetros admitidos**:

| Tipo .NET | Ejemplo de literal SQL |
|-----------|---------------|
| `String` | `'hello'` (los caracteres especiales se escapan automáticamente) |
| `Int32` / `Int64` y otros tipos numéricos | `42` |
| `Boolean` | `1` o `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | Datos de geometría en formato WKB |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Se convierte a un número |
| `null` / `DBNull.Value` | `NULL` |

---

## Transacciones

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

Se admiten cuatro niveles de aislamiento: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (predeterminado) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

Si una transacción se libera sin confirmarla ni revertirla, se revierte automáticamente.

---

## Procedimientos almacenados

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

## Sentencias preparadas

Usa `Prepare()` para compilar el SQL en el servidor; las ejecuciones posteriores usan el protocolo binario:

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

También puedes habilitarlo globalmente mediante la cadena de conexión, de modo que cada consulta parametrizada use automáticamente el protocolo binario:

```
UseServerPrepare=true;
```

---

## Operaciones por lotes

### Opción A: Conjuntos de parámetros por diccionario

Ejecuta el mismo SQL con varios conjuntos de parámetros enlazados; se prepara internamente de forma automática:

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

### Opción B: Enlace de arrays (estilo Oracle)

Establece los valores de los parámetros como arrays e indica el número de ejecuciones:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Opción C: Ejecución en pipeline

Habilita `Pipeline=true` en la cadena de conexión para enviar por lotes y recibir por lotes con una sola ida y vuelta de red:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

El pipeline funciona para INSERT / UPDATE / DELETE y puede combinarse con conjuntos de parámetros por diccionario o con enlace de arrays. Cuanto mayor sea la latencia de red (por ejemplo, entre centros de datos), mayor será el beneficio.

### Opción D: VALUES de varias filas

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Opción E: API DbBatch (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## Soporte de EF Core

El paquete `NewLife.MySql.EntityFrameworkCore` proporciona un proveedor de base de datos de EF Core compatible con .NET 6 / .NET 8 / .NET 10.

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

**Funciones admitidas**:

| Función | Descripción |
|------|------|
| Mapeo de tipos | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Traducción de consultas | LINQ → sintaxis de MySQL (comillas invertidas, LIMIT/OFFSET) |
| Traducción de métodos | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Traducción de miembros | DateTime.Year / Month / Day / Hour / Minute / Second |
| Soporte de migraciones | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Estrategia de generación de valores | Claves primarias de identidad AUTO_INCREMENT |

---

## Conexiones cifradas SSL/TLS

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Comportamiento |
|---------|------|
| `None` / `Disabled` | No usar SSL (predeterminado) |
| `Preferred` | Preferir cifrado y volver a texto plano si no es compatible |
| `Required` | Forzar cifrado |

---

## Consultas de información de esquema

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Métodos asíncronos

Todas las operaciones principales admiten versiones asíncronas:

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

## Trazado (OpenTelemetry)

El driver incluye instrumentación `ActivitySource` integrada (nombre de origen `NewLife.MySql`), que emite cuatro tipos de spans: `Open` (conexión), `Execute` (comando, que cubre la vida útil del reader), `Commit` / `Rollback` (transacciones). Los atributos siguen las convenciones semánticas estables de bases de datos de OpenTelemetry (`db.system.name` / `db.query.text` / `server.address` / `error.type`, etc.). El SDK de OpenTelemetry lo aporta el consumidor; el driver no añade dependencias:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Ámbito**: `net6.0` y superiores; en versiones inferiores y sin suscriptores es una operación nula de coste cero
- **Nota de privacidad**: `db.query.text` registra el texto del comando; prefiere consultas parametrizadas para que los literales no entren en la telemetría

---

## Pool de conexiones

El pool de conexiones está habilitado de forma predeterminada y no necesita configuración adicional. Cada cadena de conexión única se asigna a su propio pool independiente.

- Las conexiones se toman del pool automáticamente al abrirlas
- Las conexiones se devuelven automáticamente al cerrarlas (la conexión TCP no se descarta)
- Las conexiones no válidas se eliminan automáticamente
- Las conexiones nuevas se crean bajo demanda
- **Reconexión automática tras una desconexión de red**: reintenta automáticamente cuando detecta una conexión caída, lo que mejora la estabilidad en escenarios de larga duración

### Parámetros del pool de conexiones

| Parámetro | Valor predeterminado | Descripción |
|------|--------|------|
| MinPoolSize | 0 | Número mínimo de conexiones |
| MaxPoolSize | 100 | Número máximo de conexiones |
| ConnectionLifeTime | 0 | Vida útil de la conexión (segundos); 0 significa que nunca caduca |
| ConnectionIdleTime | 300 | Tiempo de espera de inactividad (segundos); las conexiones se recuperan al transcurrir |
| ValidationInterval | 30 | Intervalo de validación en inactividad (segundos) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## Integración con XCode ORM

Se registra automáticamente como driver MySQL para XCode mediante `MySqlClientFactory`:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Cambio de base de datos

### ChangeDatabase (recomendado)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ No se admite el cambio en medio de una transacción (la transacción se pierde); crea una conexión independiente para cada base de datos.

### SqlClient.SetDatabaseAsync (API de bajo nivel)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

No cierra la conexión; solo cambia la base de datos actual en el servidor, adecuado para escenarios de consultas temporales.

---

## Soporte de OceanBase / TiDB

El driver detecta automáticamente el tipo de base de datos a partir de la cadena de versión del paquete de handshake; el método de conexión es exactamente el mismo que para MySQL:

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

El SQL básico, las transacciones, las consultas parametrizadas y las operaciones por lotes son totalmente compatibles.

---

## Frameworks de destino

| Framework | Descripción |
|------|------|
| `net45` / `net461` | .NET Framework, compatible con proyectos heredados |
| `netstandard2.0` / `netstandard2.1` | Compatibilidad multiplataforma, admite Dispose asíncrono |
| `net6.0` | Admite además la API DbBatch |
| `net10.0` | Último .NET, rendimiento óptimo |

---

## Despliegue con NativeAOT

Con los frameworks de destino `net8.0+`, el driver ha superado la validación real de publicación NativeAOT (mediciones locales y CI en win-x64 y linux-x64; los 26 escenarios de humo pasaron con 0 advertencias de ILC). Publicar binarios nativos **no requiere parches en el lado del llamador**; la CI protege contra regresiones de forma continua en una matriz de RID principales (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, además de una prueba de humo emulada con QEMU para LoongArch linux-la64 durante el periodo de observación, que no bloquea):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Elemento | Contenido |
|------|------|
| Ámbito | `net8.0+`; `net6.0` requiere un interruptor experimental y los frameworks anteriores no son compatibles |
| Capacidades verificadas | Conexión/autenticación (incluidos los atributos de conexión del handshake), consultas sincrónicas y asíncronas, preparación en el servidor, lotes (diccionario/array/array de valores/pipeline), transacciones, procedimientos almacenados, metadatos de esquema, DbDataSource, DataAdapter, MySqlBulkCopy, protocolo de compresión, suscripción binlog, trazado |
| Notas | La familia Fill/FillSchema/Update de `MySqlDataAdapter` lleva anotaciones `RequiresUnreferencedCode`; las aplicaciones recortadas declaran la responsabilidad de preservación en el punto de llamada según la semántica de System.Data |
| Evaluación detallada | [Evaluación de compatibilidad AOT](Doc/AOT兼容性评估.md) |

---

## Notas

1. **Versión de MySQL**: se admite 5.x y superiores; se recomienda 8.0+
2. **Codificación de caracteres**: UTF-8 por defecto
3. **ChangeDatabase**: se implementa con Close/Reopen; no se admite el cambio en medio de una transacción, así que usa una conexión independiente
4. **Ejecución en pipeline**: solo para operaciones DML por lotes, no para consultas SELECT

---

## Documentación relacionada

| Documento | Descripción |
|------|------|
| [Requisitos](Doc/需求文档.md) | Visión, objetivos principales, direcciones funcionales |
| [Lista de funciones](Doc/功能清单.md) | Seguimiento tridimensional del estado de implementación/pruebas/comentarios |
| [Arquitectura](Doc/架构设计.md) | Visión general de la arquitectura, implementación del protocolo, decisiones tecnológicas, decisiones de diseño, comparativa de drivers |
| [Informe de pruebas de rendimiento](Doc/性能测试报告.md) | Datos detallados de benchmark de las operaciones por lotes |
| [Registro de cambios (中文)](Doc/ChangeLog.md) | Registro de versiones en chino |
| [Registro de cambios (EN)](Doc/ChangeLog.en.md) | Registro de versiones en inglés |
| [README en chino simplificado](README.zh-CN.md) | Documentación en chino, mantenida en el mismo lote que esta versión en inglés |
| [Guía de migración: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Pasos y notas para migrar desde el driver oficial de Oracle |
| [Guía de migración: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Pasos y notas para migrar desde el driver comunitario más extendido |

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

> Las insignias muestran el paquete principal de cada proyecto; los paquetes de temas, extensiones y satélites figuran en sus páginas de NuGet. Los más de 30 paquetes no listados aquí (FTP, LoRa, extensiones de Office, controladores de protocolo, etc.) están disponibles en la [organización NewLifeX](https://github.com/NewLifeX) y en [newlifex.com](https://newlifex.com).

## Equipo

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife se fundó en 2002 y lleva más de dos décadas dedicado al ecosistema .NET e IoT, mantiene cerca de 100 proyectos de código abierto (34 de ellos principales en este mapa) y ofrece soluciones integradas de software y hardware para los sectores de energía, logística, salud, control industrial e Internet. Nos guía el lema «en el aprendizaje no hay antigüedad: quien domina es maestro».

Los componentes de código abierto acumulan <!--nuget-total:start-->6.48M<!--nuget-total:end--> descargas en NuGet (ecosistema completo, a fecha de 2026-10).

Sitio web: https://newlifex.com · Organización de código abierto: https://github.com/NewLifeX

<details>
<summary>Cuenta oficial de WeChat: 智能大石头 (escanea para seguir)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
