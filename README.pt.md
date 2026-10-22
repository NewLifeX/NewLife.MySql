# NewLife.MySql — Driver MySQL de Alto Desempenho em C# Puro

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** é um driver cliente MySQL em C# puro da equipe NewLife. Construído sobre as interfaces padrão do ADO.NET, implementa a camada de protocolo MySQL (Protocol Version 10) diretamente sobre TCP. Depende apenas de `NewLife.Core`, com **zero dependências de terceiros**, **async verdadeiramente de ponta a ponta** e **licença MIT** para uso comercial sem preocupações.

Sua inovadora **Execução em Lote com Pipeline** (Pipeline) entrega um ganho de **2×~3×** em gravações em lote na faixa de dezenas de milhares de linhas (em relação à linha de base linha por linha), o que a torna a escolha ideal para operações em lote de big data e cenários de substituição doméstica (信创).

---

## Objetivos de design

| Objetivo | Descrição |
|------|------|
| **Leve e minimalista** | Apenas CRUD padrão ADO.NET, transações, consultas parametrizadas, procedures armazenadas e operações em lote |
| **Alto desempenho** | Zero alocações extras de memória (`ArrayPool` / `OwnerPacket`), E/S assíncrona real, execução em lote com pipeline |
| **Stack doméstico (chinês) puro** | Código central totalmente autocontrolado, sem risco de licenciamento GPL, adequado a ambientes 信创 |
| **Amigável a big data** | Conjuntos de parâmetros por dicionário / binding de array / pipeline / VALUES multilinha / DbBatch, cobrindo de dezenas de milhares a milhões de linhas |
| **Ampla compatibilidade** | `net45` ~ `net10` em todas as versões, MySQL 5.x ~ 9.0+, compatível com OceanBase / TiDB / Doris / Aurora / CloudSQL, e suporte a NativeAOT em `net8.0+` |

## Recursos principais

| Recurso | Descrição |
|------|------|
| Gerenciamento de conexões | MySQL 5.x / 8.0 / 9.0+, vários métodos de autenticação: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | Suporta conexões locais por Unix Domain Socket, sem overhead de TCP |
| Protocolo de compressão | Suporta transporte comprimido com zlib (`UseCompression`) para reduzir o consumo de banda de rede; zstd fica adiado por causa da restrição de zero dependências de terceiros |
| Execução de consultas | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, interfaces ADO.NET completas |
| Consultas parametrizadas | Substituição de parâmetros no cliente com a sintaxe `@nome-do-parametro` e escape automático para evitar injeção |
| Prepared statements | `Prepare()` usa o protocolo binário `COM_STMT_PREPARE`, com uma opção global `UseServerPrepare` |
| Operações em lote | `ExecuteBatch` / `ExecuteArrayBatch` / pipeline / VALUES multilinha / DbBatch / `MySqlBulkCopy` |
| Execução com pipeline | `Pipeline=true` envia em lote e recebe em lote, com apenas uma ida e volta de rede: 35× para milhares de linhas e quase 100× para dezenas de milhares de linhas |
| Transações | `BeginTransaction` / `Commit` / `Rollback`, quatro níveis de isolamento, rollback automático |
| Procedures armazenadas | `CommandType.StoredProcedure`, suporta parâmetros IN/OUT |
| SSL/TLS | `SslMode=Preferred/Required`, suporta TLS 1.2/1.3 |
| Pool de conexões | Habilitado por padrão, pool por string de conexão, tempo de vida e validação de ociosidade configuráveis, verificação de integridade e reciclagem automáticas |
| Métodos assíncronos | `async/await` em toda a cadeia: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync`, etc. |
| Consultas de schema | Metadados como Tables / Columns / Indexes / Databases, além de consultas estendidas de índices e chaves estrangeiras |
| DbDataSource | `MySqlDataSource` no estilo .NET 7+, com suporte a injeção de dependência |
| Rastreamento | Instrumentação `ActivitySource` integrada (nome da fonte `NewLife.MySql`), emitindo spans de Open / Execute / Commit / Rollback que seguem as convenções semânticas estáveis do OpenTelemetry, sem overhead quando não há assinantes |
| DbBatch | API de lote ADO.NET padrão do .NET 6+ |
| Dados espaciais | Tipos Geometry do MySQL, wrapper WKB `MySqlGeometry` |
| EF Core | Suportado pelo pacote separado `NewLife.MySql.EntityFrameworkCore` |
| Integração com XCode | Registro automático via `MySqlClientFactory`, integrando-se sem atrito ao ORM XCode |

---

## Destaques

### 🚀 Execução em Lote com Pipeline (Pipeline)

Um modo de pipeline inovador baseado no protocolo binário `COM_STMT_PREPARE + COM_STMT_EXECUTE`:

- **Fase 1**: monta e envia todos os pacotes EXECUTE em lote, com flush apenas do último pacote
- **Fase 2**: lê todas as respostas OK/Error em ordem, em lote, acumulando a contagem de linhas afetadas
- A pilha TCP agrupa pacotes pequenos (algoritmo de Nagle), então há apenas uma ida e volta de rede

**Dados medidos** (.NET 10 + MySQL 8.0.26 localhost, pipeline + transação vs linha de base linha por linha):

| Cenário | Linha de base linha por linha | Pipeline(tx) | Ganho |
|------|------:|------:|------:|
| INSERT de 100 linhas | 244ms | 20ms | 12,2× |
| INSERT de 1.000 linhas | 2.473ms | 70ms | **35,3×** |
| INSERT de 10.000 linhas | ~70s¹ | ~0,7s¹ | ~98× |

> ¹ O valor de 10.000 linhas linha por linha sem transação é extrapolado (2,4ms/linha); o valor de Pipeline foi medido sem transação. Com transação, 10.000 linhas levam cerca de 70ms.

**Dados de desempenho dos três drivers** (operações em lote de 10.000 linhas):

| Operação | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1.927ms | 1.906ms |
| UPDATE | 710ms | 2.265ms | 2.041ms |
| DELETE | 661ms | 1.961ms | 1.767ms |

> Milhares de linhas já alcançam **35×** de ganho em LAN local; o benefício é ainda maior entre data centers. Consulte o [Relatório de Testes de Desempenho](Doc/性能测试报告.md) para os dados detalhados.

### 🔧 Seis opções de operações em lote

| Opção | API | Caso de uso |
|------|-----|---------|
| Conjuntos de parâmetros por dicionário | `ExecuteBatch` | Parâmetros dinâmicos fornecidos por uma coleção |
| Binding de array de valores | `ExecuteBatchValues` | Importação em massa (interno do `MySqlBulkCopy`), zero alocações de dicionário/objeto de parâmetro |
| Binding de array | `ExecuteArrayBatch` | DML de grande volume, estilo Oracle |
| Pipeline | `Pipeline=true` | Dezenas de milhares / centenas de milhares de linhas, entre data centers |
| VALUES multilinha | SQL concatenado | Lotes somente de INSERT |
| DbBatch (.NET 6+) | `CreateBatch` | Lotes mistos de instruções SQL diferentes |

### 🛡️ Zero dependências de terceiros

Depende apenas de `NewLife.Core` (também da equipe NewLife), sem risco de licenciamento GPL, com licença MIT para uso comercial sem preocupações e adequado a ambientes 信创 e a cenários com auditorias de segurança rigorosas.

### ⚡ Otimizações extremas de desempenho

- `ArrayPool<T>` + `OwnerPacket` para zero alocações extras
- `Pool.StringBuilder` / `Pool.MemoryStream` totalmente reutilizados via pool
- `async/await` em toda a cadeia, sem o antipadrão `sync-over-async`
- Análise de protocolo enxuta que lê apenas os campos necessários

### 🌐 Ampla compatibilidade

- **Frameworks**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+` suporta trimming e NativeAOT (`IsAotCompatible`, com um teste de fumaça `AotSmoke` e uma matriz de RIDs mainstream no CI para proteção contra regressões; obrigatórios win-x64 / linux-x64 / linux-arm64; LoongArch linux-la64 é coberto por emulação QEMU (período de observação: ele executa, e falhas não bloqueiam)), consulte [Avaliação de Compatibilidade AOT](Doc/AOT兼容性评估.md)
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **Bancos compatíveis**: OceanBase, TiDB, Doris (detecção automática), Aurora / CloudSQL (bancos em nuvem), MariaDB (básico)
- **Compilação condicional**: seleciona automaticamente a implementação ideal para cada framework

---

## Comparativo de recursos

| Recurso | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **Licença** | **MIT** | MIT | GPL (pago para uso comercial) |
| **Autonomia doméstica (chinesa)** | ✅ | ❌ | ❌ |
| **Dependências de terceiros** | **Nenhuma** | Nenhuma | Nenhuma |
| **E/S assíncrona real** | ✅ | ✅ | ❌ |
| **Execução em lote com pipeline** | ✅ | ❌ | ❌ |
| **Lote com binding de array** | ✅ | ❌ | ❌ |
| **Lote com conjuntos de parâmetros por dicionário** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **Integração com o ORM XCode** | ✅ | ❌ | ❌ |
| **Frameworks alvo** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **Rastreamento OpenTelemetry** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ Detecção automática | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **Protocolo de compressão (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **Autenticação WebAuthn** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> Para a análise completa de recursos e arquitetura, consulte [Arquitetura](Doc/架构设计.md).

---

## Instalação

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## Início rápido

### String de conexão

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| Parâmetro | Aliases | Padrão | Descrição |
|------|------|--------|------|
| Server | DataSource, Data Source | - | Endereço do servidor |
| Port | - | 3306 | Número da porta |
| Database | - | - | Nome do banco de dados |
| UserID | Uid, User Id, User | - | Nome de usuário |
| Password | Pass, Pwd | - | Senha |
| ConnectionTimeout | Connection Timeout | 15 | Timeout de conexão (segundos) |
| CommandTimeout | Default Command Timeout | 30 | Timeout de comando (segundos) |
| SslMode | Ssl Mode | None | Modo SSL (None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | Chave global de prepare no servidor |
| Pipeline | Pipelining | false | Chave de execução com pipeline |

### Abrindo uma conexão

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### Consultando um conjunto de resultados

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

### Consultando um escalar

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### Executando DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## Consultas parametrizadas

Use a sintaxe `@nome-do-parametro` para vincular parâmetros; os valores são escapados automaticamente para evitar injeção:

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

**Tipos de parâmetro suportados**:

| Tipo .NET | Exemplo de literal SQL |
|-----------|---------------|
| `String` | `'hello'` (caracteres especiais escapados automaticamente) |
| `Int32` / `Int64` e outros tipos numéricos | `42` |
| `Boolean` | `1` ou `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | Dados de geometria em formato WKB |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | Convertido em número |
| `null` / `DBNull.Value` | `NULL` |

---

## Transações

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

Quatro níveis de isolamento são suportados: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead` (padrão) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

Se uma transação for descartada sem commit nem rollback, ela sofre rollback automaticamente.

---

## Procedures armazenadas

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

## Prepared statements

Use `Prepare()` para compilar o SQL no servidor; as execuções seguintes usam o protocolo binário:

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

Você também pode habilitá-lo globalmente pela string de conexão, para que toda consulta parametrizada use o protocolo binário automaticamente:

```
UseServerPrepare=true;
```

---

## Operações em lote

### Opção A: Conjuntos de parâmetros por dicionário

Executa o mesmo SQL com vários conjuntos de parâmetros vinculados; o prepare é feito internamente e de forma automática:

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

### Opção B: Binding de array (estilo Oracle)

Defina valores de parâmetros como arrays e informe a contagem de execução:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### Opção C: Execução com pipeline

Habilite `Pipeline=true` na string de conexão para enviar em lote e receber em lote com apenas uma ida e volta de rede:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

O pipeline funciona para INSERT / UPDATE / DELETE e pode ser combinado com conjuntos de parâmetros por dicionário ou binding de array. Quanto maior a latência de rede (por exemplo, entre data centers), maior o benefício.

### Opção D: VALUES multilinha

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### Opção E: API DbBatch (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## Suporte a EF Core

O pacote `NewLife.MySql.EntityFrameworkCore` fornece um provedor de banco de dados EF Core com suporte a .NET 6 / .NET 8 / .NET 10.

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

**Recursos suportados**:

| Recurso | Descrição |
|------|------|
| Mapeamento de tipos | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| Tradução de consultas | LINQ → sintaxe MySQL (backticks, LIMIT/OFFSET) |
| Tradução de métodos | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| Tradução de membros | DateTime.Year / Month / Day / Hour / Minute / Second |
| Suporte a migrações | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| Estratégia de geração de valores | Chaves primárias de identidade AUTO_INCREMENT |

---

## Conexões criptografadas SSL/TLS

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | Comportamento |
|---------|------|
| `None` / `Disabled` | Não usar SSL (padrão) |
| `Preferred` | Preferir criptografia, recorrendo a texto simples se não houver suporte |
| `Required` | Forçar criptografia |

---

## Consultas de informações de schema

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## Métodos assíncronos

Todas as operações principais têm versões assíncronas:

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

## Rastreamento (OpenTelemetry)

O driver tem instrumentação `ActivitySource` integrada (nome da fonte `NewLife.MySql`), emitindo quatro tipos de spans: `Open` (conexão), `Execute` (comando, cobrindo o ciclo de vida do reader), `Commit` / `Rollback` (transações). Os atributos seguem as convenções semânticas estáveis de banco de dados do OpenTelemetry (`db.system.name` / `db.query.text` / `server.address` / `error.type`, etc.). O SDK do OpenTelemetry é fornecido pelo consumidor; o driver não adiciona dependências:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **Escopo**: `net6.0` e superiores; em versões inferiores e sem assinantes é um no-op sem overhead
- **Aviso de privacidade**: `db.query.text` registra o texto do comando; prefira consultas parametrizadas para que literais não entrem na telemetria

---

## Pool de conexões

O pool de conexões é habilitado por padrão, sem necessidade de configuração adicional. Cada string de conexão única mapeia para o seu próprio pool independente.

- As conexões são obtidas do pool automaticamente ao abrir
- As conexões são devolvidas automaticamente ao fechar (a conexão TCP não é descartada)
- Conexões inválidas são removidas automaticamente
- Novas conexões são criadas sob demanda
- **Reconexão automática após queda de rede**: tenta novamente de forma automática ao detectar uma conexão perdida, melhorando a estabilidade em cenários de longa duração

### Parâmetros do pool de conexões

| Parâmetro | Padrão | Descrição |
|------|--------|------|
| MinPoolSize | 0 | Número mínimo de conexões |
| MaxPoolSize | 100 | Número máximo de conexões |
| ConnectionLifeTime | 0 | Tempo de vida da conexão (segundos); 0 significa que nunca expira |
| ConnectionIdleTime | 300 | Timeout de ociosidade (segundos); as conexões são recuperadas após esse período |
| ValidationInterval | 30 | Intervalo de validação de ociosidade (segundos) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## Integração com o ORM XCode

Registrado automaticamente como o driver MySQL do XCode por meio de `MySqlClientFactory`:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## Troca de banco de dados

### ChangeDatabase (recomendado)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ A troca no meio de uma transação não é suportada (a transação é perdida); crie uma conexão independente para cada banco de dados.

### SqlClient.SetDatabaseAsync (API de baixo nível)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

Não fecha a conexão; apenas troca o banco de dados atual no lado do servidor, adequado a cenários de consulta temporária.

---

## Suporte a OceanBase / TiDB

O driver detecta o tipo de banco de dados automaticamente a partir da string de versão no pacote de handshake; o método de conexão é exatamente o mesmo do MySQL:

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

SQL básico, transações, consultas parametrizadas e operações em lote são todos compatíveis.

---

## Frameworks alvo

| Framework | Descrição |
|------|------|
| `net45` / `net461` | .NET Framework, compatível com projetos legados |
| `netstandard2.0` / `netstandard2.1` | Compatibilidade multiplataforma, suporta Dispose assíncrono |
| `net6.0` | Suporta adicionalmente a API DbBatch |
| `net10.0` | .NET mais recente, desempenho ideal |

---

## Implantação NativeAOT

Com os frameworks alvo `net8.0+`, o driver passou por validação real de publicação NativeAOT (medições locais e CI em win-x64 e linux-x64; todos os 26 cenários de fumaça passaram com 0 avisos do ILC). Publicar binários nativos **não exige nenhum patch no lado do chamador**; o CI protege continuamente contra regressões em uma matriz de RIDs mainstream (win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, além de um teste de fumaça em emulação QEMU para LoongArch linux-la64 durante o período de observação, que não bloqueia):

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| Item | Conteúdo |
|------|------|
| Escopo | `net8.0+`; `net6.0` requer uma chave experimental, e frameworks anteriores não são suportados |
| Capacidades verificadas | Conexão/autenticação (incluindo os atributos de conexão do handshake), consultas síncronas e assíncronas, prepare no servidor, lote (dicionário/array/array de valores/pipeline), transações, procedures armazenadas, metadados de Schema, DbDataSource, DataAdapter, MySqlBulkCopy, protocolo de compressão, assinatura de binlog, rastreamento |
| Observações | A família Fill/FillSchema/Update do `MySqlDataAdapter` carrega anotações `RequiresUnreferencedCode`; aplicações com trimming declaram a responsabilidade de preservação no ponto de chamada, conforme a semântica do System.Data |
| Avaliação detalhada | [Avaliação de Compatibilidade AOT](Doc/AOT兼容性评估.md) |

---

## Observações

1. **Versão do MySQL**: 5.x e superiores suportadas, 8.0+ recomendada
2. **Codificação de caracteres**: UTF-8 por padrão
3. **ChangeDatabase**: implementado via Close/Reopen; a troca no meio de uma transação não é suportada, então use uma conexão independente
4. **Execução com pipeline**: apenas para operações DML em lote, não para consultas SELECT

---

## Documentação relacionada

| Documento | Descrição |
|------|------|
| [Requisitos](Doc/需求文档.md) | Visão, objetivos principais, direções funcionais |
| [Lista de recursos](Doc/功能清单.md) | Acompanhamento tridimensional do estado de implementação/testes/comentários |
| [Arquitetura](Doc/架构设计.md) | Visão geral da arquitetura, implementação do protocolo, escolhas tecnológicas, decisões de design, comparativo de drivers |
| [Relatório de Testes de Desempenho](Doc/性能测试报告.md) | Dados detalhados de benchmark das operações em lote |
| [Registro de alterações (中文)](Doc/ChangeLog.md) | Registro de versões em chinês |
| [Registro de alterações (EN)](Doc/ChangeLog.en.md) | Registro de versões em inglês |
| [README em chinês simplificado](README.zh-CN.md) | Documentação em chinês, mantida no mesmo lote desta versão em inglês |
| [Guia de migração: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Passos e observações para migrar do driver oficial da Oracle |
| [Guia de migração: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | Passos e observações para migrar do principal driver da comunidade |

## Mapa do ecossistema

NewLife é um ecossistema em camadas de projetos de código aberto com versionamento independente. O mapa abaixo é compartilhado literalmente por todos os repositórios NewLife; cada projeto aponta para o seu repositório e o seu pacote principal no NuGet.

| Projeto | O que faz | Downloads |
| --- | --- | --- |
| **Fundação** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | Logs, configuração, cache, rede, serialização, agendamento e rastreamento APM. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | Middleware de dados: tabelas únicas na escala de dezenas de bilhões de linhas, particionamento automático, separação de leitura/escrita, mais de 10 bancos de dados. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | Biblioteca de rede de alto desempenho: 1,77 M pps / 14,5 Gbps em uma única máquina (TCP 1KB, 2026-10), 4 M conexões TCP persistentes. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | Framework de comunicação por protocolo e RPC para aplicações cliente/servidor e dispositivos IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | Instala aplicações .NET como daemons do sistema operacional: serviços do Windows e systemd do Linux. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | Templates de projeto `dotnet new` para console, web, API, serviço, dados e IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **Dados e mensageria** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Cliente Redis com latência de microssegundos, milhões de operações por segundo e filas de mensagens integradas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Cliente RocketMQ totalmente gerenciado para Apache RocketMQ e Alibaba Cloud Message Queue. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | Cliente e servidor MQTT, incluindo conectividade com o Alibaba Cloud IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | Banco de dados híbrido em C#: embarcado ou em rede, relacional, séries temporais, fila de mensagens e KV em um só. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | Driver MySql e camada de apoio construídos sobre a stack de dados NewLife. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, IA e distribuído** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | Framework rápido para plataformas administrativas: permissões, SSO, servidor OAuth e scaffolding de CRUD. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | Plataforma distribuída de serviços: gestão de nós, APM, central de configuração, registro e central de distribuição. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | Plataforma distribuída de tarefas e agendamento de dados em tempo real, com particionamento, novas tentativas e preenchimento retroativo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | Cliente unificado para 46 provedores de LLM, além do ChatAI, uma aplicação web de chat pronta para uso. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **Industrial e IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | Biblioteca padrão de IoT que define contratos de protocolo e abstrações de dispositivos. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | Implementação de Modbus TCP/RTU/ASCII sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | Protocolo de PLC Siemens sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | Protocolo de PLC Omron sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | Protocolo de PLC Mitsubishi Melsec sobre NewLife.IoT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | Cliente OPC para acesso a dados industriais. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | Stack do protocolo BACnet para automação predial. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | Plataforma IoT de código aberto e leve, com gestão de gateways e dispositivos. | — |
| **Telemática veicular** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | Protocolo de posicionamento veicular JT/T 808: mais de 70 corpos de mensagem, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | Protocolo plataforma-a-plataforma JT/T 809 para autoridades de transporte. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | Vídeo embarcado JT/T 1078: análise RTP, remux FLV/fMP4 e transmissão ao vivo. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **Componentes de domínio** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Um único pacote para Excel, Word, PPT, PDF e mais de 15 formatos de documento. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | Áudio multiplataforma: codecs, E/S de dispositivos, DSP, contêineres e streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | Codecs de vídeo, formatos de contêiner, processamento de quadros e streaming. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | Interface unificada sobre Baidu, AMap, Tencent e Tianditu, com geocodificação e conversão de coordenadas. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | Geolocalização IPv4 com base de dados offline mapeada em memória e uma API online como alternativa. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **Ferramentas e produtos** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | Caixa de ferramentas do desenvolvedor: rede, porta serial, criptografia, expressões regulares, Modbus e MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | Sistema de gestão de código-fonte Git auto-hospedado. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | Instala automaticamente o runtime .NET correspondente quando iniciado a partir de um host mais antigo. | — |

> Os badges mostram o pacote principal de cada projeto; pacotes de temas, extensões e satélites estão listados nas suas páginas do NuGet. Os mais de 30 pacotes não listados aqui (FTP, LoRa, extensões do Office, drivers de protocolo e outros) estão disponíveis na [organização NewLifeX](https://github.com/NewLifeX) e em [newlifex.com](https://newlifex.com).

## Equipe

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

Fundada em 2002, a equipe NewLife dedica-se há mais de duas décadas ao ecossistema .NET e IoT, mantém cerca de 100 projetos de código aberto (34 deles centrais neste mapa) e entrega soluções integradas de software e hardware para os setores de energia, logística, saúde, controle industrial e internet. Seguimos o lema "no aprendizado não há antiguidade — o domínio conquista o respeito".

Os componentes de código aberto somam <!--nuget-total:start-->6.48M<!--nuget-total:end--> downloads no NuGet (ecossistema completo, em 2026-10).

Site: https://newlifex.com · Organização de código aberto: https://github.com/NewLifeX

<details>
<summary>Conta oficial do WeChat: 智能大石头 (escaneie para seguir)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
