# NewLife.MySql — 純自主研發高效能 MySQL 驅動

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** 是新生命團隊出品的純自主研發 MySQL 用戶端驅動，以 ADO.NET 標準介面為基礎，直接透過 TCP 實作 MySQL 通訊協定層（Protocol Version 10）。僅相依於 `NewLife.Core`，**零第三方相依性**、**全鏈路真非同步**、**MIT 授權商業使用無虞**。

獨創的**管線化批次執行**（Pipeline）在萬級列的批次寫入場景可達 **2×~3×** 加速（相較逐列基準），是大資料批次操作、信創（資訊技術應用創新）自主可控替代的理想選擇。

> ℹ️ 本繁體中文版與 [English README](README.md) 同批維護。

---

## 設計目標

| 目標 | 說明 |
|------|------|
| **輕量極簡** | 只做 ADO.NET 標準的增刪改查、交易、參數化查詢、預存程序和批次操作 |
| **高效能** | 零額外記憶體配置（`ArrayPool` / `OwnerPacket`）、真非同步 IO、管線化批次執行 |
| **自主可控** | 核心程式碼完全自主可控，無 GPL 授權風險，適合信創環境 |
| **大資料友善** | 字典參數集 / 陣列繫結 / 管線化 / 多列 VALUES / DbBatch，涵蓋萬級到百萬級列 |
| **廣泛相容** | `net45` ~ `net10` 全版本，MySQL 5.x ~ 9.0+，相容於 OceanBase / TiDB / Doris / Aurora / CloudSQL，`net8.0+` 支援 NativeAOT |

## 核心功能

| 功能 | 說明 |
|------|------|
| 連線管理 | MySQL 5.x / 8.0 / 9.0+，`mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` 多重驗證 |
| Unix Socket | 支援 Unix Domain Socket 本機連線，零 TCP 額外負擔 |
| 壓縮通訊協定 | 支援 zlib 壓縮傳輸（`UseCompression`），降低網路頻寬消耗；zstd 因零第三方相依性約束暫緩支援 |
| 查詢執行 | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`，完整 ADO.NET 介面 |
| 參數化查詢 | 用戶端參數替換，`@參數名` 語法，自動逸出防注入 |
| 預備語句 | `Prepare()` 走 `COM_STMT_PREPARE` 二進位通訊協定，可選用 `UseServerPrepare` 全域開關 |
| 批次操作 | `ExecuteBatch` / `ExecuteArrayBatch` / 管線化 / 多列 VALUES / DbBatch / `MySqlBulkCopy` |
| 管線化執行 | `Pipeline=true` 批次送出後批次接收，網路延遲僅一次，千級列 35×、萬級列近百倍加速 |
| 交易支援 | `BeginTransaction` / `Commit` / `Rollback`，四種隔離等級，自動回退 |
| 預存程序 | `CommandType.StoredProcedure`，支援 IN/OUT 參數 |
| SSL/TLS | `SslMode=Preferred/Required`，支援 TLS 1.2/1.3 |
| 連線集區 | 預設開啟，依連線字串分池，可設定存活期與閒置驗證，自動健康檢查與回收 |
| 非同步方法 | 全鏈路 `async/await`，`OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` 等 |
| Schema 查詢 | Tables / Columns / Indexes / Databases 等中繼資料，擴充索引/外鍵查詢 |
| DbDataSource | .NET 7+ 風格 `MySqlDataSource`，支援相依性注入 |
| 鏈路追蹤 | 內建 `ActivitySource` 埋點（`AddSource("NewLife.MySql")`），輸出 Open / Execute / Commit / Rollback 跨度，遵循 OpenTelemetry 穩定語意慣例，無訂閱者時零額外負擔 |
| DbBatch | .NET 6+ ADO.NET 標準批次 API |
| 空間資料 | MySQL Geometry 型別，`MySqlGeometry` WKB 封裝 |
| EF Core | 透過 `NewLife.MySql.EntityFrameworkCore` 獨立套件支援 |
| XCode 整合 | 透過 `MySqlClientFactory` 自動註冊，無縫對接 XCode ORM |

---

## 亮點特性

### 🚀 管線化批次執行（Pipeline）

獨創的管線化模式，以 `COM_STMT_PREPARE + COM_STMT_EXECUTE` 二進位通訊協定為基礎：

- **Phase 1**：批次建構並送出所有 EXECUTE 封包，僅最後一個封包 Flush
- **Phase 2**：依序批次讀取所有 OK/Error 回應，累加影響列數
- TCP 通訊協定堆疊合併小封包（Nagle 演算法），網路來回僅一次

**實測資料**（.NET 10 + MySQL 8.0.26 本機，管線化+交易 vs 逐列基準）：

| 場景 | 逐列基準 | Pipeline(tx) | 加速比 |
|------|------:|------:|------:|
| 100 列 INSERT | 244ms | 20ms | 12.2× |
| 1,000 列 INSERT | 2,473ms | 70ms | **35.3×** |
| 10,000 列 INSERT | ~70s¹ | ~0.7s¹ | ~98× |

> ¹ 10,000 列逐列無交易為推估值（2.4ms/列），Pipeline 為無交易實測。加上交易後萬列約 70ms。

**三驅動效能資料**（10,000 列批次操作）：

| 操作 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> 本機內網千級列已達 **35×** 加速，跨機房場景效益更大。詳細資料參見 [效能測試報告](Doc/性能测试报告.md)。

### 🔧 六種批次操作方案

| 方案 | API | 適用場景 |
|------|-----|---------|
| 字典參數集 | `ExecuteBatch` | 動態參數，參數來自集合 |
| 值陣列繫結 | `ExecuteBatchValues` | 批次匯入（`MySqlBulkCopy` 內部），零字典/參數物件配置 |
| 陣列繫結 | `ExecuteArrayBatch` | 大批次 DML，Oracle 風格 |
| 管線化 | `Pipeline=true` | 萬級/十萬級列，跨機房 |
| 多列 VALUES | 拼接 SQL | 純 INSERT 批次 |
| DbBatch (.NET 6+) | `CreateBatch` | 不同 SQL 混合批次 |

### 🛡️ 零第三方相依性

僅相依於 `NewLife.Core`（同為 NewLife 團隊出品），無 GPL 授權風險，MIT 授權商業使用無虞，適合信創環境與安全稽核嚴格的場景。

### ⚡ 極致效能最佳化

- `ArrayPool<T>` + `OwnerPacket` 零額外配置
- `Pool.StringBuilder` / `Pool.MemoryStream` 全面集區化
- 全鏈路 `async/await`，無 `sync-over-async` 反模式
- 精簡通訊協定解析，只解析必要欄位

### 🌐 廣泛相容

- **框架**：`net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**：`net8.0+` 支援裁剪與 NativeAOT（`IsAotCompatible`，含 `AotSmoke` 煙霧測試與 CI 主流 RID 矩陣防回歸；必選 win-x64 / linux-x64 / linux-arm64；龍芯 linux-la64 以 QEMU 模擬覆蓋（觀察期：可執行、失敗不阻斷）），見 [AOT相容性評估](Doc/AOT兼容性评估.md)
- **MySQL**：5.5 ~ 9.0+，`mysql_native_password` + `caching_sha2_password`
- **相容資料庫**：OceanBase、TiDB、Doris（自動偵測）、Aurora / CloudSQL（雲端資料庫）、MariaDB（基礎）
- **條件式編譯**：針對不同框架自動選擇最佳實作

---

## 功能特性

| 功能 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **授權條款** | **MIT** | MIT | GPL（商業使用需付費） |
| **自主可控** | ✅ | ❌ | ❌ |
| **第三方相依性** | **無** | 無 | 無 |
| **真非同步 IO** | ✅ | ✅ | ❌ |
| **管線化批次執行** | ✅ | ❌ | ❌ |
| **陣列繫結批次** | ✅ | ❌ | ❌ |
| **字典參數集批次** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode ORM 整合** | ✅ | ❌ | ❌ |
| **多目標框架** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **OpenTelemetry 鏈路追蹤** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ 自動偵測 | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **壓縮通訊協定 (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn 驗證** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> 完整功能與架構分析參見 [架構設計](Doc/架构设计.md)。

---

## 安裝

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## 快速開始

### 連線字串

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| 參數 | 別名 | 預設值 | 說明 |
|------|------|--------|------|
| Server | DataSource, Data Source | - | 伺服器位址 |
| Port | - | 3306 | 連接埠號 |
| Database | - | - | 資料庫名稱 |
| UserID | Uid, User Id, User | - | 使用者名稱 |
| Password | Pass, Pwd | - | 密碼 |
| ConnectionTimeout | Connection Timeout | 15 | 連線逾時（秒） |
| CommandTimeout | Default Command Timeout | 30 | 命令逾時（秒） |
| SslMode | Ssl Mode | None | SSL 模式（None/Preferred/Required） |
| UseServerPrepare | Use Server Prepare | false | 全域伺服器端預備語句開關 |
| Pipeline | Pipelining | false | 管線化執行開關 |

### 開啟連線

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### 查詢結果集

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

### 查詢單一值

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### 執行 DML

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## 參數化查詢

使用 `@參數名` 語法繫結參數，自動逸出防注入：

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

**支援的參數型別**：

| .NET 型別 | SQL 常值範例 |
|-----------|---------------|
| `String` | `'hello'`（自動逸出特殊字元） |
| `Int32` / `Int64` 等數字型別 | `42` |
| `Boolean` | `1` 或 `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | WKB 格式幾何資料 |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | 轉為數字 |
| `null` / `DBNull.Value` | `NULL` |

---

## 交易

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

支援四種隔離等級：`ReadUncommitted` / `ReadCommitted` / `RepeatableRead`（預設）/ `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

交易 `Dispose` 時若未提交也未回退，會自動執行回退。

---

## 預存程序

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

## 預備語句

透過 `Prepare()` 在伺服器端預先編譯 SQL，後續執行走二進位通訊協定：

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

也可透過連線字串全域啟用，所有參數化查詢自動走二進位通訊協定：

```
UseServerPrepare=true;
```

---

## 批次操作

### 方案 A：字典參數集

同一 SQL 繫結多組參數執行，內部自動預先編譯：

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

### 方案 B：陣列繫結（Oracle 風格）

參數值設為陣列，指定執行次數：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### 方案 C：管線化執行

連線字串開啟 `Pipeline=true`，批次送出後批次接收，網路來回僅一次：

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

管線化對 INSERT / UPDATE / DELETE 均有效，搭配字典參數集或陣列繫結使用。網路延遲越高（例如跨機房），效益越大。

### 方案 D：多列 VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### 方案 E：DbBatch API（.NET 6+）

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core 支援

`NewLife.MySql.EntityFrameworkCore` 套件提供 EF Core 資料庫提供者，支援 .NET 6 / .NET 8 / .NET 10。

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

**已支援功能**：

| 功能 | 說明 |
|------|------|
| 型別對應 | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| 查詢轉譯 | LINQ → MySQL 語法（反引號、LIMIT/OFFSET） |
| 方法轉譯 | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| 成員轉譯 | DateTime.Year / Month / Day / Hour / Minute / Second |
| 移轉支援 | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| 值產生策略 | AUTO_INCREMENT 自動遞增主鍵 |

---

## SSL/TLS 加密連線

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | 行為 |
|---------|------|
| `None` / `Disabled` | 不使用 SSL（預設） |
| `Preferred` | 優先加密，不支援則走明文 |
| `Required` | 強制加密 |

---

## Schema 資訊查詢

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## 非同步方法

所有核心操作均支援非同步版本：

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

## 鏈路追蹤（OpenTelemetry）

驅動內建 `ActivitySource` 埋點（來源名稱 `NewLife.MySql`），輸出 `Open`（連線）、`Execute`（命令，涵蓋讀取器生命週期）、`Commit` / `Rollback`（交易）四類跨度，屬性遵循 OpenTelemetry 穩定資料庫語意慣例（`db.system.name` / `db.query.text` / `server.address` / `error.type` 等）。OpenTelemetry SDK 由使用方自備，驅動零新增相依性：

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **生效範圍**：`net6.0` 及以上；低版本與無訂閱者時為零額外負擔的空實作
- **隱私提示**：`db.query.text` 會記錄命令文字，建議使用參數化查詢，避免常值進入遙測

---

## 連線集區

連線集區預設啟用，無需額外設定。每個唯一連線字串對應獨立連線集區。

- 開啟連線時自動從集區取得
- 關閉連線時自動歸還（不中斷 TCP）
- 無效連線自動移除
- 新連線依需求建立
- **網路斷線自動重連**：偵測到連線中斷時自動重試，提升長連線場景穩定性

### 連線集區參數設定

| 參數 | 預設值 | 說明 |
|------|--------|------|
| MinPoolSize | 0 | 最小連線數 |
| MaxPoolSize | 100 | 最大連線數 |
| ConnectionLifeTime | 0 | 連線存活期（秒），0 表示永不過期 |
| ConnectionIdleTime | 300 | 閒置逾時（秒），逾時後回收 |
| ValidationInterval | 30 | 閒置驗證間隔（秒） |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode ORM 整合

透過 `MySqlClientFactory` 自動註冊為 XCode 的 MySQL 驅動：

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## 切換資料庫

### ChangeDatabase（建議）

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ 不支援在交易中途切換（交易會遺失），建議為每個資料庫建立獨立連線。

### SqlClient.SetDatabaseAsync（低階 API）

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

不關閉連線，僅切換伺服器端目前資料庫，適合臨時查詢場景。

---

## OceanBase / TiDB 支援

驅動依握手封包版本字串自動偵測資料庫類型，連線方式與 MySQL 完全相同：

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

基礎 SQL、交易、參數化查詢、批次操作均相容。

---

## 多目標框架

| 框架 | 說明 |
|------|------|
| `net45` / `net461` | .NET Framework，相容於舊專案 |
| `netstandard2.0` / `netstandard2.1` | 跨平台相容，支援非同步 Dispose |
| `net6.0` | 額外支援 DbBatch API |
| `net10.0` | 最新 .NET，最佳效能 |

---

## NativeAOT 部署

驅動在 `net8.0+` 目標框架下已通過 NativeAOT 實際發行驗證（win-x64 與 linux-x64 本機實測及 CI，26 個場景煙霧測試全數通過、ILC 0 警告），發行原生程式**無需任何呼叫端修補**；CI 依主流 RID 矩陣持續防回歸（win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl，以及龍芯 linux-la64 的 QEMU 模擬煙霧測試，觀察期非阻斷）：

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| 說明 | 內容 |
|------|------|
| 生效範圍 | `net8.0+`；`net6.0` 需實驗開關，更早框架不支援 |
| 已驗證能力 | 連線/驗證（含握手連線屬性）、同步與非同步查詢、伺服器端預備語句、批次（字典/陣列/值陣列/管線化）、交易、預存程序、Schema 中繼資料、DbDataSource、DataAdapter、MySqlBulkCopy、壓縮通訊協定、Binlog 訂閱、鏈路追蹤 |
| 注意事項 | `MySqlDataAdapter` 的 Fill/FillSchema/Update 系列帶有 `RequiresUnreferencedCode` 標註，裁剪應用依 System.Data 語意在呼叫端宣告保留責任 |
| 詳細評估 | [AOT 相容性評估](Doc/AOT兼容性评估.md) |

---

## 注意事項

1. **MySQL 版本**：支援 5.x 及以上，建議 8.0+
2. **字元編碼**：預設 UTF-8
3. **ChangeDatabase**：透過 Close/Reopen 實作，不支援交易中切換，建議使用獨立連線
4. **管線化執行**：僅適用於 DML 批次操作，不適用於 SELECT 查詢

---

## 相關文件

| 文件 | 說明 |
|------|------|
| [需求文件](Doc/需求文档.md) | 願景、核心目標、功能方向 |
| [功能清單](Doc/功能清单.md) | 功能實作/測試/註解三維狀態追蹤 |
| [架構設計](Doc/架构设计.md) | 架構概覽、通訊協定實作、技術選型、設計決策、驅動比較 |
| [效能測試報告](Doc/性能测试报告.md) | 批次操作基準效能測試詳細資料 |
| [更新記錄 (中文)](Doc/ChangeLog.md) | 中文版版本更新記錄 |
| [Changelog (EN)](Doc/ChangeLog.en.md) | English release changelog |
| [English README](README.md) | 含效能圖表的英文文件 |
| [移轉指南：MySql.Data → NL](Doc/迁移指南_MySqlData.md) | 從 Oracle 官方驅動移轉的步驟與注意事項 |
| [移轉指南：MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | 從社群主流驅動移轉的步驟與注意事項 |

## 生態版圖

NewLife 生態系按相依分層，各專案獨立版本、獨立發行。下面的版圖在所有 NewLife 儲存庫中逐字一致；每個專案連結到程式碼儲存庫與主 NuGet 套件。

| 專案 | 一句話定位 | 下載量 |
| --- | --- | --- |
| **基礎底座** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 日誌、設定、快取、網路、序列化、排程與 APM 追蹤。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 資料中介層：單表百億級、自動分表、讀寫分離，支援 10 多種資料庫。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 高效能網路庫：單機 177 萬 pps、14.5 Gbps（TCP 1KB，2026-10 實測），400 萬 Tcp 長連線。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 協定通訊與 RPC 框架，面向 CS 應用程式與物聯網裝置。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | 將 .NET 應用程式安裝為作業系統常駐服務：Windows 服務與 Linux systemd。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | `dotnet new` 專案範本：主控台、Web、API、服務、資料與物聯網。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **資料儲存與訊息** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis 用戶端：微秒級延遲、百萬級輸送量，內建訊息佇列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | 純受控 RocketMQ 用戶端，支援 Apache RocketMQ 與阿里雲訊息佇列。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 用戶端與服務端，含阿里雲物聯網接入。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 混合式資料庫：內嵌或網路雙模，關係型、時序、訊息佇列與 KV 於一體。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | 建構於 NewLife 資料堆疊之上的 MySql 驅動與輔助層。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web、AI 與分散式** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 快速後台開發框架：權限、SSO、OAuth 服務端與 CRUD 樣板。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 分散式服務平台：節點管理、APM、設定中心、註冊中心與發佈中心。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 分散式任務與即時資料排程平台，支援分片、重試與回溯補算。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 統一接入 46 家大模型服務商，附帶開箱即用的 ChatAI Web 對話應用。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **工業與物聯網** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | 物聯網標準庫，定義通訊協定契約與裝置抽象。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | 建構於 NewLife.IoT 之上的 Modbus TCP/RTU/ASCII 實作。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | 建構於 NewLife.IoT 之上的西門子 PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | 建構於 NewLife.IoT 之上的歐姆龍 PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | 建構於 NewLife.IoT 之上的三菱 Melsec PLC 協定。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 工業資料存取的 OPC 用戶端。 | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 樓宇自動化協定堆疊。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 開源輕量級物聯網平台，含閘道與裝置管理。 | — |
| **車聯網** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 車輛定位協定：70+ 訊息體，涵蓋 2011/2013/2019 三個版本。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | JT/T 809 部標平台互聯協定，面向運輸管理部門。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 車載視訊：RTP 解析、FLV/fMP4 轉封裝與直播推送。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **領域元件** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | 一個套件涵蓋 Excel、Word、PPT、PDF 等 15+ 文件格式。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 跨平台音訊：編解碼、裝置 IO、DSP、容器與串流處理。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 視訊編解碼、封裝格式、影格處理與串流。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 百度／高德／騰訊／天地圖統一介面，支援地理編碼與座標轉換。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 歸屬地：離線記憶體映射資料庫 + 線上 API 雙來源。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **工具與產品** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 開發者工具箱：網路、序列埠、加解密、正規表示式、Modbus 與 MQTT。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 自架 Git 原始碼管理系統。 | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 從較舊的宿主啟動時，自動安裝相符的 .NET 執行階段。 | — |

> 徽章取各專案主套件；主題套件、擴充套件與衛星套件見對應 NuGet 頁面。未在表中列出的 30 多個套件（FTP、LoRa、Office 擴充、各類協定驅動等）見 [NewLifeX 組織](https://github.com/NewLifeX) 與 [newlifex.com](https://newlifex.com)。

## 新生命開發團隊

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

新生命團隊成立於 2002 年，二十餘年專注 .NET 與物聯網技術生態，主導開源專案近百個（生態版圖收錄 34 個主幹專案），為電力、物流、醫療、工控、網際網路等行業提供軟硬體一體化解決方案；以「學無先後達者為師」為信條。

開源元件在 NuGet 累計下載 <!--nuget-total:start-->6.48M<!--nuget-total:end--> 次（全生態口徑，截至 2026-10）。

網站：https://newlifex.com · 開源組織：https://github.com/NewLifeX

<details>
<summary>微信公眾號：智能大石头（掃碼關注）</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
