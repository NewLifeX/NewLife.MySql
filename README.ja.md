# NewLife.MySql — 純粋 C# 製ハイパフォーマンス MySQL ドライバー

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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql** は NewLife チームによる純粋 C# 製の MySQL クライアントドライバーです。標準の ADO.NET インターフェースを基盤に、MySQL プロトコル層（Protocol Version 10）を TCP 上で直接実装しています。依存は `NewLife.Core` のみで、**サードパーティ依存ゼロ**、**全経路で真の非同期**、**MIT ライセンス**による商用利用も安心です。

独自の**パイプライン化バッチ実行**（Pipeline）は、数万行規模のバッチ書き込みにおいて**2×〜3×** の高速化（逐行実行との比較）を実現し、大規模データのバッチ処理や信創（中国国産化）代替の場面に最適です。

> ℹ️ 本日本語版は [English README](README.md) と同一バッチで保守しています。

---

## 設計目標

| 目標 | 説明 |
|------|------|
| **軽量かつ最小限** | ADO.NET 標準の CRUD、トランザクション、パラメーター化クエリ、ストアドプロシージャ、バッチ操作のみを提供 |
| **ハイパフォーマンス** | 追加メモリ割り当てゼロ（`ArrayPool` / `OwnerPacket`）、真の非同期 I/O、パイプライン化バッチ実行 |
| **完全な国産スタック** | コアコードは完全に自主管理で GPL ライセンスリスクがなく、信創環境に適合 |
| **大規模データに適合** | 辞書パラメーターセット / 配列バインド / パイプライン化 / 複数行 VALUES / DbBatch により、数万行から数百万行までをカバー |
| **広範な互換性** | `net45` 〜 `net10` の全バージョン、MySQL 5.x 〜 9.0+、OceanBase / TiDB / Doris / Aurora / CloudSQL と互換、`net8.0+` で NativeAOT をサポート |

## コア機能

| 機能 | 説明 |
|------|------|
| 接続管理 | MySQL 5.x / 8.0 / 9.0+、`mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` の複数認証方式 |
| Unix Socket | Unix Domain Socket によるローカル接続をサポートし、TCP のオーバーヘッドはゼロ |
| 圧縮プロトコル | zlib 圧縮転送（`UseCompression`）をサポートし、ネットワーク帯域の消費を削減。zstd はサードパーティ依存ゼロの制約により見送り |
| クエリ実行 | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`、完全な ADO.NET インターフェース |
| パラメーター化クエリ | クライアント側でのパラメーター置換、`@パラメーター名` 構文、自動エスケープによるインジェクション防止 |
| プリペアドステートメント | `Prepare()` は `COM_STMT_PREPARE` バイナリプロトコルを使用し、全体設定の `UseServerPrepare` スイッチも選択可能 |
| バッチ操作 | `ExecuteBatch` / `ExecuteArrayBatch` / パイプライン化 / 複数行 VALUES / DbBatch / `MySqlBulkCopy` |
| パイプライン実行 | `Pipeline=true` でまとめて送信し、まとめて受信。ネットワーク往復は一度だけ。数千行で 35×、数万行で約 100× の高速化 |
| トランザクション | `BeginTransaction` / `Commit` / `Rollback`、4 種類の分離レベル、自動ロールバック |
| ストアドプロシージャ | `CommandType.StoredProcedure`、IN/OUT パラメーターをサポート |
| SSL/TLS | `SslMode=Preferred/Required`、TLS 1.2/1.3 をサポート |
| 接続プール | 既定で有効、接続文字列ごとにプールを分割、有効期間とアイドル検証を設定可能、ヘルスチェックと回収を自動化 |
| 非同期メソッド | 全経路 `async/await`：`OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` など |
| スキーマクエリ | Tables / Columns / Indexes / Databases などのメタデータに加え、インデックス/外部キーの拡張クエリ |
| DbDataSource | .NET 7+ 形式の `MySqlDataSource`、依存性注入をサポート |
| トレーシング | 組み込みの `ActivitySource` 計装（ソース名 `NewLife.MySql`）。Open / Execute / Commit / Rollback のスパンを出力し、OpenTelemetry の安定版セマンティック規約に準拠。購読者がいない場合のオーバーヘッドはゼロ |
| DbBatch | .NET 6+ 標準の ADO.NET バッチ API |
| 空間データ | MySQL Geometry 型、`MySqlGeometry` による WKB ラッパー |
| EF Core | 別パッケージ `NewLife.MySql.EntityFrameworkCore` を通じてサポート |
| XCode 連携 | `MySqlClientFactory` により自動登録され、XCode ORM とシームレスに統合 |

---

## ハイライト

### 🚀 パイプライン化バッチ実行（Pipeline）

`COM_STMT_PREPARE + COM_STMT_EXECUTE` バイナリプロトコルを基盤とする独自のパイプラインモード：

- **Phase 1**：すべての EXECUTE パケットをまとめて構築・送信し、最後のパケットのみ Flush する
- **Phase 2**：すべての OK/Error 応答を順番にまとめて読み取り、影響行数を累積する
- TCP スタックが小さなパケットを結合する（Nagle アルゴリズム）ため、ネットワーク往復は一度だけになる

**実測データ**（.NET 10 + MySQL 8.0.26 localhost、パイプライン化＋トランザクション vs 逐行ベースライン）：

| シナリオ | 逐行ベースライン | Pipeline(tx) | 高速化倍率 |
|------|------:|------:|------:|
| 100 行 INSERT | 244ms | 20ms | 12.2× |
| 1,000 行 INSERT | 2,473ms | 70ms | **35.3×** |
| 10,000 行 INSERT | ~70s¹ | ~0.7s¹ | ~98× |

> ¹ 10,000 行のトランザクションなし逐行の値は外挿値（2.4ms/行）。Pipeline の値はトランザクションなしの実測値。トランザクションありの場合、1 万行で約 70ms。

**3 ドライバーの性能データ**（10,000 行のバッチ操作）：

| 操作 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> ローカル LAN でも数千行で **35×** の高速化を達成しており、データセンターを跨ぐ場面ではさらに効果が大きくなります。詳細は[性能テストレポート](Doc/性能测试报告.md)を参照してください。

### 🔧 6 種類のバッチ操作オプション

| オプション | API | 用途 |
|------|-----|---------|
| 辞書パラメーターセット | `ExecuteBatch` | コレクションから供給される動的パラメーター |
| 値配列バインド | `ExecuteBatchValues` | 一括インポート（`MySqlBulkCopy` の内部処理）、辞書/パラメーターオブジェクトの割り当てゼロ |
| 配列バインド | `ExecuteArrayBatch` | 大容量 DML、Oracle スタイル |
| パイプライン化 | `Pipeline=true` | 数万/数十万行、データセンター間 |
| 複数行 VALUES | SQL の連結 | 純粋な INSERT バッチ |
| DbBatch (.NET 6+) | `CreateBatch` | 異なる SQL 文の混在バッチ |

### 🛡️ サードパーティ依存ゼロ

依存は `NewLife.Core`（同じく NewLife チーム製）のみで、GPL ライセンスリスクがなく、MIT ライセンスにより商用利用も安心です。信創環境や厳格なセキュリティ監査が求められる場面に適しています。

### ⚡ 徹底したパフォーマンス最適化

- `ArrayPool<T>` + `OwnerPacket` により追加割り当てゼロ
- `Pool.StringBuilder` / `Pool.MemoryStream` を全面的にプール化
- 全経路 `async/await`、`sync-over-async` のアンチパターンなし
- 必要最小限のフィールドのみを読む簡潔なプロトコル解析

### 🌐 広範な互換性

- **フレームワーク**：`net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**：`net8.0+` でトリミングと NativeAOT をサポート（`IsAotCompatible`、`AotSmoke` スモークテストと主要 RID マトリクスの CI による回帰保護あり。必須は win-x64 / linux-x64 / linux-arm64。LoongArch linux-la64 は QEMU エミュレーションでカバー（観察期間：実行されるが失敗してもブロックしない））。詳細は [AOT 互換性評価](Doc/AOT兼容性评估.md) を参照
- **MySQL**：5.5 〜 9.0+、`mysql_native_password` + `caching_sha2_password`
- **互換データベース**：OceanBase、TiDB、Doris（自動検出）、Aurora / CloudSQL（クラウドデータベース）、MariaDB（基本機能）
- **条件付きコンパイル**：フレームワークごとに最適な実装を自動選択

---

## 機能比較

| 機能 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **ライセンス** | **MIT** | MIT | GPL（商用利用は有償） |
| **国産自主性** | ✅ | ❌ | ❌ |
| **サードパーティ依存** | **なし** | なし | なし |
| **真の非同期 I/O** | ✅ | ✅ | ❌ |
| **パイプライン化バッチ実行** | ✅ | ❌ | ❌ |
| **配列バインドバッチ** | ✅ | ❌ | ❌ |
| **辞書パラメーターセットバッチ** | ✅ | ❌ | ❌ |
| **DbBatch (.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode ORM 連携** | ✅ | ❌ | ❌ |
| **対象フレームワーク** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT (net8.0+)** | ✅ | ✅ | ❌ |
| **OpenTelemetry トレーシング** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ 自動検出 | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **圧縮プロトコル (zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn 認証** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> 完全な機能とアーキテクチャの分析は[アーキテクチャ](Doc/架构设计.md)を参照してください。

---

## インストール

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## クイックスタート

### 接続文字列

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| パラメーター | 別名 | 既定値 | 説明 |
|------|------|--------|------|
| Server | DataSource, Data Source | - | サーバーアドレス |
| Port | - | 3306 | ポート番号 |
| Database | - | - | データベース名 |
| UserID | Uid, User Id, User | - | ユーザー名 |
| Password | Pass, Pwd | - | パスワード |
| ConnectionTimeout | Connection Timeout | 15 | 接続タイムアウト（秒） |
| CommandTimeout | Default Command Timeout | 30 | コマンドタイムアウト（秒） |
| SslMode | Ssl Mode | None | SSL モード（None/Preferred/Required） |
| UseServerPrepare | Use Server Prepare | false | サーバー側プリペアの全体スイッチ |
| Pipeline | Pipelining | false | パイプライン実行スイッチ |

### 接続のオープン

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### 結果セットのクエリ

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

### スカラーのクエリ

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### DML の実行

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## パラメーター化クエリ

`@パラメーター名` 構文でパラメーターをバインドします。値は自動的にエスケープされ、インジェクションを防ぎます：

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

**サポートされるパラメーター型**：

| .NET 型 | SQL リテラルの例 |
|-----------|---------------|
| `String` | `'hello'`（特殊文字は自動エスケープ） |
| `Int32` / `Int64` などの数値型 | `42` |
| `Boolean` | `1` または `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | WKB 形式のジオメトリデータ |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | 数値に変換 |
| `null` / `DBNull.Value` | `NULL` |

---

## トランザクション

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

4 種類の分離レベルをサポートします：`ReadUncommitted` / `ReadCommitted` / `RepeatableRead`（既定）/ `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

トランザクションがコミットもロールバックもされずに破棄された場合、自動的にロールバックされます。

---

## ストアドプロシージャ

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

## プリペアドステートメント

`Prepare()` を使うと SQL がサーバー側でコンパイルされ、以降の実行はバイナリプロトコルが使われます：

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

接続文字列で全体設定を有効にすると、すべてのパラメーター化クエリが自動的にバイナリプロトコルを使うようになります：

```
UseServerPrepare=true;
```

---

## バッチ操作

### オプション A：辞書パラメーターセット

同じ SQL を複数セットのパラメーターで実行します。内部で自動的にプリペアされます：

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

### オプション B：配列バインド（Oracle スタイル）

パラメーターの値を配列にし、実行回数を指定します：

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### オプション C：パイプライン実行

接続文字列で `Pipeline=true` を有効にすると、まとめて送信し、まとめて受信するため、ネットワーク往復は一度だけになります：

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

パイプライン化は INSERT / UPDATE / DELETE で利用でき、辞書パラメーターセットや配列バインドと組み合わせることもできます。ネットワーク遅延が大きいほど（たとえばデータセンターを跨ぐ場合）、効果は大きくなります。

### オプション D：複数行 VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### オプション E：DbBatch API (.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core サポート

`NewLife.MySql.EntityFrameworkCore` パッケージは、.NET 6 / .NET 8 / .NET 10 をサポートする EF Core データベースプロバイダーを提供します。

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

**サポートされる機能**：

| 機能 | 説明 |
|------|------|
| 型マッピング | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| クエリ変換 | LINQ → MySQL 構文（バッククォート、LIMIT/OFFSET） |
| メソッド変換 | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| メンバー変換 | DateTime.Year / Month / Day / Hour / Minute / Second |
| マイグレーション対応 | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| 値生成戦略 | AUTO_INCREMENT の ID 主キー |

---

## SSL/TLS 暗号化接続

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | 動作 |
|---------|------|
| `None` / `Disabled` | SSL を使用しない（既定） |
| `Preferred` | 暗号化を優先し、未対応なら平文にフォールバック |
| `Required` | 暗号化を強制 |

---

## スキーマ情報のクエリ

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## 非同期メソッド

すべての主要操作に非同期版があります：

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

## トレーシング（OpenTelemetry）

ドライバーには `ActivitySource` の計装が組み込まれており（ソース名 `NewLife.MySql`）、4 種類のスパンを出力します：`Open`（接続）、`Execute`（コマンド、リーダーの生存期間をカバー）、`Commit` / `Rollback`（トランザクション）。属性は OpenTelemetry の安定版データベースセマンティック規約（`db.system.name` / `db.query.text` / `server.address` / `error.type` など）に従います。OpenTelemetry SDK は利用側が用意するもので、ドライバーは依存を追加しません：

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **適用範囲**：`net6.0` 以降。それ以前のバージョンや購読者がいない場合はオーバーヘッドゼロのノーオペレーション
- **プライバシーに関する注意**：`db.query.text` はコマンドテキストを記録します。リテラルがテレメトリに入らないよう、パラメーター化クエリの使用を推奨します

---

## 接続プール

接続プールは既定で有効で、追加の設定は不要です。一意な接続文字列ごとに独立したプールが割り当てられます。

- オープン時に自動的にプールから接続を取得
- クローズ時に自動的に返却（TCP 接続は切断されない）
- 無効な接続は自動的に除去
- 必要に応じて新しい接続を作成
- **ネットワーク切断後の自動再接続**：切断された接続を検出すると自動的に再試行し、長時間稼働するシナリオでの安定性を向上

### 接続プールのパラメーター

| パラメーター | 既定値 | 説明 |
|------|--------|------|
| MinPoolSize | 0 | 最小接続数 |
| MaxPoolSize | 100 | 最大接続数 |
| ConnectionLifeTime | 0 | 接続の有効期間（秒）。0 は無期限 |
| ConnectionIdleTime | 300 | アイドルタイムアウト（秒）。経過した接続は回収される |
| ValidationInterval | 30 | アイドル検証の間隔（秒） |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode ORM 連携

`MySqlClientFactory` により、XCode の MySQL ドライバーとして自動登録されます：

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## データベースの切り替え

### ChangeDatabase（推奨）

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ トランザクションの途中での切り替えはサポートされません（トランザクションは失われます）。データベースごとに独立した接続を作成してください。

### SqlClient.SetDatabaseAsync（低レベル API）

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

接続はクローズせず、サーバー側の現在のデータベースのみを切り替えます。一時的なクエリのシナリオに適しています。

---

## OceanBase / TiDB サポート

ドライバーはハンドシェイクパケットのバージョン文字列からデータベース種別を自動検出します。接続方法は MySQL とまったく同じです：

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

基本的な SQL、トランザクション、パラメーター化クエリ、バッチ操作はいずれも互換です。

---

## 対象フレームワーク

| フレームワーク | 説明 |
|------|------|
| `net45` / `net461` | .NET Framework、レガシープロジェクトと互換 |
| `netstandard2.0` / `netstandard2.1` | クロスプラットフォーム互換、非同期 Dispose をサポート |
| `net6.0` | DbBatch API もサポート |
| `net10.0` | 最新の .NET、最適なパフォーマンス |

---

## NativeAOT デプロイ

`net8.0+` の対象フレームワークでは、ドライバーは実際の NativeAOT 公開検証を通過しています（ローカル実測および win-x64 / linux-x64 の CI。26 のスモークシナリオすべてが ILC 警告 0 で成功）。ネイティブバイナリの公開に**呼び出し側のパッチは不要**です。CI は主要 RID マトリクス（win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl、および観察期間中の LoongArch linux-la64 向け QEMU エミュレーションスモークテスト。後者はブロックしない）で回帰を継続的に防いでいます：

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| 項目 | 内容 |
|------|------|
| 適用範囲 | `net8.0+`。`net6.0` は実験的スイッチが必要で、それ以前のフレームワークは非対応 |
| 検証済み機能 | 接続/認証（ハンドシェイクの接続属性を含む）、同期・非同期クエリ、サーバー側プリペア、バッチ（辞書/配列/値配列/パイプライン）、トランザクション、ストアドプロシージャ、Schema メタデータ、DbDataSource、DataAdapter、MySqlBulkCopy、圧縮プロトコル、binlog 購読、トレーシング |
| 注意事項 | `MySqlDataAdapter` の Fill/FillSchema/Update 系には `RequiresUnreferencedCode` 注釈が付与されます。トリミングされたアプリケーションは System.Data のセマンティクスに従い、呼び出し側で保持の責任を宣言してください |
| 詳細評価 | [AOT 互換性評価](Doc/AOT兼容性评估.md) |

---

## 注意事項

1. **MySQL バージョン**：5.x 以降をサポート、8.0+ を推奨
2. **文字エンコーディング**：既定は UTF-8
3. **ChangeDatabase**：Close/Reopen で実装。トランザクションの途中での切り替えはサポートされないため、独立した接続を使用してください
4. **パイプライン実行**：バッチ DML 操作のみ対象で、SELECT クエリには使用できません

---

## 関連ドキュメント

| ドキュメント | 説明 |
|------|------|
| [要件定義](Doc/需求文档.md) | ビジョン、コア目標、機能の方向性 |
| [機能一覧](Doc/功能清单.md) | 実装/テスト/コメントの 3 次元ステータス追跡 |
| [アーキテクチャ](Doc/架构设计.md) | アーキテクチャ概要、プロトコル実装、技術選定、設計判断、ドライバー比較 |
| [性能テストレポート](Doc/性能测试报告.md) | バッチ操作の詳細なベンチマークデータ |
| [変更履歴（中文）](Doc/ChangeLog.md) | 中国語のリリース変更履歴 |
| [変更履歴（EN）](Doc/ChangeLog.en.md) | 英語のリリース変更履歴 |
| [简体中文 README](README.zh-CN.md) | 中国語ドキュメント。本英語版と同一バッチで保守 |
| [移行ガイド：MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Oracle 公式ドライバーからの移行手順と注意点 |
| [移行ガイド：MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | 主流コミュニティドライバーからの移行手順と注意点 |

## エコシステムマップ

NewLife は、それぞれ独立してバージョン管理されるオープンソースプロジェクトによる階層型エコシステムです。以下のマップはすべての NewLife リポジトリで同一の内容を掲載しています。各プロジェクトはそのリポジトリと主要な NuGet パッケージにリンクしています。

| プロジェクト | 役割 | ダウンロード数 |
| --- | --- | --- |
| **基盤** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | ログ、設定、キャッシュ、ネットワーク、シリアル化、スケジューリング、APM トレーシング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | データミドルウェア：数百億行規模の単一テーブル、自動シャーディング、読み書き分離、10 以上のデータベース。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | ハイパフォーマンスネットワークライブラリ：単一マシンで 1.77M pps / 14.5 Gbps（TCP 1KB、2026-10）、400 万の長時間 TCP 接続。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | クライアント/サーバーアプリケーションと IoT デバイス向けのプロトコル通信および RPC フレームワーク。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | .NET アプリケーションを OS のデーモン（Windows サービス、Linux systemd）としてインストール。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | コンソール、Web、API、サービス、データ、IoT プロジェクト向けの `dotnet new` プロジェクトテンプレート。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **データとメッセージング** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | マイクロ秒のレイテンシ、毎秒数百万オペレーション、組み込みメッセージキューを備えた Redis クライアント。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Apache RocketMQ と Alibaba Cloud Message Queue 向けの純マネージド RocketMQ クライアント。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT クライアントとサーバー。Alibaba Cloud IoT への接続を含む。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 製のハイブリッドデータベース：組み込みまたはネットワーク、リレーショナル、時系列、メッセージキュー、KV を 1 つに。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | NewLife データスタック上に構築された MySql ドライバーとヘルパー層。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web、AI、分散システム** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 迅速な管理プラットフォームフレームワーク：権限、SSO、OAuth サーバー、CRUD スキャフォールディング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 分散サービスプラットフォーム：ノード管理、APM、設定センター、レジストリ、リリースセンター。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | シャーディング、リトライ、バックフィルを備えた分散タスクおよびリアルタイムデータスケジューリングプラットフォーム。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 46 の LLM プロバイダー向け統合クライアントと、すぐに使える Web チャットアプリケーション ChatAI。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **産業と IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | プロトコル契約とデバイス抽象化を定義する IoT 標準ライブラリ。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | NewLife.IoT 上の Modbus TCP/RTU/ASCII 実装。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | NewLife.IoT 上の Siemens PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | NewLife.IoT 上の Omron PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | NewLife.IoT 上の三菱 Melsec PLC プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 産業データアクセス向けの OPC クライアント。 | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet ビルオートメーションプロトコルスタック。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | ゲートウェイとデバイス管理を備えた軽量オープンソース IoT プラットフォーム。 | — |
| **車載テレマティクス** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 車両位置プロトコル：70 以上のメッセージボディ、2011/2013/2019。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | 交通管理部門向けの JT/T 809 プラットフォーム間プロトコル。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 車載ビデオ：RTP 解析、FLV/fMP4 リマックス、ライブストリーミング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **ドメインコンポーネント** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Excel、Word、PPT、PDF など 15 以上のドキュメント形式を 1 つのパッケージで。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | クロスプラットフォーム音声：コーデック、デバイス I/O、DSP、コンテナ、ストリーミング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | ビデオコーデック、コンテナ形式、フレーム処理、ストリーミング。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 百度地図、高徳地図、騰訊地図、天地図を統合したインターフェース。ジオコーディングと座標変換を提供。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | オフラインのメモリマップドデータベースとオンライン API フォールバックを備えた IPv4 位置情報。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **ツールと製品** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 開発者向けツールボックス：ネットワーク、シリアルポート、暗号、正規表現、Modbus、MQTT。 | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | セルフホスト型の Git ソースコード管理システム。 | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 古いホストから起動された際に、対応する .NET ランタイムを自動的にインストール。 | — |

> バッジは各プロジェクトの主要パッケージを示しています。テーマ、拡張、サテライトパッケージは各 NuGet ページに掲載されています。ここに記載のない 30 以上のパッケージ（FTP、LoRa、Office 拡張、プロトコルドライバーなど）は [NewLifeX オーガナイゼーション](https://github.com/NewLifeX) および [newlifex.com](https://newlifex.com) で入手できます。

## チーム

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife チームは 2002 年に設立され、20 年以上にわたり .NET と IoT のエコシステムに取り組み、約 100 のオープンソースプロジェクト（うち 34 がこのマップの中核）を維持し、電力、物流、医療、産業制御、インターネットの各業界向けにソフトウェアとハードウェアを統合したソリューションを提供しています。私たちは「学問に先輩後輩はなく、達した者が尊ばれる」という言葉を信条としています。

オープンソースコンポーネントは NuGet から <!--nuget-total:start-->6.48M<!--nuget-total:end--> 回ダウンロードされています（エコシステム全体、2026-10 時点）。

ウェブサイト：https://newlifex.com · オープンソース組織：https://github.com/NewLifeX

<details>
<summary>WeChat 公式アカウント：智能大石头（スキャンしてフォロー）</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
