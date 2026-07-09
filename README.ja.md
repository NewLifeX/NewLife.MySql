# NewLife.MySql — 純粋 C# 高パフォーマンス MySQL ドライバー

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)
[![Downloads](https://img.shields.io/nuget/dt/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)

> 🌐 [中文](Readme.MD) | [English](README.en.md) | **日本語** | [한국어](README.ko.md) | [Español](README.es.md) | [Português](README.pt.md)

**NewLife.MySql** は、NewLife チームが開発した純粋 C# 製 MySQL ADO.NET ドライバーです。TCP 上で MySQL ワイヤープロトコル（Protocol Version 10）を直接実装し、**サードパーティ依存ゼロ**、**完全 async/await 対応**、**MIT ライセンス**で商用利用も安心です。

独自の**パイプライン化バッチ実行**（Pipeline）により、競合ドライバーと比較してバッチ DML が **2～3 倍高速**です。

---

## 他ドライバーとの比較

| 機能 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|---------|:---:|:---:|:---:|
| ライセンス | **MIT** ✅ | MIT ✅ | GPLv2 ⚠️ |
| 依存関係 | **0** (NewLife.Core のみ) | 1 | 6 |
| 真の非同期 IO | ✅ | ✅ | ❌ |
| パイプライン化バッチ | ✅ **独自** | ❌ | ❌ |
| 配列バインドバッチ | ✅ | ❌ | ❌ |
| 辞書パラメータバッチ | ✅ | ❌ | ❌ |
| MySqlBulkCopy | ✅ | ✅ | ✅ |
| 圧縮 (zlib/zstd) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| WebAuthn 認証 | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ 自動検出 | ❌ | ❌ |
| 対象フレームワーク | net45 ~ net10 | net462+ | net462+ |

---

## パフォーマンス

> テスト環境: .NET 10 + MySQL 8.0.39 localhost (Windows)、TCP 127.0.0.1:3306

### バッチ DML: 10,000 行（ms、低いほど良い）

| 操作 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) | 高速化倍率 |
|------|------:|------:|------:|------:|
| INSERT | **899** | 1,927 | 1,906 | **2.1×** |
| UPDATE | **710** | 2,265 | 2,041 | **2.9×** |
| DELETE | **661** | 1,961 | 1,767 | **2.7×** |

### パイプライン化加速（1,000 行）

| シナリオ | 逐次実行 | Pipeline + Tx | 高速化倍率 |
|----------|----------:|-------------:|-------:|
| INSERT | 437ms | 54ms | **8.1×** |

---

## クイックスタート

### インストール

```shell
dotnet add package NewLife.MySql
```

### 接続

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();
```

### 基本 CRUD

```csharp
// クエリ
using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}, {reader.GetInt64(2)}");
}

// スカラー
var count = (Int64)cmd.ExecuteScalar("SELECT COUNT(*) FROM users");

// 非クエリ
var rows = cmd.ExecuteNonQuery("INSERT INTO users(name, age) VALUES('Tom', 25)");
```

### パラメータ化クエリ

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);
using var reader = cmd.ExecuteReader();
```

### トランザクション

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

### バッチ操作（パイプライン）

```csharp
var connStr = "Server=localhost;Database=mydb;...;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);  // 競合比 2~3× 高速！
```

### 非同期 API

```csharp
await conn.OpenAsync();
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## 接続文字列パラメータ

| パラメータ | 別名 | 既定値 | 説明 |
|-----------|---------|---------|-------------|
| Server | DataSource, Data Source | — | サーバーアドレス |
| Port | — | 3306 | ポート番号 |
| Database | — | — | データベース名 |
| UserID | Uid, User Id, User | — | ユーザー名 |
| Password | Pass, Pwd | — | パスワード |
| ConnectionTimeout | Connection Timeout | 15 | 接続タイムアウト（秒） |
| SslMode | Ssl Mode | None | None / Preferred / Required |
| UseServerPrepare | Use Server Prepare | false | サーバーサイド準備 |
| Pipeline | Pipelining | false | パイプライン化バッチ実行 |
| MinPoolSize | Min Pool Size | 0 | 最小プールサイズ |
| MaxPoolSize | Max Pool Size | 100 | 最大プールサイズ |

---

## フレームワーク互換性

| TFM | 状態 |
|-----|--------|
| `net45` | ✅ .NET 4.5 対応の唯一のモダン MySQL ドライバー |
| `net461` | ✅ |
| `netstandard2.0` | ✅ |
| `netstandard2.1` | ✅ |
| `net6.0` | ✅ + DbBatch API |
| `net8.0` | ✅ |
| `net10.0` | ✅ 最新 .NET |

---

## データベース互換性

| データベース | 検出 | 備考 |
|----------|:---------:|-------|
| MySQL 5.x ~ 9.0+ | 自動 | 3 種類の認証方式に対応 |
| OceanBase | ハンドシェイクから自動検出 | 完全 CRUD + トランザクション互換 |
| TiDB | ハンドシェイクから自動検出 | 完全 CRUD + トランザクション互換 |
| Aurora / CloudSQL | ハンドシェイクから自動検出 | AWS Aurora / GCP Cloud SQL 互換 |
| MariaDB | 基本 | `ed25519` 認証非対応 |

---

## ライセンス

MIT License — 個人・商用利用ともに無料。

Copyright © 2002-2026 [NewLife](https://newlifex.com)
