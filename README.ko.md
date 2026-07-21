# NewLife.MySql — 순수 C# 고성능 MySQL 드라이버

[![NuGet](https://img.shields.io/nuget/v/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)
[![License](https://img.shields.io/github/license/NewLifeX/NewLife.MySql)](https://github.com/NewLifeX/NewLife.MySql/blob/master/LICENSE)
[![Downloads](https://img.shields.io/nuget/dt/NewLife.MySql.svg)](https://www.nuget.org/packages/NewLife.MySql)

> 🌐 [中文](Readme.MD) | [English](README.en.md) | [日本語](README.ja.md) | **한국어** | [Español](README.es.md) | [Português](README.pt.md)

**NewLife.MySql**은 NewLife 팀이 개발한 순수 C# MySQL ADO.NET 드라이버입니다. TCP를 통해 MySQL 와이어 프로토콜(Protocol Version 10)을 직접 구현했으며, **서드파티 의존성 제로**, **완전한 async/await 지원**, **MIT 라이선스**로 상업적 사용이 자유롭습니다.

독자적인 **파이프라인 배치 실행**(Pipeline)으로 행 단위 실행 대비 배치 DML이 **2~3배 빠릅니다**.

---

## 기능 비교

| 기능 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|---------|:---:|:---:|:---:|
| 라이선스 | **MIT** ✅ | MIT ✅ | GPLv2 ⚠️ |
| 의존성 | **0** (NewLife.Core만) | 1 | 6 |
| 진정한 비동기 IO | ✅ | ✅ | ❌ |
| 파이프라인 배치 | ✅ | ❌ | ❌ |
| 배열 바인드 배치 | ✅ | ❌ | ❌ |
| 사전 파라미터 배치 | ✅ | ❌ | ❌ |
| MySqlBulkCopy | ✅ | ✅ | ✅ |
| 압축 (zlib/zstd) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| WebAuthn 인증 | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ 자동 감지 | ❌ | ❌ |
| 대상 프레임워크 | net45 ~ net10 | net462+ | net462+ |

---

## 성능

> 테스트 환경: .NET 10 + MySQL 8.0.39 localhost (Windows), TCP 127.0.0.1:3306

### 배치 DML: 10,000행 (ms, 낮을수록 좋음)

| 작업 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | **899** | 1,927 | 1,906 |
| UPDATE | **710** | 2,265 | 2,041 |
| DELETE | **661** | 1,961 | 1,767 |

### 파이프라인 가속 (1,000행)

| 시나리오 | 행 단위 | Pipeline + Tx | 가속비 |
|----------|----------:|-------------:|-------:|
| INSERT | 437ms | 54ms | **8.1×** |

---

## 빠른 시작

### 설치

```shell
dotnet add package NewLife.MySql
```

### 연결

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;Password=pass;");
conn.Open();
```

### 기본 CRUD

```csharp
// 쿼리
using var cmd = new MySqlCommand(conn, "SELECT id, name, age FROM users WHERE age > 18");
using var reader = cmd.ExecuteReader();
while (reader.Read())
{
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}, {reader.GetInt64(2)}");
}

// 스칼라
var count = (Int64)cmd.ExecuteScalar("SELECT COUNT(*) FROM users");

// 비쿼리
var rows = cmd.ExecuteNonQuery("INSERT INTO users(name, age) VALUES('Tom', 25)");
```

### 파라미터화 쿼리

```csharp
using var cmd = new MySqlCommand(conn, "SELECT * FROM users WHERE name = @name AND age > @age");
cmd.Parameters.AddWithValue("name", "Tom");
cmd.Parameters.AddWithValue("age", 18);
using var reader = cmd.ExecuteReader();
```

### 트랜잭션

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

### 배치 작업 (파이프라인)

```csharp
var connStr = "Server=localhost;Database=mydb;...;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);  // 경쟁사 대비 2~3× 빠름!
```

### 비동기 API

```csharp
await conn.OpenAsync();
using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

---

## 연결 문자열 파라미터

| 파라미터 | 별칭 | 기본값 | 설명 |
|-----------|---------|---------|-------------|
| Server | DataSource, Data Source | — | 서버 주소 |
| Port | — | 3306 | 포트 번호 |
| Database | — | — | 데이터베이스 이름 |
| UserID | Uid, User Id, User | — | 사용자 이름 |
| Password | Pass, Pwd | — | 비밀번호 |
| ConnectionTimeout | Connection Timeout | 15 | 연결 타임아웃(초) |
| SslMode | Ssl Mode | None | None / Preferred / Required |
| UseServerPrepare | Use Server Prepare | false | 서버 측 준비 |
| Pipeline | Pipelining | false | 파이프라인 배치 실행 |
| MinPoolSize | Min Pool Size | 0 | 최소 풀 크기 |
| MaxPoolSize | Max Pool Size | 100 | 최대 풀 크기 |

---

## 프레임워크 호환성

| TFM | 상태 |
|-----|--------|
| `net45` | ✅ .NET 4.5를 지원하는 유일한 최신 MySQL 드라이버 |
| `net461` | ✅ |
| `netstandard2.0` | ✅ |
| `netstandard2.1` | ✅ |
| `net6.0` | ✅ + DbBatch API |
| `net8.0` | ✅ |
| `net10.0` | ✅ 최신 .NET |

---

## 데이터베이스 호환성

| 데이터베이스 | 감지 | 비고 |
|----------|:---------:|-------|
| MySQL 5.x ~ 9.0+ | 자동 | 3가지 인증 방식 지원 |
| OceanBase | 핸드셰이크에서 자동 감지 | 완전 CRUD + 트랜잭션 호환 |
| TiDB | 핸드셰이크에서 자동 감지 | 완전 CRUD + 트랜잭션 호환 |
| Aurora / CloudSQL | 핸드셰이크에서 자동 감지 | AWS Aurora / GCP Cloud SQL 호환 |
| MariaDB | 기본 | `ed25519` 인증 미지원 |

---

## 라이선스

MIT License — 개인 및 상업적 사용 무료.

Copyright © 2002-2026 [NewLife](https://newlifex.com)
