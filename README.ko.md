# NewLife.MySql — 순수 C# 고성능 MySQL 드라이버

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
🌐 [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [한국어](README.ko.md) | [العربية](README.ar.md)
<!-- lang:end -->

**NewLife.MySql**은 NewLife 팀이 만든 순수 C# MySQL 클라이언트 드라이버입니다. 표준 ADO.NET 인터페이스를 기반으로 MySQL 프로토콜 계층(Protocol Version 10)을 TCP 위에 직접 구현했습니다. 의존성은 `NewLife.Core` 하나뿐이며, **서드파티 의존성 제로**, **전 구간 진정한 비동기**, **MIT 라이선스**로 상업적 사용도 부담이 없습니다.

독자적인 **파이프라인 배치 실행**(Pipeline)은 수만 행 규모의 배치 쓰기에서 행 단위 기준 대비 **2~3배** 빠른 속도를 제공하여, 빅데이터 배치 작업과 信创(중국 자체 기술) 대체 시나리오에 이상적인 선택입니다.

---

## 설계 목표

| 목표 | 설명 |
|------|------|
| **가볍고 최소한의 기능** | 표준 ADO.NET의 CRUD, 트랜잭션, 매개변수화 쿼리, 저장 프로시저, 배치 작업만 제공 |
| **고성능** | 추가 메모리 할당 제로(`ArrayPool` / `OwnerPacket`), 진정한 비동기 I/O, 파이프라인 배치 실행 |
| **순수 국산(중국) 스택** | 핵심 코드를 완전히 자체 통제하여 GPL 라이선스 리스크가 없고 信创 환경에 적합 |
| **빅데이터 친화** | 딕셔너리 매개변수 집합 / 배열 바인딩 / 파이프라인 / 다중 행 VALUES / DbBatch로 수만 행부터 수백만 행까지 대응 |
| **폭넓은 호환성** | 모든 버전의 `net45` ~ `net10`, MySQL 5.x ~ 9.0+, OceanBase / TiDB / Doris / Aurora / CloudSQL 호환, `net8.0+`에서 NativeAOT 지원 |

## 핵심 기능

| 기능 | 설명 |
|------|------|
| 연결 관리 | MySQL 5.x / 8.0 / 9.0+, 다양한 인증 방식: `mysql_native_password` + `caching_sha2_password` + `authentication_webauthn` |
| Unix Socket | TCP 오버헤드 없이 Unix Domain Socket 로컬 연결 지원 |
| 압축 프로토콜 | 네트워크 대역폭 소모를 줄이는 zlib 압축 전송(`UseCompression`) 지원; zstd는 서드파티 의존성 제로 제약 때문에 보류 |
| 쿼리 실행 | `ExecuteReader` / `ExecuteScalar` / `ExecuteNonQuery`, 완전한 ADO.NET 인터페이스 |
| 매개변수화 쿼리 | `@parameter-name` 구문으로 클라이언트 측에서 매개변수를 치환하고 자동 이스케이프로 인젝션 방지 |
| 준비된 문 | `Prepare()`는 `COM_STMT_PREPARE` 바이너리 프로토콜을 사용하며, 전역 `UseServerPrepare` 스위치를 선택적으로 사용 |
| 배치 작업 | `ExecuteBatch` / `ExecuteArrayBatch` / 파이프라인 / 다중 행 VALUES / DbBatch / `MySqlBulkCopy` |
| 파이프라인 실행 | `Pipeline=true`는 배치로 보내고 배치로 받아 네트워크 왕복이 한 번뿐: 수천 행에서 35배, 수만 행에서 약 100배 |
| 트랜잭션 | `BeginTransaction` / `Commit` / `Rollback`, 네 가지 격리 수준, 자동 롤백 |
| 저장 프로시저 | `CommandType.StoredProcedure`, IN/OUT 매개변수 지원 |
| SSL/TLS | `SslMode=Preferred/Required`, TLS 1.2/1.3 지원 |
| 연결 풀 | 기본 활성화, 연결 문자열 단위로 풀링, 수명과 유휴 검증 설정 가능, 자동 상태 확인과 재활용 |
| 비동기 메서드 | 전 구간 `async/await`: `OpenAsync` / `ExecuteReaderAsync` / `ReadAsync` 등 |
| 스키마 조회 | Tables / Columns / Indexes / Databases 등의 메타데이터와 확장 인덱스/외래 키 조회 |
| DbDataSource | .NET 7+ 스타일 `MySqlDataSource`, 의존성 주입 지원 |
| 추적 | 내장 `ActivitySource` 계측(소스 이름 `NewLife.MySql`)으로 Open / Execute / Commit / Rollback 스팬을 생성하며, OpenTelemetry 안정 시맨틱 규약을 따르고 구독자가 없을 때는 오버헤드가 없음 |
| DbBatch | .NET 6+ 표준 ADO.NET 배치 API |
| 공간 데이터 | MySQL Geometry 타입, `MySqlGeometry` WKB 래퍼 |
| EF Core | 별도의 `NewLife.MySql.EntityFrameworkCore` 패키지로 지원 |
| XCode 통합 | `MySqlClientFactory`를 통해 자동 등록되어 XCode ORM과 매끄럽게 연동 |

---

## 주요 특징

### 🚀 파이프라인 배치 실행(Pipeline)

`COM_STMT_PREPARE + COM_STMT_EXECUTE` 바이너리 프로토콜을 기반으로 한 독자적인 파이프라인 모드입니다:

- **1단계**: 모든 EXECUTE 패킷을 한 번에 만들어 보내고 마지막 패킷에서만 플러시
- **2단계**: OK/Error 응답을 순서대로 한 번에 읽어 영향받은 행 수를 누적
- TCP 스택이 작은 패킷을 병합(Nagle 알고리즘)하므로 네트워크 왕복은 한 번뿐

**실측 데이터**(.NET 10 + MySQL 8.0.26 localhost, 파이프라인 + 트랜잭션 대 행 단위 기준):

| 시나리오 | 행 단위 기준 | Pipeline(tx) | 배속 |
|------|------:|------:|------:|
| 100행 INSERT | 244ms | 20ms | 12.2배 |
| 1,000행 INSERT | 2,473ms | 70ms | **35.3배** |
| 10,000행 INSERT | 약 70초¹ | 약 0.7초¹ | 약 98배 |

> ¹ 트랜잭션 없는 10,000행 행 단위 수치는 외삽값(행당 2.4ms)이며, Pipeline 수치는 트랜잭션 없이 측정한 값입니다. 트랜잭션을 사용하면 10,000행에 약 70ms가 걸립니다.

**세 드라이버 성능 데이터**(10,000행 배치 작업):

| 작업 | NewLife Pipeline(tx) | MySql.Data Batch(tx) | MySqlConnector Batch(tx) |
|------|------:|------:|------:|
| INSERT | 899ms | 1,927ms | 1,906ms |
| UPDATE | 710ms | 2,265ms | 2,041ms |
| DELETE | 661ms | 1,961ms | 1,767ms |

> 수천 행만으로도 로컬 LAN에서 **35배** 속도 향상을 달성하며, 데이터 센터를 넘는 환경에서는 이점이 더 큽니다. 자세한 데이터는 [성능 테스트 보고서](Doc/性能测试报告.md)를 참고하세요.

### 🔧 여섯 가지 배치 작업 방식

| 방식 | API | 사용 사례 |
|------|-----|---------|
| 딕셔너리 매개변수 집합 | `ExecuteBatch` | 컬렉션으로 동적 매개변수를 공급 |
| 값 배열 바인딩 | `ExecuteBatchValues` | 대량 가져오기(`MySqlBulkCopy` 내부), 딕셔너리/매개변수 객체 할당 제로 |
| 배열 바인딩 | `ExecuteArrayBatch` | 대용량 DML, Oracle 스타일 |
| 파이프라인 | `Pipeline=true` | 수만 / 수십만 행, 데이터 센터 간 |
| 다중 행 VALUES | SQL 연결 | 순수 INSERT 배치 |
| DbBatch(.NET 6+) | `CreateBatch` | 서로 다른 SQL 문의 혼합 배치 |

### 🛡️ 서드파티 의존성 제로

(역시 NewLife 팀이 만든) `NewLife.Core`에만 의존하므로 GPL 라이선스 리스크가 없고, MIT 라이선스로 상업적 사용이 자유로우며, 信创 환경과 보안 감사가 엄격한 시나리오에 적합합니다.

### ⚡ 극한의 성능 최적화

- `ArrayPool<T>` + `OwnerPacket`으로 추가 할당 제로
- 완전히 풀링된 `Pool.StringBuilder` / `Pool.MemoryStream`
- 전 구간 `async/await`, `sync-over-async` 안티패턴 없음
- 필요한 필드만 읽는 간결한 프로토콜 파싱

### 🌐 폭넓은 호환성

- **프레임워크**: `net45` / `net461` / `netstandard2.0` / `netstandard2.1` / `net6.0` / `net8.0` / `net10.0`
- **NativeAOT**: `net8.0+`는 트리밍과 NativeAOT를 지원합니다(`IsAotCompatible`, `AotSmoke` 스모크 테스트와 회귀 방지를 위한 CI 주류 RID 매트릭스 포함; 필수는 win-x64 / linux-x64 / linux-arm64, LoongArch linux-la64는 QEMU 에뮬레이션으로 포함(관찰 기간: 실행되지만 실패해도 차단하지 않음)), [AOT 호환성 평가](Doc/AOT兼容性评估.md) 참고
- **MySQL**: 5.5 ~ 9.0+, `mysql_native_password` + `caching_sha2_password`
- **호환 데이터베이스**: OceanBase, TiDB, Doris(자동 감지), Aurora / CloudSQL(클라우드 데이터베이스), MariaDB(기본 지원)
- **조건부 컴파일**: 프레임워크별 최적 구현을 자동 선택

---

## 기능 비교

| 기능 | NewLife.MySql | MySqlConnector | MySql.Data (Oracle) |
|------|:---:|:---:|:---:|
| **라이선스** | **MIT** | MIT | GPL(상업적 사용은 유료) |
| **국산(중국) 자주성** | ✅ | ❌ | ❌ |
| **서드파티 의존성** | **없음** | 없음 | 없음 |
| **진정한 비동기 I/O** | ✅ | ✅ | ❌ |
| **파이프라인 배치 실행** | ✅ | ❌ | ❌ |
| **배열 바인딩 배치** | ✅ | ❌ | ❌ |
| **딕셔너리 매개변수 집합 배치** | ✅ | ❌ | ❌ |
| **DbBatch(.NET 6+)** | ✅ | ✅ | ❌ |
| **DataAdapter** | ✅ | ❌ | ✅ |
| **EF Core** | ✅ | ✅ (Pomelo) | ✅ |
| **XCode ORM 통합** | ✅ | ❌ | ❌ |
| **대상 프레임워크** | net45~net10 | netstandard2.0+ | net462+ |
| **NativeAOT(net8.0+)** | ✅ | ✅ | ❌ |
| **OpenTelemetry 추적** | ✅ | ✅ | ✅ (8.1+) |
| **OceanBase / TiDB / Aurora / Doris** | ✅ 자동 감지 | ❌ | ❌ |
| **MySqlBulkCopy** | ✅ | ✅ | ✅ |
| **압축 프로토콜(zlib)** | ✅ | ✅ | ❌ |
| **Unix Socket** | ✅ | ✅ | ❌ |
| **WebAuthn 인증** | ✅ | ✅ | ❌ |
| **DbDataSource** | ✅ | ❌ | ❌ |

> 전체 기능과 아키텍처 분석은 [아키텍처](Doc/架构设计.md)를 참고하세요.

---

## 설치

```shell
dotnet add package NewLife.MySql
```

```xml
<PackageReference Include="NewLife.MySql" Version="1.0.*" />
```

---

## 빠른 시작

### 연결 문자열

```
Server=localhost;Port=3306;Database=mydb;User Id=root;******;
```

| 매개변수 | 별칭 | 기본값 | 설명 |
|------|------|--------|------|
| Server | DataSource, Data Source | - | 서버 주소 |
| Port | - | 3306 | 포트 번호 |
| Database | - | - | 데이터베이스 이름 |
| UserID | Uid, User Id, User | - | 사용자 이름 |
| Password | Pass, Pwd | - | 비밀번호 |
| ConnectionTimeout | Connection Timeout | 15 | 연결 제한 시간(초) |
| CommandTimeout | Default Command Timeout | 30 | 명령 제한 시간(초) |
| SslMode | Ssl Mode | None | SSL 모드(None/Preferred/Required) |
| UseServerPrepare | Use Server Prepare | false | 전역 서버 측 준비 스위치 |
| Pipeline | Pipelining | false | 파이프라인 실행 스위치 |

### 연결 열기

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=mydb;User Id=root;******;");
conn.Open();
// The connection is closed automatically and returned to the pool when the using scope ends
```

### 결과 집합 조회

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

### 스칼라 조회

```csharp
using var cmd = new MySqlCommand(conn, "SELECT COUNT(*) FROM users");
var count = cmd.ExecuteScalar();
```

### DML 실행

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES('Tom', 25)");
var affectedRows = cmd.ExecuteNonQuery();

// Shortcut
var rows = conn.ExecuteNonQuery("DELETE FROM logs WHERE created < '2024-01-01'");
```

---

## 매개변수화 쿼리

`@parameter-name` 구문으로 매개변수를 바인딩하며, 값은 자동으로 이스케이프되어 인젝션을 방지합니다:

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

**지원하는 매개변수 타입**:

| .NET 타입 | SQL 리터럴 예 |
|-----------|---------------|
| `String` | `'hello'`(특수 문자는 자동 이스케이프) |
| `Int32` / `Int64` 및 기타 숫자 타입 | `42` |
| `Boolean` | `1` 또는 `0` |
| `DateTime` | `'2025-07-01 12:30:00'` |
| `Byte[]` | `X'CAFE'` |
| `MySqlGeometry` | WKB 형식의 기하 데이터 |
| `Guid` | `'01234567-89ab-cdef-...'` |
| `Enum` | 숫자로 변환 |
| `null` / `DBNull.Value` | `NULL` |

---

## 트랜잭션

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

네 가지 격리 수준을 지원합니다: `ReadUncommitted` / `ReadCommitted` / `RepeatableRead`(기본값) / `Serializable`

```csharp
using var tr = conn.BeginTransaction(IsolationLevel.ReadCommitted);
```

트랜잭션이 커밋이나 롤백 없이 Dispose되면 자동으로 롤백됩니다.

---

## 저장 프로시저

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

## 준비된 문

`Prepare()`로 SQL을 서버에서 컴파일하며, 이후 실행은 바이너리 프로토콜을 사용합니다:

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

연결 문자열로 전역 활성화하여 모든 매개변수화 쿼리가 자동으로 바이너리 프로토콜을 사용하도록 할 수도 있습니다:

```
UseServerPrepare=true;
```

---

## 배치 작업

### 방식 A: 딕셔너리 매개변수 집합

동일한 SQL에 여러 매개변수 집합을 바인딩하여 실행하며, 내부에서 자동으로 준비합니다:

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

### 방식 B: 배열 바인딩(Oracle 스타일)

매개변수 값에 배열을 지정하고 실행 횟수를 명시합니다:

```csharp
using var cmd = new MySqlCommand(conn, "INSERT INTO users(name, age) VALUES(@name, @age)");
cmd.Parameters.AddWithValue("name", new[] { "Alice", "Bob", "Charlie" });
cmd.Parameters.AddWithValue("age", new[] { 25, 30, 22 });

var totalAffected = cmd.ExecuteArrayBatch(3);
// or await cmd.ExecuteArrayBatchAsync(3);
```

### 방식 C: 파이프라인 실행

연결 문자열에서 `Pipeline=true`를 활성화하면 네트워크 왕복 한 번으로 배치 전송과 배치 수신을 수행합니다:

```csharp
var connStr = "Server=localhost;Database=mydb;User Id=root;******;Pipeline=true;";
using var conn = new MySqlConnection(connStr);
conn.Open();

using var cmd = new MySqlCommand(conn, "UPDATE users SET age=@age WHERE name=@name");
cmd.Parameters.AddWithValue("age", agesArray);    // Int32[10000]
cmd.Parameters.AddWithValue("name", namesArray);   // String[10000]
var totalAffected = cmd.ExecuteArrayBatch(10000);
```

파이프라인은 INSERT / UPDATE / DELETE에 모두 적용되며, 딕셔너리 매개변수 집합이나 배열 바인딩과 조합할 수 있습니다. 네트워크 지연이 클수록(예: 데이터 센터 간) 이점이 커집니다.

### 방식 D: 다중 행 VALUES

```csharp
var sql = "INSERT INTO users(name, age) VALUES('Alice', 25), ('Bob', 30), ('Charlie', 22)";
conn.ExecuteNonQuery(sql);
```

### 방식 E: DbBatch API(.NET 6+)

```csharp
var batch = conn.CreateBatch();
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X1', 10)"));
batch.BatchCommands.Add(new MySqlBatchCommand("INSERT INTO users(name, age) VALUES('X2', 20)"));
batch.BatchCommands.Add(new MySqlBatchCommand("UPDATE users SET age = age + 1 WHERE name = 'X1'"));

using var reader = batch.ExecuteReader();
```

---

## EF Core 지원

`NewLife.MySql.EntityFrameworkCore` 패키지는 .NET 6 / .NET 8 / .NET 10을 지원하는 EF Core 데이터베이스 공급자를 제공합니다.

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

**지원 기능**:

| 기능 | 설명 |
|------|------|
| 타입 매핑 | Int / Long / String / DateTime / Decimal / Guid / Boolean / Byte[] |
| 쿼리 변환 | LINQ → MySQL 구문(백틱, LIMIT/OFFSET) |
| 메서드 변환 | Contains / StartsWith / EndsWith / ToUpper / ToLower / Trim / Replace / Substring / Length |
| 멤버 변환 | DateTime.Year / Month / Day / Hour / Minute / Second |
| 마이그레이션 지원 | CreateTable / AlterColumn / AddColumn / DropColumn / RenameTable / CreateIndex / DropIndex |
| 값 생성 전략 | AUTO_INCREMENT 자동 증가 기본 키 |

---

## SSL/TLS 암호화 연결

```csharp
// Encrypt if the server supports it, otherwise use plaintext
var connStr = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Preferred;";

// Require encryption; throw if the server does not support it
var connStr2 = "Server=localhost;Database=mydb;User Id=root;******;SslMode=Required;";
```

| SslMode | 동작 |
|---------|------|
| `None` / `Disabled` | SSL을 사용하지 않음(기본값) |
| `Preferred` | 암호화를 우선하고, 지원하지 않으면 평문으로 폴백 |
| `Required` | 암호화를 강제 |

---

## 스키마 정보 조회

```csharp
var databases = conn.GetSchema("Databases");  // Database list
var tables = conn.GetSchema("Tables");         // Table list
var columns = conn.GetSchema("Columns");       // Column information
var indexes = conn.GetSchema("Indexes");       // Index information
```

---

## 비동기 메서드

모든 핵심 작업은 비동기 버전을 지원합니다:

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

## 추적(OpenTelemetry)

드라이버에는 `ActivitySource` 계측이 내장되어 있으며(소스 이름 `NewLife.MySql`), `Open`(연결), `Execute`(명령, 리더 수명 포함), `Commit` / `Rollback`(트랜잭션) 네 종류의 스팬을 생성합니다. 속성은 OpenTelemetry 안정 데이터베이스 시맨틱 규약(`db.system.name` / `db.query.text` / `server.address` / `error.type` 등)을 따릅니다. OpenTelemetry SDK는 사용하는 쪽에서 제공하며, 드라이버는 어떤 의존성도 추가하지 않습니다:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("NewLife.MySql")
    .AddOtlpExporter()
    .Build();
```

- **적용 범위**: `net6.0` 이상, 하위 버전이나 구독자가 없을 때는 오버헤드 없는 no-op
- **개인정보 참고**: `db.query.text`는 명령 텍스트를 기록하므로, 리터럴이 텔레메트리에 남지 않도록 매개변수화 쿼리를 사용하는 것이 좋습니다

---

## 연결 풀

연결 풀은 기본으로 활성화되어 있으며 별도 설정이 필요 없습니다. 서로 다른 연결 문자열마다 독립적인 풀이 만들어집니다.

- 연결을 열면 풀에서 자동으로 가져옴
- 연결을 닫으면 자동으로 반환(TCP 연결은 끊지 않음)
- 유효하지 않은 연결은 자동으로 제거
- 필요할 때 새 연결 생성
- **네트워크 단절 후 자동 재연결**: 끊긴 연결을 감지하면 자동으로 재시도하여 장시간 실행 시나리오의 안정성을 높임

### 연결 풀 매개변수

| 매개변수 | 기본값 | 설명 |
|------|--------|------|
| MinPoolSize | 0 | 최소 연결 수 |
| MaxPoolSize | 100 | 최대 연결 수 |
| ConnectionLifeTime | 0 | 연결 수명(초), 0은 만료 없음 |
| ConnectionIdleTime | 300 | 유휴 제한 시간(초), 경과하면 연결 회수 |
| ValidationInterval | 30 | 유휴 검증 주기(초) |

```csharp
var connStr = "Server=localhost;Database=mydb;...;MinPoolSize=5;MaxPoolSize=50;ConnectionLifeTime=3600;";
```

---

## XCode ORM 통합

`MySqlClientFactory`를 통해 XCode의 MySQL 드라이버로 자동 등록됩니다:

```csharp
using XCode.DataAccessLayer;

DAL.AddConnStr("mysql", "Server=localhost;Database=mydb;User Id=root;******;", null, "MySql");

var dal = DAL.Create("mysql");
var tables = dal.Tables;
```

---

## 데이터베이스 전환

### ChangeDatabase(권장)

```csharp
using var conn = new MySqlConnection("Server=localhost;Database=db1;...");
conn.Open();
conn.ChangeDatabase("db2"); // Internally Close + Reopen
```

> ⚠️ 트랜잭션 도중 전환은 지원하지 않습니다(트랜잭션이 유실됩니다). 데이터베이스마다 독립적인 연결을 만드세요.

### SqlClient.SetDatabaseAsync(저수준 API)

```csharp
await conn.Client.SetDatabaseAsync("information_schema"); // COM_INIT_DB
```

연결을 닫지 않고 서버 측의 현재 데이터베이스만 전환하므로, 임시 조회 시나리오에 적합합니다.

---

## OceanBase / TiDB 지원

드라이버는 핸드셰이크 패킷의 버전 문자열에서 데이터베이스 유형을 자동으로 감지하며, 연결 방법은 MySQL과 완전히 동일합니다:

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

기본 SQL, 트랜잭션, 매개변수화 쿼리, 배치 작업 모두 호환됩니다.

---

## 대상 프레임워크

| 프레임워크 | 설명 |
|------|------|
| `net45` / `net461` | .NET Framework, 레거시 프로젝트와 호환 |
| `netstandard2.0` / `netstandard2.1` | 크로스 플랫폼 호환, 비동기 Dispose 지원 |
| `net6.0` | DbBatch API 추가 지원 |
| `net10.0` | 최신 .NET, 최적의 성능 |

---

## NativeAOT 배포

`net8.0+` 대상 프레임워크에서 드라이버는 실제 NativeAOT 게시 검증을 통과했습니다(로컬 측정과 win-x64, linux-x64 CI, 26개 스모크 시나리오 전부 통과, ILC 경고 0건). 네이티브 바이너리를 게시할 때 **호출 측 수정이 필요 없으며**, CI는 주류 RID 매트릭스(win-x64 / linux-x64 / linux-arm64 / win-arm64 / osx-x64 / osx-arm64 / musl, 그리고 관찰 기간 중 차단하지 않는 LoongArch linux-la64 QEMU 에뮬레이션 스모크 테스트)로 회귀를 지속적으로 감시합니다:

```xml
<PropertyGroup>
  <PublishAot>true</PublishAot>
</PropertyGroup>
```

```powershell
dotnet publish -c Release -r linux-x64
```

| 항목 | 내용 |
|------|------|
| 범위 | `net8.0+`; `net6.0`은 실험적 스위치가 필요하며 그 이전 프레임워크는 지원하지 않음 |
| 검증된 기능 | 연결/인증(핸드셰이크 연결 속성 포함), 동기 및 비동기 쿼리, 서버 측 준비, 배치(딕셔너리/배열/값 배열/파이프라인), 트랜잭션, 저장 프로시저, Schema 메타데이터, DbDataSource, DataAdapter, MySqlBulkCopy, 압축 프로토콜, binlog 구독, 추적 |
| 참고 | `MySqlDataAdapter`의 Fill/FillSchema/Update 계열에는 `RequiresUnreferencedCode` 주석이 있으며, 트리밍된 애플리케이션은 System.Data 의미론에 따라 호출 지점에서 보존 책임을 선언해야 함 |
| 상세 평가 | [AOT 호환성 평가](Doc/AOT兼容性评估.md) |

---

## 참고 사항

1. **MySQL 버전**: 5.x 이상 지원, 8.0+ 권장
2. **문자 인코딩**: 기본 UTF-8
3. **ChangeDatabase**: Close/Reopen으로 구현되며 트랜잭션 도중 전환은 지원하지 않으므로 독립적인 연결을 사용하세요
4. **파이프라인 실행**: 배치 DML 작업에만 사용하며 SELECT 쿼리에는 사용하지 않습니다

---

## 관련 문서

| 문서 | 설명 |
|------|------|
| [요구사항](Doc/需求文档.md) | 비전, 핵심 목표, 기능 방향 |
| [기능 목록](Doc/功能清单.md) | 구현/테스트/주석의 3차원 상태 추적 |
| [아키텍처](Doc/架构设计.md) | 아키텍처 개요, 프로토콜 구현, 기술 선택, 설계 결정, 드라이버 비교 |
| [성능 테스트 보고서](Doc/性能测试报告.md) | 배치 작업의 상세 벤치마크 데이터 |
| [변경 로그(중국어)](Doc/ChangeLog.md) | 중국어 릴리스 변경 로그 |
| [변경 로그(영어)](Doc/ChangeLog.en.md) | 영어 릴리스 변경 로그 |
| [简体中文 README](README.zh-CN.md) | 이 영어판과 같은 배치로 유지되는 중국어 문서 |
| [마이그레이션 가이드: MySql.Data → NL](Doc/迁移指南_MySqlData.md) | Oracle 공식 드라이버에서 마이그레이션하는 절차와 유의 사항 |
| [마이그레이션 가이드: MySqlConnector → NL](Doc/迁移指南_MySqlConnector.md) | 주류 커뮤니티 드라이버에서 마이그레이션하는 절차와 유의 사항 |

## 생태계 지도

NewLife는 각자 독립적으로 버전이 매겨지는 오픈 소스 프로젝트들의 계층적 생태계입니다. 아래 지도는 모든 NewLife 저장소에서 동일하게 공유되며, 각 프로젝트는 해당 저장소와 대표 NuGet 패키지로 연결됩니다.

| 프로젝트 | 역할 | 다운로드 |
| --- | --- | --- |
| **기반** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 로깅, 구성, 캐싱, 네트워킹, 직렬화, 스케줄링, APM 추적. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 데이터 미들웨어: 수백억 규모 단일 테이블, 자동 샤딩, 읽기/쓰기 분리, 10여 종 데이터베이스. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 고성능 네트워크 라이브러리: 단일 머신 1.77M pps / 14.5 Gbps(TCP 1KB, 2026-10), 4M 장수명 TCP 연결. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | 클라이언트/서버 애플리케이션과 IoT 기기를 위한 프로토콜 통신 및 RPC 프레임워크. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | .NET 애플리케이션을 운영체제 데몬으로 설치: Windows 서비스와 Linux systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | 콘솔, 웹, API, 서비스, 데이터, IoT 프로젝트용 `dotnet new` 프로젝트 템플릿. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **데이터 및 메시징** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | 마이크로초 지연, 초당 수백만 건 처리, 메시지 큐 내장 Redis 클라이언트. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Apache RocketMQ와 알리바바 클라우드 메시지 큐를 위한 순수 관리형 RocketMQ 클라이언트. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 클라이언트와 서버, 알리바바 클라우드 IoT 연결 포함. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C#로 만든 하이브리드 데이터베이스: 임베디드 또는 네트워크, 관계형, 시계열, 메시지 큐, KV를 하나로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | NewLife 데이터 스택 기반의 MySql 드라이버와 헬퍼 계층. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **웹, AI 및 분산** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 빠른 관리 플랫폼 프레임워크: 권한, SSO, OAuth 서버, CRUD 스캐폴딩. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 분산 서비스 플랫폼: 노드 관리, APM, 구성 센터, 레지스트리, 릴리스 센터. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 샤딩, 재시도, 백필을 갖춘 분산 작업 및 실시간 데이터 스케줄링 플랫폼. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 46개 LLM 공급자를 위한 통합 클라이언트와 즉시 실행 가능한 웹 채팅 애플리케이션 ChatAI. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **산업 및 IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | 프로토콜 계약과 기기 추상화를 정의하는 IoT 표준 라이브러리. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | NewLife.IoT 위에 구현한 Modbus TCP/RTU/ASCII. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | NewLife.IoT 위에 구현한 지멘스 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | NewLife.IoT 위에 구현한 오므론 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | NewLife.IoT 위에 구현한 미쓰비시 Melsec PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 산업 데이터 액세스를 위한 OPC 클라이언트. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 빌딩 자동화 프로토콜 스택. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 게이트웨이와 기기 관리를 갖춘 경량 오픈 소스 IoT 플랫폼. | — |
| **차량 텔레매틱스** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 차량 위치 프로토콜: 70여 종 메시지 본문, 2011/2013/2019. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | 운수 관리 부문을 위한 JT/T 809 플랫폼 간 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 차량 내 영상: RTP 파싱, FLV/fMP4 리먹싱, 라이브 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **도메인 컴포넌트** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Excel, Word, PPT, PDF 및 15가지 이상 문서 형식을 하나의 패키지로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 크로스 플랫폼 오디오: 코덱, 기기 I/O, DSP, 컨테이너, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 비디오 코덱, 컨테이너 형식, 프레임 처리, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 바이두, AMap, 텐센트, 티안디투를 지오코딩과 좌표 변환으로 통합한 인터페이스. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | 오프라인 메모리 매핑 데이터베이스와 온라인 API 폴백을 갖춘 IPv4 지역 정보. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **도구 및 제품** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 개발자 도구 상자: 네트워크, 시리얼 포트, 암호화, 정규식, Modbus, MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 셀프 호스팅 Git 소스 코드 관리 시스템. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 구버전 호스트에서 실행될 때 일치하는 .NET 런타임을 자동으로 설치. | — |

> 배지는 각 프로젝트의 대표 패키지를 보여 줍니다. 테마, 확장, 부속 패키지는 각 NuGet 페이지에 나열되어 있습니다.여기에 나열되지 않은 30여 개 패키지(FTP, LoRa, Office 확장, 프로토콜 드라이버 등)는 [NewLifeX 조직](https://github.com/NewLifeX)과 [newlifex.com](https://newlifex.com)에서 확인할 수 있습니다.

## 팀

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

NewLife 팀은 2002년에 창립되어 20년 넘게 .NET과 IoT 생태계에 매진해 왔으며, 약 100개의 오픈 소스 프로젝트(그중 34개가 이 지도의 핵심)를 유지하고, 전력, 물류, 의료, 산업 제어, 인터넷 산업에 소프트웨어와 하드웨어 통합 솔루션을 제공합니다. 우리는 "배움에 선후는 없고, 통달함이 존경을 얻는다"는 신조를 따릅니다.

오픈 소스 컴포넌트는 NuGet에서 <!--nuget-total:start-->6.48M<!--nuget-total:end-->회 다운로드되었습니다(생태계 전체 기준, 2026-10).

웹사이트: https://newlifex.com · 오픈 소스 조직: https://github.com/NewLifeX

<details>
<summary>위챗 공식 계정: 智能大石头 (스캔하여 팔로우)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>
