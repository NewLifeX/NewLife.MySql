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
🌐 [English](README.md) | [简体中文](README.zh-CN.md) | [繁體中文](README.zh-TW.md) | [Español](README.es.md) | [Deutsch](README.de.md) | [日本語](README.ja.md) | [Français](README.fr.md) | [Português](README.pt.md) | [Русский](README.ru.md) | [العربية](README.ar.md)
<!-- lang:end -->

> ⚠️ 이 번역은 업데이트가 늦어질 수 있습니다. 정식 내용은 [영어 README](README.md)를 기준으로 합니다.

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
| 압축 (zlib) | ✅ | ✅ | ❌ |
| Unix Socket | ✅ | ✅ | ❌ |
| WebAuthn 인증 | ✅ | ✅ | ❌ |
| DbDataSource | ✅ | ❌ | ❌ |
| OceanBase / TiDB / Aurora / Doris | ✅ 자동 감지 | ❌ | ❌ |
| 대상 프레임워크 | net45 ~ net10 | net462+ | net462+ |
| NativeAOT (net8.0+) | ✅ | ✅ | ❌ |
| OpenTelemetry 추적 | ✅ | ✅ | ✅ (8.1+) |

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

## 생태계 지도

NewLife는 독립적으로 버전이 관리되는 오픈소스 프로젝트의 계층형 생태계입니다. 아래 지도는 모든 NewLife 리포지토리에서 그대로 동일하게 공유되며, 각 프로젝트는 코드 리포지토리와 기본 NuGet 패키지로 연결됩니다.

| 프로젝트 | 한 줄 소개 | 다운로드 |
| --- | --- | --- |
| **기반** | | |
| [NewLife.Core](https://github.com/NewLifeX/X) | 로깅, 구성, 캐시, 네트워크, 직렬화, 스케줄링, APM 추적. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Core?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Core) |
| [NewLife.XCode](https://github.com/NewLifeX/NewLife.XCode) | 데이터 미들웨어: 단일 테이블 수백억 건, 자동 분할 테이블, 읽기/쓰기 분리, 10여 종 데이터베이스 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCode?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCode) |
| [NewLife.Net](https://github.com/NewLifeX/NewLife.Net) | 고성능 네트워크 라이브러리: 단일 노드 177만 pps, 14.5 Gbps(TCP 1KB, 2026-10 실측), 400만 Tcp 장기 연결. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Net?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Net) |
| [NewLife.Remoting](https://github.com/NewLifeX/NewLife.Remoting) | CS 애플리케이션과 IoT 장치를 위한 프로토콜 통신 및 RPC 프레임워크. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Remoting?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Remoting) |
| [NewLife.Agent](https://github.com/NewLifeX/NewLife.Agent) | .NET 애플리케이션을 운영체제 데몬으로 설치: Windows 서비스와 Linux systemd. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Agent?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Agent) |
| [NewLife.Templates](https://github.com/NewLifeX/NewLife.Zero) | 콘솔, Web, API, 서비스, 데이터, IoT용 `dotnet new` 프로젝트 템플릿. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Templates?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Templates) |
| **데이터 및 메시징** | | |
| [NewLife.Redis](https://github.com/NewLifeX/NewLife.Redis) | Redis 클라이언트: 마이크로초 수준 지연, 백만 수준 처리량, 메시지 큐 내장. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Redis?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Redis) |
| [NewLife.RocketMQ](https://github.com/NewLifeX/NewLife.RocketMQ) | Apache RocketMQ와 알리바바 클라우드 메시지 큐를 지원하는 순수 관리형 RocketMQ 클라이언트. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.RocketMQ?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.RocketMQ) |
| [NewLife.MQTT](https://github.com/NewLifeX/NewLife.MQTT) | MQTT 클라이언트와 서버, 알리바바 클라우드 IoT 연결 포함. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MQTT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MQTT) |
| [NewLife.NovaDb](https://github.com/NewLifeX/NewLife.NovaDb) | C# 하이브리드 데이터베이스: 임베디드/네트워크 두 모드, 관계형, 시계열, 메시지 큐, KV를 하나로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.NovaDb?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.NovaDb) |
| [NewLife.MySql](https://github.com/NewLifeX/NewLife.MySql) | NewLife 데이터 스택 위에 구축한 MySql 드라이버와 도우미 계층. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.MySql?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.MySql) |
| **Web, AI 및 분산** | | |
| [NewLife.Cube](https://github.com/NewLifeX/NewLife.Cube) | 빠른 관리 플랫폼 프레임워크: 권한, SSO, OAuth 서버, CRUD 스캐폴딩. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Cube?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Cube) |
| [Stardust](https://github.com/NewLifeX/Stardust) | 분산 서비스 플랫폼: 노드 관리, APM, 구성 센터, 등록 센터, 배포 센터. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Stardust?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Stardust) |
| [AntJob](https://github.com/NewLifeX/AntJob) | 샤딩, 재시도, 백필을 지원하는 분산 작업 및 실시간 데이터 스케줄링 플랫폼. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AntJob?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AntJob) |
| [NewLife.AI](https://github.com/NewLifeX/NewLife.AI) | 46개 LLM 공급자를 통합 연결하는 클라이언트와 바로 사용할 수 있는 ChatAI 웹 채팅 애플리케이션. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.AI?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.AI) |
| **산업 및 IoT** | | |
| [NewLife.IoT](https://github.com/NewLifeX/NewLife.IoT) | 프로토콜 계약과 장치 추상을 정의하는 IoT 표준 라이브러리. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IoT?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IoT) |
| [NewLife.Modbus](https://github.com/NewLifeX/NewLife.Modbus) | NewLife.IoT 기반으로 구현한 Modbus TCP/RTU/ASCII. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Modbus?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Modbus) |
| [NewLife.Siemens](https://github.com/NewLifeX/NewLife.Siemens) | NewLife.IoT 기반으로 구현한 지멘스 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Siemens?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Siemens) |
| [NewLife.Omron](https://github.com/NewLifeX/NewLife.Omron) | NewLife.IoT 기반으로 구현한 오므론 PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Omron?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Omron) |
| [NewLife.Melsec](https://github.com/NewLifeX/NewLife.Melsec) | NewLife.IoT 기반으로 구현한 미쓰비시 Melsec PLC 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Melsec?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Melsec) |
| [NewLife.OPC](https://github.com/NewLifeX/NewLife.OPC) | 산업 데이터 수집용 OPC 클라이언트. | — |
| [NewLife.BACnet](https://github.com/NewLifeX/NewLife.BACnet) | BACnet 빌딩 자동화 프로토콜 스택. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.BACnet?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.BACnet) |
| [ZeroIoT](https://github.com/NewLifeX/ZeroIoT) | 게이트웨이와 장치 관리를 갖춘 경량 오픈소스 IoT 플랫폼. | — |
| **차량 텔레매틱스** | | |
| [NewLife.JT808](https://github.com/NewLifeX/NewLife.JT808) | JT/T 808 차량 위치 프로토콜: 70개 이상 메시지 본문, 2011/2013/2019 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT808?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT808) |
| [NewLife.JT809](https://github.com/NewLifeX/NewLife.JT809) | 운송 관리 부서를 위한 JT/T 809 플랫폼 간 프로토콜. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT809?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT809) |
| [NewLife.JT1078](https://github.com/NewLifeX/NewLife.JT1078) | JT/T 1078 차량용 비디오: RTP 파싱, FLV/fMP4 리먹싱, 라이브 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.JT1078?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.JT1078) |
| **도메인 컴포넌트** | | |
| [NewLife.Office](https://github.com/NewLifeX/NewLife.Office) | Excel, Word, PPT, PDF 등 15개 이상 문서 형식을 하나의 패키지로. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Office?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Office) |
| [NewLife.Audio](https://github.com/NewLifeX/NewLife.Audio) | 크로스 플랫폼 오디오: 코덱, 장치 IO, DSP, 컨테이너, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Audio?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Audio) |
| [NewLife.Video](https://github.com/NewLifeX/NewLife.Video) | 비디오 코덱, 컨테이너 형식, 프레임 처리, 스트리밍. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Video?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Video) |
| [NewLife.Map](https://github.com/NewLifeX/NewLife.Map) | 바이두/고덕/텐센트/톈디투 지도 통합 인터페이스, 지오코딩과 좌표 변환 지원. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.Map?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.Map) |
| [NewLife.IP](https://github.com/NewLifeX/NewLife.IP) | IPv4 위치 정보: 오프라인 메모리 매핑 데이터베이스와 온라인 API 이중 소스. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.IP?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.IP) |
| **도구 및 제품** | | |
| [CrazyCoder](https://github.com/NewLifeX/XCoder) | 개발자 도구 모음: 네트워크, 시리얼 포트, 암호화, 정규식, Modbus, MQTT. | [![NuGet](https://img.shields.io/nuget/dt/NewLife.XCoder?style=flat-square&logo=nuget&label=downloads)](https://www.nuget.org/packages/NewLife.XCoder) |
| [GitCandy](https://github.com/NewLifeX/GitCandy) | 셀프 호스팅 Git 소스 코드 관리 시스템. | — |
| [LuckyClover](https://github.com/NewLifeX/LuckyClover) | 구버전 호스트에서 시작할 때 일치하는 .NET 런타임을 자동으로 설치합니다. | — |

> 배지는 각 프로젝트의 기본 패키지를 나타냅니다. 테마, 확장, 위성 패키지는 해당 NuGet 페이지에서 확인하세요.여기에 나열하지 않은 30여 개 패키지(FTP, LoRa, Office 확장, 각종 프로토콜 드라이버 등)는 [NewLifeX 조직](https://github.com/NewLifeX)과 [newlifex.com](https://newlifex.com)에서 확인할 수 있습니다.

## 뉴라이프 개발팀

<img src="https://newlifex.com/logo.png" width="48" align="right" alt="NewLife" />

뉴라이프 팀(NewLife)은 2002년에 설립되어 20여 년간 .NET과 IoT 기술 생태계에 집중하며 100여 개의 오픈소스 프로젝트를 주도하고(이 중 34개가 핵심 프로젝트), 전력, 물류, 의료, 산업 제어, 인터넷 등 산업에 소프트웨어와 하드웨어를 아우르는 통합 솔루션을 제공해 왔습니다. "학문에는 선후가 없으며, 이룬 사람이 스승이다"라는 신조를 따릅니다.

오픈소스 컴포넌트는 NuGet에서 누적 <!--nuget-total:start-->6.48M<!--nuget-total:end-->회 다운로드되었습니다(생태계 전체 기준, 2026-10).

웹사이트: https://newlifex.com · 오픈소스 조직: https://github.com/NewLifeX

<details>
<summary>위챗 공식 계정: 智能大石头(스캔하여 팔로우)</summary>

![智能大石头](https://newlifex.com/stone.jpg)

</details>

## 라이선스

MIT License — 개인 및 상업적 사용 무료.

Copyright © 2002-2026 [NewLife](https://newlifex.com)
