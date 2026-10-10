# NewLife.MySql AOT 兼容性评估

> 评估日期：2026-10-09｜基线：驱动 1.4.2026.1009 + NewLife.Core 12.0.2026.1009-beta0916｜评估方式：**真实 NativeAOT 编译 + 运行实测**（win-x64，.NET 10）+ 编译期分析器取证

---

## 1. 结论摘要

- **主链路在 NativeAOT 下实测可用**：12 个冒烟场景中 11 个在未做任何改造的情况下直接通过（连接、认证、查询、类型化读取、服务器预编译、字典参数集批量、值数组批量 + 管道化、事务、压缩协议、DataAdapter.Fill、MySqlBulkCopy、异步读取）。
- **唯一功能性缺口是"元数据保留"而非"动态能力"**：`ReadModelsAsync<T>` 依赖运行时反射枚举实体属性与无参构造，AOT 裁剪后失败；补一处泛型入口注解（`DynamicallyAccessedMembers`）后 12/12 全通。驱动不使用 Reflection.Emit / DLR / 脚本等设计性不支持的动态能力；唯一的表达式编译（实体 setter）在 .NET 10 NativeAOT 由官方解释器承载，**实测 `Expression.Compile` 可用**。
- **静默降级已修复**：AOT 下 `ConnectAttributes` 私有属性被裁剪，握手连接属性块变为空（服务端 `session_connect_attrs` 实测 0 条），无异常、无日志；已改为 `typeof` 静态字面量取属性（P0-3），AOT 复测 7 类属性完整下发（§2.2）。
- **工作项已完成并实测**：P0（功能性三项）与 P1（告警收敛五项）已于 2026-10-09 实施完成——库自身 `IsAotCompatible` 常驻后 net8.0/net10.0 分析器 **0 条告警**、AOT 发布期驱动侧 **0 条**（§2.3），NativeAOT 复测 **24/24 场景全通**（S0~S19 + P1~P4，含连接属性、反射/表达式探针、链路追踪、Schema/DataSource/存储过程/Binlog/DataAdapter）。P2 验证闭环（冒烟工程入库与 CI 防回归）同日落地，见 §3。
- **EF Core 提供程序已对齐**（2026-10-10）：`NewLife.MySql.EntityFrameworkCore` 打开 `IsAotCompatible` 后修复 `AddMySql<TContext>` 泛型注解（IL2091），net8.0/net10.0 分析器 **0 条告警**（§2.3）；运行时 AOT 能力取决于 EF Core 本体与编译模型，见 §4 边界。
- **已正式标注 AOT 兼容**：主链路 0 告警 + 真实 AOT 实测全通 + 防回归 CI（分析器守卫与 NativeAOT 冒烟）已建立（§3 P2），README、竞品分析与 NuGet 包描述/标签均已同步。

---

## 2. 实测数据

### 2.1 冒烟场景（24 项：20 数据库场景 + 4 纯内存场景，win-x64 / .NET 10）

| # | 场景 | JIT 基线 | NativeAOT（原始） | NativeAOT（修复后） |
|---|------|---------|------------------|------------------------------|
| S0 | 建库（DDL） | ✅ | ✅ | ✅ |
| S1 | 连接 + ExecuteScalar | ✅ | ✅ | ✅ |
| S2 | 建表 + 参数化插入 + 类型化读取 + GetSchemaTable | ✅ | ✅ | ✅ |
| S3 | 服务器预编译 + 参数 | ✅ | ✅ | ✅ |
| S4 | ExecuteBatch 字典参数集（50 行） | ✅ | ✅ | ✅ |
| S5 | ExecuteBatchValues 值数组 + Pipeline（50 行） | ✅ | ✅ | ✅ |
| S6 | `ReadModelsAsync<T>` 实体映射 | ✅ | ❌ 实体构造被裁剪 | ✅ |
| S7 | MySqlDataAdapter.Fill | ✅ | ✅ | ✅ |
| S8 | MySqlBulkCopy（500 行） | ✅ | ✅ | ✅ |
| S9 | 事务提交与回滚 | ✅ | ✅ | ✅ |
| S10 | 压缩协议（zlib） | ✅ | ✅ | ✅ |
| S11 | 异步读取（ReadAsync） | ✅ | ✅ | ✅ |
| S12 | 握手连接属性下发 | ✅ 7 类属性 | ❌ 静默丢失（0 条） | ✅ 7 类属性（P0-3 修复后） |
| S13 | 反射/表达式探针（构造、属性、`Expression.Compile`） | ✅ | ❌ 构造与属性被裁剪 | ✅（P0 注解保留后） |
| S14 | 链路追踪（`ActivitySource` 埋点） | ✅ | — | ✅（SYS-11 新增） |
| S15 | Schema 元数据（`GetSchema` TABLES/COLUMNS） | ✅ | — | ✅ |
| S16 | `MySqlDataSource`（DbDataSource） | ✅ | — | ✅ |
| S17 | 存储过程 IN/OUT 参数 | ✅ | — | ✅ |
| S18 | Binlog 订阅（`SubscribeAsync` + WRITE_ROWS 事件） | ✅ | — | ✅ |
| S19 | DataAdapter Fill / FillSchema / Update | ✅ | — | ✅（调用方声明保留责任） |
| P1 | 工厂创建（Connection/Command/Parameter/DataAdapter） | ✅ | — | ✅ |
| P2 | 参数替换与转义（注入防护路径） | ✅ | — | ✅ |
| P3 | `MySqlGeometry` WKB 封装 | ✅ | — | ✅ |
| P4 | DbDataSource 创建（不连接） | ✅ | — | ✅ |

> 复测（2026-10-10）：NativeAOT 真实发布件 **24/24 场景全部 PASS**（S0~S19 + P1~P4，含连接属性、反射/表达式探针、链路追踪、Schema/DataSource/存储过程/Binlog/DataAdapter），且 **无任何调用方补丁**——实体元数据保留完全由库侧泛型入口注解传导。P1~P4 为纯内存场景，可用 `--no-db` 在无 MySQL 环境运行。

- AOT 判真：原生发布件 `Assembly.Location` 为空、JIT 为磁盘路径（另：冒烟工程固定"严格 AOT 姿态"，两种运行的 `IsDynamicCodeSupported` 均为 False，该值不作判别依据）。
- S6 原始失败：`Fail to create object type=AotSmoke.Row parameters= No parameterless constructor defined for type 'AotSmoke.Row'`——实体无参构造未被反射元数据保留。
- S6 修复验证：调用方以 `DynamicallyAccessedMembers(PublicParameterlessConstructor | PublicProperties)` 保留后，`ctor=True props=4`、`Activator.CreateInstance` OK、`Reflect.CreateInstance` OK、`Expression.Compile` OK、S6 通过。

### 2.2 连接属性静默降级（A/B 证据）

| 运行形态 | 服务端 `performance_schema.session_connect_attrs` |
|---------|--------------------------------------------------|
| JIT | 7 类属性（`_client_name=NewLife.MySql`、`_client_version`、`_os`、`_platform`、`_pid`、`_framework`、`_client_licence`） |
| NativeAOT | **0 条** |
| NativeAOT（P0-3 修复后） | ✅ 7 类属性完整下发（35 行 = 连接池 5 连接 × 7 属性） |

原因：`Authentication.GetConnectAttrs()` 经 `att.GetType().GetProperties()` 反射枚举 `ConnectAttributes` 私有属性；AOT 裁剪后属性消失，属性块长度为 0，握手仍成功。已修复：改用 `typeof(ConnectAttributes)` 静态字面量，裁剪分析器据此保留属性元数据。

### 2.3 编译期告警（库自身 `IsAotCompatible=true`）

| TFM | 原始告警（唯一位置） | 分布 |
|-----|---------------------|------|
| net10.0 | 19 | IL2026×13、IL2046、IL2070、IL2072、IL2075、IL2093、IL2111 |
| net8.0 | 18 | IL2026×13、IL2046、IL2070、IL2072、IL2093、IL2111 |

AOT 发布期 ILC 对驱动报告 5 条唯一：`IL2075`（Authentication）、`IL2070`（MySqlDataReader）、`IL2111`（MySqlDataReader.GetSchemaTable）、`IL2046`（MySqlDataAdapter.Update）、`IL2093`（MySqlDataReader.GetFieldType）。ILC 仅对保留代码报告，分析器清单为完整口径。

> 修复后复测（2026-10-09，P0+P1 实施完成）：net8.0/net10.0 分析器 **0 条**；AOT 发布期驱动侧 **0 条**（仅剩冒烟工程自身诊断代码的 4 条，与本库无关）。`IsAotCompatible` 已在 csproj 对 net8.0+ 常驻启用，防止回归。

> EF Core 提供程序（2026-10-10）：`NewLife.MySql.EntityFrameworkCore` 启用 `IsAotCompatible` 后暴露 IL2091×2（`AddMySql<TContext>` 未向 `AddDbContext<TContext>` 传导构造/属性保留要求），已按基类同款 `DynamicallyAccessedMembers(PublicConstructors | NonPublicConstructors | PublicProperties)` 注解泛型参数修复，net8.0/net10.0 分析器 **0 条**（net6.0 亦为 0）。

> 提示：ILC 只认 `UnconditionalSuppressMessage`，不认 `#pragma warning disable`；库内声明一律用特性（沿用 Core 的结论）。

### 2.4 验证矩阵（RID 与执行环境）

| RID / 环境 | 执行方式 | 状态 |
|-----------|---------|------|
| win-x64 / .NET 10 | 本地 `dotnet publish -r win-x64` 原生运行 | ✅ 24/24 PASS，ILC 0 告警 |
| linux-x64 / .NET 10 | CI `aot.yml` → native-smoke（运行器 MySQL 8，20 数据库场景） | 作业已配置，待首次运行 |
| win-x64 / .NET 10 | CI `aot.yml` → native-smoke-windows（`--no-db`，5 项纯内存场景） | 作业已配置，待首次运行 |
| 其他 RID（linux-arm64 / osx-arm64 等） | 未覆盖 | 纯托管实现无平台特定代码，理论等价；如需可扩展 CI 矩阵 |

---

## 3. 工作项清单（改造路线）

> **状态（2026-10-09）**：P0-1 ~ P0-3、P1-1 ~ P1-5 与 P2-1/P2-2 **已全部实施并通过验证**（详见 §1/§2 复测数据）。
> **状态（2026-10-10）**：场景扩展至 24 项（新增 Schema/DataSource/存储过程/Binlog/DataAdapter 与 4 项纯内存）；CI 新增 Windows NativeAOT 作业并扩大分析器守卫范围（含 EF Core 提供程序）；EF Core 提供程序注解对齐后 0 告警。

### P0 功能性（不修则 AOT 下行为缺失/失败）

| # | 项 | 位置 | 处置方向 |
|---|---|------|---------|
| P0-1 | `ReadModelsAsync<T>` 实体元数据保留 | `MySqlDataReader.ReadModelsAsync<T>` | 泛型参数加 `[DynamicallyAccessedMembers(PublicProperties \| PublicParameterlessConstructor)]`，经泛型入口向调用方传导保留要求 |
| P0-2 | 属性映射装饰注解 | `MySqlDataReader.GetModelProperties` | `Type` 形参加 `[DynamicallyAccessedMembers(PublicProperties)]`，与 P0-1 同切片 |
| P0-3 | 握手连接属性保留 | `Authentication.GetConnectAttrs` | 改为 `typeof(ConnectAttributes).GetProperties()` 静态类型化，消除 IL2075 并确保属性保留下发 |

### P1 告警收敛（对齐 0 告警目标，可与 P0 分批）

| # | 项 | 位置 | 处置方向 |
|---|---|------|---------|
| P1-1 | `GetFieldType` 返回注解与基类对齐 | `MySqlDataReader.GetFieldType` | 按 `DbDataReader` 基类同款 `[return: DynamicallyAccessedMembers(...)]` 声明（IL2093） |
| P1-2 | `FillSchema` / `Update` 系列裁剪语义声明 | `MySqlDataAdapter`（14 处） | 覆写与基类一致地标注 `RequiresUnreferencedCode`（IL2046），BCL RUC 调用按语义声明/抑制（IL2026） |
| P1-3 | Schema 数据类型流转 | `SchemaProvider`（IL2072） | `DataColumnCollection.Add` 入参保留要求显式化或定点抑制 |
| P1-4 | `GetSchemaTable` 的 `typeof(Type)` | `MySqlDataReader.GetSchemaTable`（IL2111） | `UnconditionalSuppressMessage` + 理由注释 |
| P1-5 | `IsAotCompatible` 常驻 | `NewLife.MySql.csproj` net8.0+ | 对齐 Core：对 net8.0+（含未来 `-windows` 形态）启用，保护后续回归 |

### P2 验证闭环

| # | 项 | 说明 |
|---|---|------|
| P2-1 | 冒烟工程 `AotSmoke` 入库 | ✅ 已入库 `AotSmoke/`（net10.0 + PublishAot），24 场景（20 数据库场景 S0~S19 + 4 纯内存场景 P1~P4，含连接属性、反射/表达式探针、链路追踪、Schema/DataSource/存储过程/Binlog/DataAdapter）；连接串支持参数/环境变量 `MYSQL_CONN`，支持 `--drop` 清理与 `--no-db` 无数据库运行；已加入解决方案 |
| P2-2 | CI 防回归 | ✅ 已建立 `.github/workflows/aot.yml`：① 分析器守卫（驱动与 EF Core 提供程序的 net8.0/net10.0 构建，`warning IL####` 命中即失败）；② Linux NativeAOT 冒烟（linux-x64 真实发布 + 运行器自带 MySQL 8 上运行 20 个数据库场景，ILC 告警命中即失败）；③ Windows NativeAOT 冒烟（win-x64 真实发布 + `--no-db` 纯内存场景） |

### 依赖与参考

- 驱动所用 Core 能力已经是 AOT 友好的：包数据（`IPacket`）、`SpanReader`、日志、定时器均为 Core 声明的"核心链路已支持"范围。
- Core 12 新增 `Reflect.CreateInstance<T>` / `GetPropertyEx<T>` 等泛型入口可作为 P0-1 的实现备选（把保留要求放到泛型参数上）。
- 若后续 Core 收紧 `Expression.Compile`（当前 .NET 10 NativeAOT 经解释器可用），实体 setter 可切换到 Core 的 `MemberAccessorTable`（源生成器生成，AOT 友好）。

---

## 4. 已知边界（不在本次评估范围）

| 项 | 说明 |
|---|------|
| EF Core Provider 包 | `NewLife.MySql.EntityFrameworkCore`（net6/8/10）已启用 `IsAotCompatible` 并通过分析器（0 告警），注解完整；**运行时** AOT 能力取决于 EF Core 本体（EF Core 10 起支持编译模型 + NativeAOT 的受限场景），提供程序侧不做额外承诺，端到端 EF AOT 实测不在本次范围 |
| EF6 Provider 包 | `NewLife.MySql.EntityFramework` 目标 net472，与 NativeAOT 无关 |
| 低版本目标 | net45/net461/netstandard2.0/2.1/net6.0 不在 AOT 支持范围（AOT 自 net8.0 起；net6.0 需实验开关） |
| 动态能力 | 驱动不使用脚本/插件/DLR 等设计性不支持的动态能力，无需守卫降级 |

## 5. 复现方法

```powershell
# 1) 编译期分析器（IsAotCompatible 已对 net8.0+ 常驻，期望 0 条；CI 由 aot.yml 守卫）
dotnet build NewLife.MySql\NewLife.MySql.csproj -f net10.0 -c Release
dotnet build NewLife.MySql.EntityFrameworkCore\NewLife.MySql.EntityFrameworkCore.csproj -f net10.0 -c Release

# 2) JIT 基线冒烟（需本地 MySQL，连接串可用参数或 MYSQL_CONN 环境变量，默认本机 root/root）
dotnet run -c Release --project AotSmoke\AotSmoke.csproj
dotnet run -c Release --project AotSmoke\AotSmoke.csproj -- --no-db   # 无 MySQL：仅跑纯内存场景

# 3) 真实 NativeAOT 冒烟（PublishAot 写在工程文件内）
dotnet publish AotSmoke\AotSmoke.csproj -c Release -r win-x64 -o out
.\out\AotSmoke.exe                                           # 逐场景 PASS/FAIL，非 0 退出码即失败
.\out\AotSmoke.exe "..\Bin\UnitTest\Config\mysql.config"     # 也可传连接串或配置文件路径
.\out\AotSmoke.exe --no-db                                   # 跳过依赖数据库的场景
.\out\AotSmoke.exe --drop                                    # 清理冒烟库
```

冒烟自建 `aotsmoke` 库，运行后可 `--drop` 清理。CI（[`.github/workflows/aot.yml`](../.github/workflows/aot.yml)）在 linux-x64 + 运行器 MySQL 8 上执行 20 个数据库场景，在 windows-latest 上执行 win-x64 发布 + `--no-db` 纯内存场景，并对分析器与 ILC 的 `warning IL####` 实施硬约束（命中即失败）。

---

（完）
