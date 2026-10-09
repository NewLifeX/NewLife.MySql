# NewLife.MySql AOT 兼容性评估

> 评估日期：2026-10-09｜基线：驱动 1.4.2026.1009 + NewLife.Core 12.0.2026.1009-beta0916｜评估方式：**真实 NativeAOT 编译 + 运行实测**（win-x64，.NET 10）+ 编译期分析器取证

---

## 1. 结论摘要

- **主链路在 NativeAOT 下实测可用**：12 个冒烟场景中 11 个在未做任何改造的情况下直接通过（连接、认证、查询、类型化读取、服务器预编译、字典参数集批量、值数组批量 + 管道化、事务、压缩协议、DataAdapter.Fill、MySqlBulkCopy、异步读取）。
- **唯一功能性缺口是"元数据保留"而非"动态能力"**：`ReadModelsAsync<T>` 依赖运行时反射枚举实体属性与无参构造，AOT 裁剪后失败；补一处泛型入口注解（`DynamicallyAccessedMembers`）后 12/12 全通。驱动不使用 Reflection.Emit / DLR / 脚本等设计性不支持的动态能力；唯一的表达式编译（实体 setter）在 .NET 10 NativeAOT 由官方解释器承载，**实测 `Expression.Compile` 可用**。
- **存在静默降级需要修复**：AOT 下 `ConnectAttributes` 私有属性被裁剪，握手连接属性块变为空（服务端 `session_connect_attrs` 实测 0 条），无异常、无日志——属"沉默行为变化"，必须修。
- **剩余工作是"注解与告警收敛"**：库自身开启 `IsAotCompatible` 后 net10.0 19 条 / net8.0 18 条唯一告警；AOT 发布期驱动侧 5 条唯一告警。全部可经声明（`RequiresUnreferencedCode` / `UnconditionalSuppressMessage`）或静态化消除，预计为小型切片，无架构改造。
- **当前不建议对外宣称"AOT 兼容"**：需完成 §3 的 P0 + P1 工作项并建立 CI 防回归（对齐 Core 的 `AotSmoke` + `aot.yml` 方案）后再标注。

---

## 2. 实测数据

### 2.1 冒烟场景（12 项，win-x64 / .NET 10）

| # | 场景 | JIT 基线 | NativeAOT（原始） | NativeAOT（保留实体元数据后） |
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
| S12 | 握手连接属性下发 | ✅ 7 类属性 | ❌ 静默丢失（0 条） | 待 P0-3 修复 |

- AOT 判真：原生发布件 `Assembly.Location` 为空、JIT 为磁盘路径（另：冒烟工程固定"严格 AOT 姿态"，两种运行的 `IsDynamicCodeSupported` 均为 False，该值不作判别依据）。
- S6 原始失败：`Fail to create object type=AotSmoke.Row parameters= No parameterless constructor defined for type 'AotSmoke.Row'`——实体无参构造未被反射元数据保留。
- S6 修复验证：调用方以 `DynamicallyAccessedMembers(PublicParameterlessConstructor | PublicProperties)` 保留后，`ctor=True props=4`、`Activator.CreateInstance` OK、`Reflect.CreateInstance` OK、`Expression.Compile` OK、S6 通过。

### 2.2 连接属性静默降级（A/B 证据）

| 运行形态 | 服务端 `performance_schema.session_connect_attrs` |
|---------|--------------------------------------------------|
| JIT | 7 类属性（`_client_name=NewLife.MySql`、`_client_version`、`_os`、`_platform`、`_pid`、`_framework`、`_client_licence`） |
| NativeAOT | **0 条** |

原因：`Authentication.GetConnectAttrs()` 经 `att.GetType().GetProperties()` 反射枚举 `ConnectAttributes` 私有属性；AOT 裁剪后属性消失，属性块长度为 0，握手仍成功——需要 P0-3 修复。

### 2.3 编译期告警（库自身 `IsAotCompatible=true`）

| TFM | 原始告警 | 唯一位置 | 分布 |
|-----|---------|---------|------|
| net10.0 | 38 | 19 | IL2026×13、IL2046×2、IL2070、IL2072、IL2075、IL2093、IL2111 |
| net8.0 | 36 | 18 | IL2026×13、IL2046×2、IL2070、IL2072、IL2093、IL2111 |

AOT 发布期 ILC 对驱动报告 5 条唯一：`IL2075`（Authentication）、`IL2070`（MySqlDataReader）、`IL2111`（MySqlDataReader.GetSchemaTable）、`IL2046`（MySqlDataAdapter.Update）、`IL2093`（MySqlDataReader.GetFieldType）。ILC 仅对保留代码报告，分析器清单为完整口径。

> 提示：ILC 只认 `UnconditionalSuppressMessage`，不认 `#pragma warning disable`；库内声明一律用特性（沿用 Core 的结论）。

---

## 3. 工作项清单（改造路线）

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

### P2 验证闭环（建议随改造一起落地）

| # | 项 | 说明 |
|---|---|------|
| P2-1 | 冒烟工程 `AotSmoke` 入库 | 沿用本次临时探针的 12 场景结构（评估用工程未入库） |
| P2-2 | CI 防回归 | 对齐 Core 的 `aot.yml`：`IsAotCompatible` 分析器硬约束 + win-x64 真实 AOT 冒烟（`PublishAot` 写在工程内，不用命令行全局属性，避免多目标依赖报 NETSDK1207） |

### 依赖与参考

- 驱动所用 Core 能力已经是 AOT 友好的：包数据（`IPacket`）、`SpanReader`、日志、定时器均为 Core 声明的"核心链路已支持"范围。
- Core 12 新增 `Reflect.CreateInstance<T>` / `GetPropertyEx<T>` 等泛型入口可作为 P0-1 的实现备选（把保留要求放到泛型参数上）。
- 若后续 Core 收紧 `Expression.Compile`（当前 .NET 10 NativeAOT 经解释器可用），实体 setter 可切换到 Core 的 `MemberAccessorTable`（源生成器生成，AOT 友好）。

---

## 4. 已知边界（不在本次评估范围）

| 项 | 说明 |
|---|------|
| EF Core Provider 包 | `NewLife.MySql.EntityFrameworkCore`（net6/8/10）的 AOT 支持依赖 EF Core 10 编译模型方案，需单独评估，本次未覆盖 |
| EF6 Provider 包 | `NewLife.MySql.EntityFramework` 目标 net472，与 NativeAOT 无关 |
| 低版本目标 | net45/net461/netstandard2.0/2.1/net6.0 不在 AOT 支持范围（AOT 自 net8.0 起；net6.0 需实验开关） |
| 动态能力 | 驱动不使用脚本/插件/DLR 等设计性不支持的动态能力，无需守卫降级 |

## 5. 复现方法

```powershell
# 1) 编译期分析器（期望按 §3 收敛至 0 条）
dotnet build NewLife.MySql\NewLife.MySql.csproj -f net10.0 -c Release -p:IsAotCompatible=true

# 2) 真实 AOT 冒烟（临时探针工程，PublishAot 写在工程文件内）
dotnet build   -c Release --project AotSmoke\AotSmoke.csproj                # JIT 基线
dotnet publish -c Release -r win-x64 -o out --project AotSmoke\AotSmoke.csproj
.\out\AotSmoke.exe                                                          # 逐场景 PASS/FAIL，非 0 退出码即失败
```

冒烟需本地 MySQL（连接串取自 `Bin\UnitTest\Config\mysql.config`），场景自建 `aotsmoke` 库，运行后可清理。

---

（完）
