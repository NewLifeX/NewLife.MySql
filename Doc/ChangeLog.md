# NewLife.MySql 更新日志

纯国产高性能 MySQL 驱动，零第三方依赖，支持管道化批量与真异步。

---

## v1.0 正式版（2026-03-01）

### 新增功能

- 完整 ADO.NET 实现，支持 MySqlConnection/MySqlCommand/MySqlDataReader/MySqlParameter/MySqlTransaction 全套组件
- 支持 MySQL 客户端 SSL/TLS 安全连接
- 支持参数化查询，兼容命名参数与顺序映射，多种 SQL 写法
- 支持事务提交、回滚与自动回滚机制
- 支持多语句与多结果集解析
- 支持 GetSchema 元数据查询，完善 ADO.NET 兼容性
- 支持 MySqlDataAdapter，异步操作及事件
- 增强 MySqlCommand 对存储过程参数的支持
- 支持 .NET 6.0+ DbBatch 批量命令 API
- 支持 MySQL 服务端预编译语句（PrepareStatement）与二进制参数绑定
- 实现 MySQL 批量操作真管道化执行，大幅提升批量性能
- 完善 ChangeDatabase 机制，新增 SetDatabaseAsync API
- 增加客户端心跳机制，避免连接超时断开
- 为 MySql 客户端增加读取超时控制机制
- 支持异步获取连接并增加连接心跳检测
- 支持 MySQL Binlog 订阅与事件解析
- 全面异步化，所有核心操作支持异步方法与 CancellationToken
- 增强异步取消支持
- 新增 EF Core Provider，支持 MySQL 全功能集成
- 新增 EF6 适配层，支持类型映射、迁移等
- 兼容 OceanBase 和 TiDB 数据库自动识别
- 新增 SqlClient 多种 MySQL 协议命令
- 支持 .NET Framework 4.5 / 4.6.1、.NET Standard 2.0 / 2.1、.NET 6.0 / 8.0 / 10.0 全版本

### 优化

- 使用缓冲区优化响应包解析，减少 GC
- 引入 BufferedReader，支持大数据包流式读取，避免单包过大无法直接加载
- 创建 BufferedReader 时限制数据帧大小，避免从网络流读取过多数据
- 改用核心库 SpanReader，提升协议解析效率
- 优化连接管理与认证流程
- 优化 SqlClient 连接与内存管理
- 重构 WelcomeMessage 握手包解析
- 重构 MySqlColumn 字段解析逻辑，提升复用性
- MySQL 类型系统重构，支持 BLOB/TEXT 自动映射
- 重构 MySQL 字段编解码，统一协议解析与类型映射
- 重命名 Response 为 ServerPacket 并同步相关修改
- 优化 ServerPacket 资源释放，防止内存泄漏
- 优化 SqlClient 协议处理与错误解析逻辑
- 客户端打开关闭连接时增加埋点，记录异常信息
- 重置网络数据流，清理上次未处理完的数据；网络断开时自动从池里获取新连接
- 从连接池借出连接时清空网络流未接收数据
- 配置参数优先从连接池获取缓存的变量
- 修复 SpanReader 传递方式并简化参数包构造逻辑
- MySqlCommand 参数顺序映射与 SQL 解析兼容性增强
- 重构连接字符串断言为属性级别校验
- 优化各项目 csproj 描述与标签，提升包可检索性

### 修复

- 修正 ReadZero 导致密码登录失败
- 精确读取长度，避免网络原因导致数据不完整
- 使用 Convert.To*() 替代直接类型转换，修复 MySqlDataReader.GetXxx 方法跨类型转换异常
- 修复 Bit 类型解析问题
- 启用 ReadExactly，确保数据读取完整性

### 测试

- 所有单元测试通过
- 测试用例改为动态临时表，提升隔离与健壮性
- 增强 Authentication 可测试性并补充单元测试
- 优化测试并发隔离，提升覆盖率
- 新增批量操作性能基准测试
- 新增三驱动对比与全操作批量基准测试
- 新增并完善 MySql 核心组件单元测试

### 文档

- 补充完善架构文档和使用手册
- 整理合并文档，介绍性功能放在 Readme.MD，设计性资料放在架构设计.md
- 优化性能测试文档与基准规模，突出管道化优势

### 兼容性

- 零第三方依赖，仅依赖 NewLife.Core
- 支持 .NET Framework 4.5 至 .NET 10 全版本跨平台
