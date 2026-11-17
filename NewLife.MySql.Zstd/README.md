# NewLife.MySql.Zstd

NewLife.MySql 的可选 zstd 压缩包。核心驱动保持零第三方依赖，只有需要 zstd 的应用才安装本包。

## 使用

```csharp
using NewLife.MySql.Zstd;

// 应用启动时调用一次；不调用则 CompressionAlgorithm=zstd 会在建连时明确报错
ZstdCompression.Use();

var connStr = "server=127.0.0.1;database=test;uid=root;pwd=123456;UseCompression=true;CompressionAlgorithm=zstd";
```

`ZstdCompressionLevel` 默认 3，范围 1~22。服务器不支持 zstd 时连接会明确报错，不会静默回退 zlib。

## 兼容性

- zstd 需要服务器端支持（MySQL 8.0.18+，以服务器握手能力位为准）。
- 本包依赖 MIT 协议的 `ZstdSharp.Port`；核心 `NewLife.MySql` 不引用本包。
