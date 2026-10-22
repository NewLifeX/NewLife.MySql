# tools

本目录存放仓库级脚本。根目录只保留 README、`readme-*.json` 等配置与产物，不再放脚本。

| 脚本 | 用途 |
|------|------|
| `check-locales.ps1` | 词表键集合、占位符、格式说明符、孤儿键、源码消息反查；`-RequireTranslated` 检查未翻译项 |
| `report-locales.ps1` | 按语言输出键数、缺失、未翻译、已翻译统计 |
| `sync-locale-keys.ps1` | 以基准表为源，给所有语言追加缺失键的英文占位 |
| `check-english-exceptions.ps1` | 扫描异常/日志/控制台/L.T 的 CJK 源文 |
| `check-assembly-metadata.ps1` | 校验 `AssemblyTitle`/`Description` 为英文且进入基准词表 |
| `check-config-metadata.ps1` | 校验配置类 `DisplayName`/`Description`/`Category` 为英文 |
| `check-readme-coverage.ps1` | 校验各语言 README 的标题、代码块、表格与占位标记 |
| `sync-readme-languages.ps1` | 按 `readme-languages.json` 重写 README 语言切换行 |
| `sync-readme-package.ps1` | 从 `README.md` 生成 `README.nuget.md` |
| `check-package-consumer.ps1` | 打包后在临时工程验证词表与元数据译文 |
| `update-newlife-packages.ps1` | 查询 NuGet 最新 beta，可选写回 csproj |
| `verify-localization.ps1` | 串联构建、测试、词表、README、包消费验证 |

约定：

- 脚本默认以仓库根为工作目录（`$Root = Split-Path -Parent $PSScriptRoot`）。
- 词表目录名必须是 `Locales`，基准文件为 `zh-Hans.yml`，英文是源文与兜底。
- 新增/改名/删除脚本时，同步更新 `.github/workflows/` 的引用与 `paths` 过滤，以及 `Doc/多语言本地化.md`。
