#if NETFRAMEWORK
namespace System.Runtime.CompilerServices;

/// <summary>模块初始化器标记。net45/net461 缺少该特性，供 NewLife.Core 词表源生成器注入注册代码</summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false)]
internal sealed class ModuleInitializerAttribute : Attribute { }
#endif
