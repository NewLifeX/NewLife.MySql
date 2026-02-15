using Microsoft.EntityFrameworkCore.Update;

namespace NewLife.MySql.EntityFrameworkCore;

/// <summary>MySql 修改命令批量工厂。为 MySQL 创建 SingularModificationCommandBatch（逐条执行）</summary>
public class MySqlModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    /// <summary>实例化</summary>
    /// <param name="dependencies">工厂依赖</param>
    public MySqlModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    /// <summary>创建修改命令批。使用 SingularModificationCommandBatch（每批一条命令）</summary>
    /// <returns></returns>
    public ModificationCommandBatch Create() => new SingularModificationCommandBatch(_dependencies);
}
