using System.Data.Common;
using System.Data.Entity.Core.Common;
using System.Data.Entity.Core.Common.CommandTrees;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Migrations.Sql;

namespace NewLife.MySql.EntityFramework;

/// <summary>MySql EF6 提供程序服务。实现 DbProviderServices 以支持 EF6 的 DDL/DML 生成</summary>
public class MySqlProviderServices : DbProviderServices
{
    /// <summary>默认实例</summary>
    public static readonly MySqlProviderServices Instance = new();

    /// <summary>获取数据库提供程序清单</summary>
    /// <param name="manifestToken">清单标记</param>
    /// <returns></returns>
    protected override DbProviderManifest GetDbProviderManifest(String manifestToken) => new MySqlProviderManifest(manifestToken);

    /// <summary>获取数据库提供程序清单标记</summary>
    /// <param name="connection">数据库连接</param>
    /// <returns></returns>
    protected override String GetDbProviderManifestToken(DbConnection connection) => connection.ServerVersion ?? "8.0";

    /// <summary>创建数据库命令定义</summary>
    /// <param name="providerManifest">提供程序清单</param>
    /// <param name="commandTree">命令树</param>
    /// <returns></returns>
    protected override DbCommandDefinition CreateDbCommandDefinition(DbProviderManifest providerManifest, DbCommandTree commandTree)
    {
        if (providerManifest == null) throw new ArgumentNullException(nameof(providerManifest));
        if (commandTree == null) throw new ArgumentNullException(nameof(commandTree));

        var cmd = CreateCommand(providerManifest, commandTree);
        return CreateCommandDefinition(cmd);
    }

    private DbCommand CreateCommand(DbProviderManifest manifest, DbCommandTree commandTree)
    {
        var cmd = new MySqlCommand();
        var sqlGenerator = new MySqlSqlGenerator();
        var sql = sqlGenerator.GenerateSql(commandTree);

        cmd.CommandText = sql.CommandText;

        foreach (var param in sql.Parameters)
        {
            var p = new MySqlParameter
            {
                ParameterName = param.Key,
                Value = param.Value ?? DBNull.Value
            };
            cmd.Parameters.Add(p);
        }

        return cmd;
    }
}
