using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace NewLife.MySql;

/// <summary>行更新前事件参数</summary>
public sealed class MySqlRowUpdatingEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) : RowUpdatingEventArgs(row, command, statementType, tableMapping)
{
    /// <summary>获取或设置要执行的 MySqlCommand</summary>
    public new MySqlCommand Command { get => (MySqlCommand)base.Command; set => base.Command = value; }
}

/// <summary>行更新后事件参数</summary>
public sealed class MySqlRowUpdatedEventArgs(DataRow row, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) : RowUpdatedEventArgs(row, command, statementType, tableMapping)
{
    /// <summary>获取执行的 MySqlCommand</summary>
    public new MySqlCommand Command => (MySqlCommand)base.Command;
}

/// <summary>行更新前事件处理程序</summary>
/// <param name="sender">事件源</param>
/// <param name="e">事件参数</param>
public delegate void MySqlRowUpdatingEventHandler(Object sender, MySqlRowUpdatingEventArgs e);

/// <summary>行更新后事件处理程序</summary>
/// <param name="sender">事件源</param>
/// <param name="e">事件参数</param>
public delegate void MySqlRowUpdatedEventHandler(Object sender, MySqlRowUpdatedEventArgs e);

/// <summary>MySql 数据适配器，用于在 DataSet 和数据库之间传输数据</summary>
[DesignerCategory("Code")]
public sealed class MySqlDataAdapter : DbDataAdapter, IDbDataAdapter, IDataAdapter
{
    #region 属性
    private Boolean _loadingDefaults;

    /// <summary>是否加载默认值</summary>
    internal Boolean LoadDefaults { get => _loadingDefaults; set => _loadingDefaults = value; }

    /// <summary>行更新前事件</summary>
    public event MySqlRowUpdatingEventHandler? RowUpdating;

    /// <summary>行更新后事件</summary>
    public event MySqlRowUpdatedEventHandler? RowUpdated;
    #endregion

    #region 构造
    /// <summary>初始化 MySqlDataAdapter 的新实例</summary>
    public MySqlDataAdapter() => _loadingDefaults = true;

    /// <summary>使用指定的查询命令初始化 MySqlDataAdapter 的新实例</summary>
    /// <param name="selectCommand">SELECT 命令</param>
    public MySqlDataAdapter(MySqlCommand selectCommand) : this() => SelectCommand = selectCommand;

    /// <summary>使用查询文本和连接初始化 MySqlDataAdapter 的新实例</summary>
    /// <param name="selectCommandText">SELECT 命令文本</param>
    /// <param name="connection">数据库连接</param>
    public MySqlDataAdapter(String selectCommandText, MySqlConnection connection) : this() => SelectCommand = new MySqlCommand(selectCommandText, connection);

    /// <summary>使用查询文本和连接字符串初始化 MySqlDataAdapter 的新实例</summary>
    /// <param name="selectCommandText">SELECT 命令文本</param>
    /// <param name="selectConnString">数据库连接字符串</param>
    public MySqlDataAdapter(String selectCommandText, String selectConnString) : this() => SelectCommand = new MySqlCommand(selectCommandText, new MySqlConnection(selectConnString));
    #endregion

    #region 方法
    /// <summary>更新 DataRow 数组中的数据到数据库</summary>
    /// <param name="dataRows">要更新的数据行数组</param>
    /// <param name="tableMapping">表映射</param>
    /// <returns>成功更新的行数</returns>
    protected override Int32 Update(DataRow[] dataRows, DataTableMapping tableMapping)
    {
        List<MySqlConnection> list = [];
        try
        {
            foreach (var dataRow in dataRows)
            {
                OpenConnectionIfClosed(dataRow.RowState, list);
            }
            return base.Update(dataRows, tableMapping);
        }
        finally
        {
            foreach (var item in list)
            {
                item.Close();
            }
        }
    }

    /// <summary>创建行更新后事件参数</summary>
    /// <param name="dataRow">数据行</param>
    /// <param name="command">命令</param>
    /// <param name="statementType">语句类型</param>
    /// <param name="tableMapping">表映射</param>
    /// <returns>行更新后事件参数</returns>
    protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) => new MySqlRowUpdatedEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>创建行更新前事件参数</summary>
    /// <param name="dataRow">数据行</param>
    /// <param name="command">命令</param>
    /// <param name="statementType">语句类型</param>
    /// <param name="tableMapping">表映射</param>
    /// <returns>行更新前事件参数</returns>
    protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping) => new MySqlRowUpdatingEventArgs(dataRow, command, statementType, tableMapping);

    /// <summary>触发行更新前事件</summary>
    /// <param name="value">事件参数</param>
    protected override void OnRowUpdating(RowUpdatingEventArgs value) => RowUpdating?.Invoke(this, value as MySqlRowUpdatingEventArgs);

    /// <summary>触发行更新后事件</summary>
    /// <param name="value">事件参数</param>
    protected override void OnRowUpdated(RowUpdatedEventArgs value) => RowUpdated?.Invoke(this, value as MySqlRowUpdatedEventArgs);

    /// <summary>异步填充 DataSet</summary>
    /// <param name="dataSet">要填充的 DataSet</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataSet dataSet, CancellationToken cancellationToken = default) => ExecuteAsync(() => Fill(dataSet), cancellationToken);

    /// <summary>异步填充 DataTable</summary>
    /// <param name="dataTable">要填充的 DataTable</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataTable dataTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => Fill(dataTable), cancellationToken);

    /// <summary>异步填充 DataSet 的指定表</summary>
    /// <param name="dataSet">要填充的 DataSet</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataSet dataSet, String srcTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => Fill(dataSet, srcTable), cancellationToken);

    /// <summary>使用 IDataReader 异步填充 DataTable</summary>
    /// <param name="dataTable">要填充的 DataTable</param>
    /// <param name="dataReader">数据读取器</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataTable dataTable, IDataReader dataReader, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Fill(dataTable, dataReader), cancellationToken);

    /// <summary>使用命令异步填充 DataTable</summary>
    /// <param name="dataTable">要填充的 DataTable</param>
    /// <param name="command">数据库命令</param>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataTable dataTable, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Fill(dataTable, command, behavior), cancellationToken);

    /// <summary>异步填充多个 DataTable</summary>
    /// <param name="startRecord">起始记录</param>
    /// <param name="maxRecords">最大记录数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <param name="dataTables">要填充的 DataTable 数组</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(Int32 startRecord, Int32 maxRecords, CancellationToken cancellationToken = default, params DataTable[] dataTables) => ExecuteAsync(() => Fill(startRecord, maxRecords, dataTables), cancellationToken);

    /// <summary>异步填充 DataSet 的指定范围</summary>
    /// <param name="dataSet">要填充的 DataSet</param>
    /// <param name="startRecord">起始记录</param>
    /// <param name="maxRecords">最大记录数</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataSet dataSet, Int32 startRecord, Int32 maxRecords, String srcTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => Fill(dataSet, startRecord, maxRecords, srcTable), cancellationToken);

    /// <summary>使用 IDataReader 异步填充 DataSet 的指定范围</summary>
    /// <param name="dataSet">要填充的 DataSet</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="dataReader">数据读取器</param>
    /// <param name="startRecord">起始记录</param>
    /// <param name="maxRecords">最大记录数</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataSet dataSet, String srcTable, IDataReader dataReader, Int32 startRecord, Int32 maxRecords, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Fill(dataSet, srcTable, dataReader, startRecord, maxRecords), cancellationToken);

    /// <summary>使用命令异步填充多个 DataTable 的指定范围</summary>
    /// <param name="dataTables">要填充的 DataTable 数组</param>
    /// <param name="startRecord">起始记录</param>
    /// <param name="maxRecords">最大记录数</param>
    /// <param name="command">数据库命令</param>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataTable[] dataTables, Int32 startRecord, Int32 maxRecords, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Fill(dataTables, startRecord, maxRecords, command, behavior), cancellationToken);

    /// <summary>使用命令异步填充 DataSet 的指定范围</summary>
    /// <param name="dataSet">要填充的 DataSet</param>
    /// <param name="startRecord">起始记录</param>
    /// <param name="maxRecords">最大记录数</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="command">数据库命令</param>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功添加或刷新的行数</returns>
    public Task<Int32> FillAsync(DataSet dataSet, Int32 startRecord, Int32 maxRecords, String srcTable, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Fill(dataSet, startRecord, maxRecords, srcTable, command, behavior), cancellationToken);

    /// <summary>异步获取 DataSet 的架构信息</summary>
    /// <param name="dataSet">要填充架构的 DataSet</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable 数组</returns>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, CancellationToken cancellationToken = default) => ExecuteAsync(() => FillSchema(dataSet, schemaType), cancellationToken);

    /// <summary>异步获取 DataSet 指定表的架构信息</summary>
    /// <param name="dataSet">要填充架构的 DataSet</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable 数组</returns>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, String srcTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => FillSchema(dataSet, schemaType, srcTable), cancellationToken);

    /// <summary>使用 IDataReader 异步获取 DataSet 的架构信息</summary>
    /// <param name="dataSet">要填充架构的 DataSet</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="dataReader">数据读取器</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable 数组</returns>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, String srcTable, IDataReader dataReader, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.FillSchema(dataSet, schemaType, srcTable, dataReader), cancellationToken);

    /// <summary>使用命令异步获取 DataSet 的架构信息</summary>
    /// <param name="dataSet">要填充架构的 DataSet</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="command">数据库命令</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable 数组</returns>
    public Task<DataTable[]> FillSchemaAsync(DataSet dataSet, SchemaType schemaType, IDbCommand command, String srcTable, CommandBehavior behavior, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.FillSchema(dataSet, schemaType, command, srcTable, behavior), cancellationToken);

    /// <summary>异步获取 DataTable 的架构信息</summary>
    /// <param name="dataTable">要填充架构的 DataTable</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable</returns>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, CancellationToken cancellationToken = default) => ExecuteAsync(() => FillSchema(dataTable, schemaType), cancellationToken);

    /// <summary>使用 IDataReader 异步获取 DataTable 的架构信息</summary>
    /// <param name="dataTable">要填充架构的 DataTable</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="dataReader">数据读取器</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable</returns>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDataReader dataReader, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.FillSchema(dataTable, schemaType, dataReader), cancellationToken);

    /// <summary>使用命令异步获取 DataTable 的架构信息</summary>
    /// <param name="dataTable">要填充架构的 DataTable</param>
    /// <param name="schemaType">架构类型</param>
    /// <param name="command">数据库命令</param>
    /// <param name="behavior">命令行为</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>包含架构信息的 DataTable</returns>
    public Task<DataTable> FillSchemaAsync(DataTable dataTable, SchemaType schemaType, IDbCommand command, CommandBehavior behavior, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.FillSchema(dataTable, schemaType, command, behavior), cancellationToken);

    /// <summary>异步更新 DataRow 数组</summary>
    /// <param name="dataRows">要更新的数据行数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功更新的行数</returns>
    public Task<Int32> UpdateAsync(DataRow[] dataRows, CancellationToken cancellationToken = default) => ExecuteAsync(() => Update(dataRows), cancellationToken);

    /// <summary>异步更新 DataSet</summary>
    /// <param name="dataSet">要更新的 DataSet</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功更新的行数</returns>
    public Task<Int32> UpdateAsync(DataSet dataSet, CancellationToken cancellationToken = default) => ExecuteAsync(() => Update(dataSet), cancellationToken);

    /// <summary>异步更新 DataTable</summary>
    /// <param name="dataTable">要更新的 DataTable</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功更新的行数</returns>
    public Task<Int32> UpdateAsync(DataTable dataTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => Update(dataTable), cancellationToken);

    /// <summary>使用表映射异步更新 DataRow 数组</summary>
    /// <param name="dataRows">要更新的数据行数组</param>
    /// <param name="tableMapping">表映射</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功更新的行数</returns>
    public Task<Int32> UpdateAsync(DataRow[] dataRows, DataTableMapping tableMapping, CancellationToken cancellationToken = default) => ExecuteAsync(() => base.Update(dataRows, tableMapping), cancellationToken);

    /// <summary>异步更新 DataSet 的指定表</summary>
    /// <param name="dataSet">要更新的 DataSet</param>
    /// <param name="srcTable">源表名称</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>成功更新的行数</returns>
    public Task<Int32> UpdateAsync(DataSet dataSet, String srcTable, CancellationToken cancellationToken = default) => ExecuteAsync(() => Update(dataSet, srcTable), cancellationToken);
    #endregion

    #region 辅助
    /// <summary>如果连接已关闭则打开连接</summary>
    /// <param name="state">数据行状态</param>
    /// <param name="openedConnections">已打开的连接列表</param>
    private void OpenConnectionIfClosed(DataRowState state, List<MySqlConnection> openedConnections)
    {
        var command = state switch
        {
            DataRowState.Added => InsertCommand,
            DataRowState.Deleted => DeleteCommand,
            DataRowState.Modified => UpdateCommand,
            _ => null,
        };
        if (command?.Connection is MySqlConnection conn && conn.State == ConnectionState.Closed)
        {
            conn.Open();
            openedConnections.Add(conn);
        }
    }

    /// <summary>执行异步操作的通用辅助方法</summary>
    /// <typeparam name="T">返回值类型</typeparam>
    /// <param name="func">要执行的同步方法</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>异步任务</returns>
    private static Task<T> ExecuteAsync<T>(Func<T> func, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<T>();
        if (cancellationToken.IsCancellationRequested)
        {
            tcs.SetCanceled();
            return tcs.Task;
        }

        try
        {
            var result = func();
            tcs.SetResult(result);
        }
        catch (Exception ex)
        {
            tcs.SetException(ex);
        }
        return tcs.Task;
    }
    #endregion
}
