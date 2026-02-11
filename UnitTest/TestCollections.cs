namespace UnitTest;

/// <summary>测试集合定义。xUnit 会串行执行同一个集合内的测试类，不同集合间可并行执行</summary>
public static class TestCollections
{
    /// <summary>只读操作集合。查询数据库元数据、连接测试等，可以并行执行</summary>
    public const String ReadOnly = "ReadOnly";

    /// <summary>写操作集合。修改数据库结构或数据，需要串行执行以避免冲突</summary>
    public const String WriteOperations = "WriteOperations";

    /// <summary>内存测试集合。不涉及数据库连接，纯内存操作，可以并行执行</summary>
    public const String InMemory = "InMemory";

    /// <summary>Schema 查询集合。查询数据库元数据（表、列、索引等），需要与写操作隔离</summary>
    public const String SchemaQuery = "SchemaQuery";

    /// <summary>数据修改集合。修改 sys.sys_config 等系统表数据，需要串行执行</summary>
    public const String DataModification = "DataModification";

    /// <summary>表结构操作集合。创建/删除测试表，需要串行执行</summary>
    public const String TableOperations = "TableOperations";
}

/// <summary>只读操作集合定义</summary>
[CollectionDefinition(TestCollections.ReadOnly)]
public class ReadOnlyCollection
{
}

/// <summary>写操作集合定义</summary>
[CollectionDefinition(TestCollections.WriteOperations)]
public class WriteOperationsCollection
{
}

/// <summary>内存测试集合定义</summary>
[CollectionDefinition(TestCollections.InMemory)]
public class InMemoryCollection
{
}

/// <summary>Schema 查询集合定义</summary>
[CollectionDefinition(TestCollections.SchemaQuery)]
public class SchemaQueryCollection
{
}

/// <summary>数据修改集合定义</summary>
[CollectionDefinition(TestCollections.DataModification)]
public class DataModificationCollection
{
}

/// <summary>表结构操作集合定义</summary>
[CollectionDefinition(TestCollections.TableOperations)]
public class TableOperationsCollection
{
}
