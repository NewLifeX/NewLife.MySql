using System.Data;
using NewLife.MySql;

namespace UnitTest;

public class MySqlParameterTests
{
    [Fact]
    public void TestParameterName()
    {
        var param = new MySqlParameter
        {
            ParameterName = "TestParam"
        };
        Assert.Equal("TestParam", param.ParameterName);
    }

    [Fact]
    public void TestValue()
    {
        var param = new MySqlParameter
        {
            Value = 123
        };
        Assert.Equal(123, param.Value);
    }

    [Fact]
    public void TestDbType()
    {
        var param = new MySqlParameter
        {
            DbType = DbType.Int32
        };
        Assert.Equal(DbType.Int32, param.DbType);
    }

    [Fact]
    public void TestDirection()
    {
        var param = new MySqlParameter
        {
            Direction = ParameterDirection.Input
        };
        Assert.Equal(ParameterDirection.Input, param.Direction);
    }

    [Fact]
    public void TestIsNullable()
    {
        var param = new MySqlParameter
        {
            IsNullable = true
        };
        Assert.True(param.IsNullable);
    }

    [Fact]
    public void TestSize()
    {
        var param = new MySqlParameter
        {
            Size = 100
        };
        Assert.Equal(100, param.Size);
    }

    [Fact]
    public void TestSourceColumn()
    {
        var param = new MySqlParameter
        {
            SourceColumn = "TestColumn"
        };
        Assert.Equal("TestColumn", param.SourceColumn);
    }

    [Fact]
    public void TestSourceColumnNullMapping()
    {
        var param = new MySqlParameter
        {
            SourceColumnNullMapping = true
        };
        Assert.True(param.SourceColumnNullMapping);
    }

    [Fact]
    public void TestSourceVersion()
    {
        var param = new MySqlParameter
        {
            SourceVersion = DataRowVersion.Original
        };
        Assert.Equal(DataRowVersion.Original, param.SourceVersion);
    }

    [Fact]
    public void TestConstructorWithParameters()
    {
        var param = new MySqlParameter("TestParam", DbType.String);
        Assert.Equal("TestParam", param.ParameterName);
        Assert.Equal(DbType.String, param.DbType);
    }

    [Fact]
    public void TestResetDbType()
    {
        var param = new MySqlParameter();
        Assert.Throws<NotImplementedException>(() => param.ResetDbType());
    }
}
