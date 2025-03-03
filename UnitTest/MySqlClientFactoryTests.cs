using System.Data.Common;
using NewLife.MySql;

namespace UnitTest;

public class MySqlClientFactoryTests
{
    [Fact]
    public void CreateCommand_ShouldReturnMySqlCommand()
    {
        // Arrange
        var factory = MySqlClientFactory.Instance;

        // Act
        var command = factory.CreateCommand();

        // Assert
        Assert.NotNull(command);
        Assert.IsType<MySqlCommand>(command);
    }

    [Fact]
    public void CreateConnection_ShouldReturnMySqlConnection()
    {
        // Arrange
        var factory = MySqlClientFactory.Instance;

        // Act
        var connection = factory.CreateConnection();

        // Assert
        Assert.NotNull(connection);
        Assert.IsType<MySqlConnection>(connection);
    }

    [Fact]
    public void CreateParameter_ShouldReturnMySqlParameter()
    {
        // Arrange
        var factory = MySqlClientFactory.Instance;

        // Act
        var parameter = factory.CreateParameter();

        // Assert
        Assert.NotNull(parameter);
        Assert.IsType<MySqlParameter>(parameter);
    }

    [Fact]
    public void CreateConnectionStringBuilder_ShouldReturnMySqlConnectionStringBuilder()
    {
        // Arrange
        var factory = MySqlClientFactory.Instance;

        // Act
        var connectionStringBuilder = factory.CreateConnectionStringBuilder();

        // Assert
        Assert.NotNull(connectionStringBuilder);
        Assert.IsType<MySqlConnectionStringBuilder>(connectionStringBuilder);
    }

    [Fact]
    public void Test1()
    {
        var factory = DbProviderFactories.GetFactory("NewLife.MySql.MySqlClient");
        Assert.NotNull(factory);
    }
}