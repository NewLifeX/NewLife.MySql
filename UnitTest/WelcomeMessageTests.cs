using System;
using NewLife;
using NewLife.MySql.Messages;

namespace UnitTest;

public class WelcomeMessageTests
{
    [Fact]
    public void Read_ShouldParseCorrectly()
    {
        // Arrange
        var data = new Byte[]
        {
            10, // Protocol
            53, 46, 55, 46, 50, 0, // Version "5.7.2"
            1, 0, 0, 0, // ConnectionId
            97, 98, 99, 0, // Seed1 "abc"
            255, 255, // Capability part 1
            8, // CharacterSet
            2, 0, // StatusFlags
            255, 255, // Capability part 2
            21, // Auth plugin data length
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // Reserved
            100, 101, 102, 0, // Seed2 "def"
            109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0 // AuthMethod "mysql_native_password"
        };
        var span = new ReadOnlySpan<Byte>(data);
        var message = new WelcomeMessage();

        // Act
        message.Read(span);

        // Assert
        Assert.Equal(10, message.Protocol);
        Assert.Equal("5.7.2", message.Version);
        Assert.Equal(1u, message.ConnectionId);
        Assert.Equal([97, 98, 99, 0, 100, 101, 102, 0], message.Seed);
        Assert.Equal((UInt32)0xFFFF, message.Capability & 0xFFFF);
        Assert.Equal(0xFFFF0000, message.Capability & 0xFFFF0000);
        Assert.Equal((Byte)8, message.CharacterSet);
        Assert.Equal((UInt16)2, message.StatusFlags);
        Assert.Equal("mysql_native_password", message.AuthMethod);
    }

    [Fact]
    public void Read()
    {
        var str = "0A382E302E3339001C000000024439523E53074100FFFFFF0200FFDF15000000000000000000007F14467E1B0B131D1C5C5C250063616368696E675F736861325F70617373776F726400";
        var buf = str.ToHex();

        var message = new WelcomeMessage();

        // Act
        message.Read(buf);

        // Assert
        Assert.Equal(10, message.Protocol);
        Assert.Equal("8.0.39", message.Version);
        Assert.Equal(28u, message.ConnectionId);
        Assert.Equal([2, 68, 57, 82, 62, 83, 7, 65, 0], message.Seed);
        Assert.Equal((UInt32)0xFFFF, message.Capability & 0xFFFF);
        Assert.Equal(0xFFFF0000, message.Capability & 0xFFFF0000);
        Assert.Equal((Byte)8, message.CharacterSet);
        Assert.Equal((UInt16)2, message.StatusFlags);
        Assert.Equal("mysql_native_password", message.AuthMethod);
    }
}
