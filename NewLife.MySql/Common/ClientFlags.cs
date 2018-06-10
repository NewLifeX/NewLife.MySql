using System;

namespace NewLife.MySql.Common
{
    [Flags]
    internal enum ClientFlags : UInt64
    {
        LONG_PASSWORD = 1, // new more secure passwords
        FOUND_ROWS = 2, // found instead of affected rows
        LONG_FLAG = 4, // Get all column flags
        CONNECT_WITH_DB = 8, // One can specify db on connect
        NO_SCHEMA = 16, // Don't allow db.table.column
        COMPRESS = 32, // Client can use compression protocol
        ODBC = 64, // ODBC client
        LOCAL_FILES = 128, // Can use LOAD DATA LOCAL
        IGNORE_SPACE = 256, // Ignore spaces before '('
        PROTOCOL_41 = 512, // Support new 4.1 protocol
        INTERACTIVE = 1024, // This is an interactive client
        SSL = 2048, // Switch to SSL after handshake
        IGNORE_SIGPIPE = 4096, // IGNORE sigpipes
        TRANSACTIONS = 8192, // Client knows about transactions
        RESERVED = 16384,               // old 4.1 protocol flag
        SECURE_CONNECTION = 32768,      // new 4.1 authentication
        MULTI_STATEMENTS = 65536,       // Allow multi-stmt support
        MULTI_RESULTS = 131072,         // Allow multiple resultsets
        PS_MULTI_RESULTS = 1UL << 18,    // allow multi results using PS protocol
        PLUGIN_AUTH = (1UL << 19), //Client supports plugin authentication
        CONNECT_ATTRS = (1UL << 20),    // Allows client connection attributes
        CAN_HANDLE_EXPIRED_PASSWORD = (1UL << 22),   // Support for password expiration > 5.6.6
        CLIENT_SSL_VERIFY_SERVER_CERT = (1UL << 30),
        CLIENT_REMEMBER_OPTIONS = (1UL << 31)
    }
}