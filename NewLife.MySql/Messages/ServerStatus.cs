using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NewLife.MySql.Messages;

/// <summary>服务器状态</summary>
[Flags]
public enum ServerStatus : UInt16
{
    /// <summary>事务中</summary>
    InTransaction = 1,

    /// <summary>自动提交模式</summary>
    AutoCommitMode = 2,

    /// <summary>服务器更多结果</summary>
    MoreResults = 4,
    AnotherQuery = 8, // Multi query - next query exists
    BadIndex = 16,
    NoIndex = 32,
    CursorExists = 64,
    LastRowSent = 128,
    DbDropped = 256,
    NoBackslashEscapes = 512,
    MetadataChanged = 1024,
    WasSlow = 2048,
    OutputParameters = 4096,
    InTransactionReadOnly = 8192, // In a read-only transaction
    SessionStateChanged = 16384 // Connection state information has changed
}
