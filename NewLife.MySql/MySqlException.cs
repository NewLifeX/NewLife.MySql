namespace NewLife.MySql;

/// <summary>MySql异常</summary>
public class MySqlException : Exception
{
    #region 属性
    /// <summary>错误码</summary>
    public Int32 ErrorCode { get; private set; }
    #endregion

    #region 构造
    /// <summary>实例化</summary>
    public MySqlException() { }

    /// <summary>实例化</summary>
    /// <param name="message"></param>
    public MySqlException(String message) : base(message) { }

    /// <summary>实例化</summary>
    /// <param name="error"></param>
    /// <param name="message"></param>
    public MySqlException(Int32 error, String message) : base(message) => ErrorCode = error;
    #endregion
}