using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace NewLife.MySql;

/// <summary>异步读前置缓冲区。合并多次小读取为一次大的底层读取，减少系统调用次数</summary>
/// <remarks>
/// 仅缓冲读取方向，不持有流引用，由调用方传入当前流。
/// 适用于 TCP 全双工场景：管道化模式下读写并发执行在同一 Stream 上，
/// BufferedStream 会同时缓冲双向数据导致竞态，而本类仅管理读缓冲。
///
/// 默认 16KB 缓冲区可将行读取路径（每行 4B 帧头 + NB 帧体）的系统调用次数
/// 从每行 2 次降低到约每 160 行 1 次（典型行约 100 字节时）。
/// </remarks>
class ReadBuffer
{
    private readonly Int32 _bufferSize;
    private Byte[]? _buffer;
    private Int32 _pos;
    private Int32 _len;

    /// <summary>实例化读前置缓冲区</summary>
    /// <param name="bufferSize">缓冲区大小，默认16KB</param>
    public ReadBuffer(Int32 bufferSize = 16384) => _bufferSize = bufferSize;

    /// <summary>从流中精确读取指定字节数到目标缓冲区</summary>
    /// <param name="stream">底层数据流</param>
    /// <param name="buffer">目标缓冲区</param>
    /// <param name="offset">目标偏移量</param>
    /// <param name="count">需要读取的字节数</param>
    /// <param name="token">取消令牌</param>
    /// <returns>异步任务</returns>
    public async Task ReadAsync(Stream stream, Byte[] buffer, Int32 offset, Int32 count, CancellationToken token)
    {
        while (count > 0)
        {
            // 先从缓冲区中满足请求
            var buffered = _len - _pos;
            if (buffered > 0)
            {
                var toCopy = Math.Min(buffered, count);
                Buffer.BlockCopy(_buffer!, _pos, buffer, offset, toCopy);
                _pos += toCopy;
                offset += toCopy;
                count -= toCopy;
                if (count == 0) return;
            }

            // 大块读取直接走底层流，跳过缓冲
            if (count >= _bufferSize)
            {
                await stream.ReadExactlyAsync(buffer, offset, count, token).ConfigureAwait(false);
                return;
            }

            // 补充缓冲区
            _buffer ??= new Byte[_bufferSize];

            var n = await stream.ReadAsync(_buffer, 0, _buffer.Length, token).ConfigureAwait(false);
            if (n == 0) throw new EndOfStreamException("读取MySQL数据包时连接意外关闭");
            _pos = 0;
            _len = n;
        }
    }

    /// <summary>清空缓冲区中的残留数据</summary>
    public void Reset()
    {
        _pos = 0;
        _len = 0;
    }
}