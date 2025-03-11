using System.Buffers.Binary;
using NewLife.Data;

namespace NewLife.MySql.Common;

/// <summary>缓存的读取器</summary>
public ref struct BufferedReader
{
    #region 属性
    private ReadOnlySpan<Byte> _span;
    /// <summary>数据片段</summary>
    public ReadOnlySpan<Byte> Span => _span;

    private Int32 _index;
    /// <summary>已读取字节数</summary>
    public Int32 Position { get => _index; set => _index = value; }

    /// <summary>最大容量</summary>
    public Int32 MaxCapacity { get; set; }

    /// <summary>空闲容量</summary>
    public Int32 FreeCapacity => _span.Length - _index;

    /// <summary>是否小端字节序。默认true</summary>
    public Boolean IsLittleEndian { get; set; } = true;

    private readonly Stream _stream;
    private readonly Int32 _bufferSize;
    private IPacket _data;
    private Int32 _total;
    #endregion

    #region 构造
    /// <summary>实例化Span读取器</summary>
    /// <param name="stream"></param>
    /// <param name="data"></param>
    /// <param name="bufferSize"></param>
    public BufferedReader(Stream stream, IPacket data, Int32 bufferSize = 8192)
    {
        _stream = stream;
        _bufferSize = bufferSize;
        _data = data;
        _span = data.GetSpan();
        _total = data.Total;
    }
    #endregion

    #region 基础方法
    /// <summary>告知有多少数据已从缓冲区读取</summary>
    /// <param name="count"></param>
    public void Advance(Int32 count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (_index + count > _span.Length) throw new ArgumentOutOfRangeException(nameof(count));

        _index += count;
    }

    /// <summary>返回要写入到的Span，其大小按 sizeHint 参数指定至少为所请求的大小</summary>
    /// <param name="sizeHint"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public ReadOnlySpan<Byte> GetSpan(Int32 sizeHint = 0)
    {
        if (sizeHint > FreeCapacity) throw new ArgumentOutOfRangeException(nameof(sizeHint));

        return _span[_index..];
    }
    #endregion

    #region 读取方法
    /// <summary>确保缓冲区中有足够的空间。</summary>
    /// <param name="size">需要的字节数。</param>
    /// <exception cref="InvalidOperationException"></exception>
    public void EnsureSpace(Int32 size)
    {
        // 检查剩余空间大小，不足时，再从数据流中读取。此时需要注意，创建新的OwnerPacket后，需要先把之前剩余的一点数据拷贝过去，然后再读取Stream
        var remain = FreeCapacity;
        if (remain < size)
        {
            // 申请指定大小的数据包缓冲区，实际大小可能更大
            var idx = 0;
            var bsize = size;
            if (bsize < _bufferSize) bsize = _bufferSize;
            if (bsize > MaxCapacity - _total) bsize = MaxCapacity - _total;
            var pk = new OwnerPacket(size <= bsize ? bsize : size);
            if (_data != null && remain > 0)
            {
                if (!_data.TryGetArray(out var arr)) throw new NotSupportedException();

                arr.AsSpan(_index, remain).CopyTo(pk.Buffer);
                idx += remain;
            }

            _data.TryDispose();
            _data = pk;
            _index = 0;

            // 多次读取，直到满足需求
            //var n = _stream.ReadExactly(pk.Buffer, pk.Offset + idx, pk.Length - idx);
            while (idx < size)
            {
                // 实际缓冲区大小可能大于申请大小，充分利用缓冲区，避免多次读取
                var len = pk.Buffer.Length - pk.Offset;
                var n = _stream.Read(pk.Buffer, pk.Offset + idx, len - idx);
                if (n <= 0) break;

                idx += n;
            }
            if (idx < size)
                throw new InvalidOperationException("Not enough data to read.");
            pk.Resize(idx);

            _span = pk.GetSpan();
            _total += idx - remain;
        }

        if (_index + size > _span.Length)
            throw new InvalidOperationException("Not enough data to read.");
    }

    /// <summary>读取单个字节</summary>
    /// <returns></returns>
    public Byte ReadByte()
    {
        var size = sizeof(Byte);
        EnsureSpace(size);

        var result = _span[_index];
        _index += size;
        return result;
    }

    /// <summary>读取字节数组</summary>
    /// <param name="length"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public ReadOnlySpan<Byte> ReadBytes(Int32 length)
    {
        EnsureSpace(length);

        var result = _span.Slice(_index, length);
        _index += length;
        return result;
    }

    /// <summary>读取UInt16整数</summary>
    /// <returns></returns>
    public UInt16 ReadUInt16()
    {
        var size = sizeof(UInt16);
        EnsureSpace(size);
        var result = IsLittleEndian ?
            BinaryPrimitives.ReadUInt16LittleEndian(_span.Slice(_index, size)) :
            BinaryPrimitives.ReadUInt16BigEndian(_span.Slice(_index, size));
        _index += size;
        return result;
    }

    /// <summary>读取Int32整数</summary>
    /// <returns></returns>
    public Int32 ReadInt32()
    {
        var size = sizeof(Int32);
        EnsureSpace(size);
        var result = IsLittleEndian ?
            BinaryPrimitives.ReadInt32LittleEndian(_span.Slice(_index, size)) :
            BinaryPrimitives.ReadInt32BigEndian(_span.Slice(_index, size));
        _index += size;
        return result;
    }
    #endregion

    #region 业务读取
    /// <summary>读取集合元素个数</summary>
    /// <returns></returns>
    public Int32 ReadLength()
    {
        var c = ReadByte();

        return c switch
        {
            251 => -1,
            252 => ReadUInt16(),
            253 => ReadByte() << 16 | ReadByte() << 8 | ReadByte(),
            254 => ReadInt32(),
            _ => c,
        };
    }
    #endregion
}
