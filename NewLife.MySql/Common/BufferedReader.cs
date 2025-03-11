using System.Buffers.Binary;
using System.Text;
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

    /// <summary>总容量</summary>
    public Int32 Capacity => _span.Length;

    /// <summary>空闲容量</summary>
    public Int32 FreeCapacity => _span.Length - _index;

    /// <summary>是否小端字节序。默认true</summary>
    public Boolean IsLittleEndian { get; set; } = true;

    private readonly Stream _stream;
    private readonly Int32 _bufferSize;
    private IPacket _data;
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
            var pk = new OwnerPacket(size <= _bufferSize ? _bufferSize : size);
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

    /// <summary>读取数据包</summary>
    /// <param name="length"></param>
    /// <returns></returns>
    public IPacket ReadPacket(Int32 length)
    {
        EnsureSpace(length);

        var result = _data.Slice(_index, length);
        _index += length;
        return result;
    }

    /// <summary>读取Int16整数</summary>
    /// <returns></returns>
    public Int16 ReadInt16()
    {
        var size = sizeof(Int16);
        EnsureSpace(size);
        var result = IsLittleEndian ?
            BinaryPrimitives.ReadInt16LittleEndian(_span.Slice(_index, size)) :
            BinaryPrimitives.ReadInt16BigEndian(_span.Slice(_index, size));
        _index += size;
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

    /// <summary>读取字符串。支持定长、全部和长度前缀</summary>
    /// <param name="length">需要读取的长度。-1表示读取全部，默认0表示读取7位压缩编码整数长度</param>
    /// <param name="encoding">字符串编码，默认UTF8</param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public String ReadString(Int32 length = 0, Encoding? encoding = null)
    {
        if (length < 0)
            length = _span.Length - _index;
        else if (length == 0)
            length = ReadEncodedInt();
        if (length == 0) return String.Empty;

        EnsureSpace(length);

        encoding ??= Encoding.UTF8;

        var result = encoding.GetString(_span.Slice(_index, length));
        _index += length;
        return result;
    }

    /// <summary>以压缩格式读取32位整数</summary>
    /// <returns></returns>
    public Int32 ReadEncodedInt()
    {
        Byte b;
        UInt32 rs = 0;
        Byte n = 0;
        while (true)
        {
            var bt = ReadByte();
            if (bt < 0) throw new Exception($"The data stream is out of range! The integer read is {rs: n0}");
            b = (Byte)bt;

            // 必须转为Int32，否则可能溢出
            rs |= (UInt32)((b & 0x7f) << n);
            if ((b & 0x80) == 0) break;

            n += 7;
            if (n >= 32) throw new FormatException("The number value is too large to read in compressed format!");
        }
        return (Int32)rs;
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

    /// <summary>读取零结尾的C格式字符串</summary>
    public ReadOnlySpan<Byte> ReadZero()
    {
        var span = GetSpan();
        for (var k = 0; k < span.Length; k++)
        {
            if (span[k] == 0)
            {
                Advance(k + 1);
                return span[..(k + 1)];
            }
        }

        Advance(span.Length);
        return span;
    }

    /// <summary>读取零结尾的C格式字符串</summary>
    /// <returns></returns>
    public String ReadZeroString() => ReadZero()[..^1].ToStr();
    #endregion
}
