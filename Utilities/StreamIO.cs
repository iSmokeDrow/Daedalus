using System;
using System.Text;
using System.IO;
using Daedalus.Enums;

namespace Daedalus.Utilities
{
    public class StreamIO : MemoryStream
    {
        #region fields

        Encoding encoding = Encoding.Default;
        byte[] buffer = new byte[default(int)];
        MemoryStream ms;

        #endregion

        #region constructors

        public StreamIO() { }

        public StreamIO(Encoding encoding)
        {
            this.encoding = encoding;
            ms = new MemoryStream();
        }

        public StreamIO(byte[] buffer) => ms = new MemoryStream(buffer);

        public StreamIO(byte[] buffer, Encoding encoding)
        {
            ms = new MemoryStream(buffer);
            this.encoding = encoding;          
        }

        #endregion

        #region public methods

        public void Clear() => ms.Position = 0;

        #region Read

        public dynamic Read<T>(int length = 0)
        {
            buffer = null;

            if (!is_eos)
            {
                Type type = typeof(T);

                switch (type)
                {
                    case Type _type when type == typeof(byte) && length == 0:
                        return ms.ReadByte();

                    case Type _type when type == typeof(byte[]) && length > 0:
                        buffer = new byte[length];
                        ms.Read(buffer, 0, buffer.Length);
                        return buffer;

                    case Type _type when type == typeof(char):
                         return (char)ms.ReadByte();
                        
                    case Type _type when type == typeof(short):
                        buffer = new byte[sizeof(short)];
                        ms.Read(buffer, 0, sizeof(short));
                        return BitConverter.ToInt16(buffer, 0);
                        
                    case Type _type when type == typeof(ushort):
                        buffer = new byte[sizeof(ushort)];
                        ms.Read(buffer, 0, sizeof(ushort));
                        return BitConverter.ToUInt16(buffer, 0);
                        
                    case Type _type when type == typeof(int):
                        buffer = new byte[sizeof(int)];
                        ms.Read(buffer, 0, sizeof(int));
                        return BitConverter.ToInt32(buffer, 0);
                        
                    case Type _type when type == typeof(uint):
                        buffer = new byte[sizeof(uint)];
                        ms.Read(buffer, 0, sizeof(uint));
                        return BitConverter.ToUInt32(buffer, 0);

                    case Type _type when type == typeof(long):
                        buffer = new byte[sizeof(long)];
                        ms.Read(buffer, 0, sizeof(long));
                        return BitConverter.ToInt64(buffer, 0);

                    case Type _type when type == typeof(ulong):
                        buffer = new byte[sizeof(ulong)];
                        ms.Read(buffer, 0, sizeof(ulong));
                        return BitConverter.ToUInt64(buffer, 0);

                    case Type _type when type == typeof(float):
                        buffer = new byte[sizeof(float)];
                        ms.Read(buffer, 0, sizeof(float));
                        return BitConverter.ToSingle(buffer, 0);

                    case Type _type when type == typeof(double):
                        buffer = new byte[sizeof(double)];
                        ms.Read(buffer, 0, sizeof(double));
                        return BitConverter.ToDouble(buffer, 0);                  

                    case Type _type when type == typeof(string):
                        buffer = new byte[length];
                        ms.Read(buffer, 0, buffer.Length);
                        return ByteConverterExt.ToString(buffer, encoding);

                    default:
                        return null;
                }
            }

            return null;
        }

        #endregion

        #region Write

        public void Write(dynamic value)
        {
            if (value.GetType() == typeof(string))
                Write<string>(value, ((string)value).Length);
            else
                Write(value, 0);
        }

        public int Write<T>(dynamic value, int length = 0) => Write(typeof(T), value, length);

        public int Write(Type type, dynamic value, int length = 0)
        {
            if (value == null)
                throw new Exception("Cannot write 'nothing' (null) to the stream!");

            if (type == typeof(string)) //If the value is a string we need to write it a special way
            {
                if (length <= 0)
                    length = ((string)value).Length;

                byte[] b = ByteConverterExt.ToBytes(value, encoding);
                ms.Write(b, 0, b.Length);

                int remainder = length - b.Length;
                if (remainder > 0)
                {
                    byte[] b2 = new byte[remainder];
                    ms.Write(b2, 0, b2.Length);
                }
            }
            else if (type == typeof(byte[]))
            {
                buffer = value as byte[];
                ms.Write(buffer, 0, buffer.Length);
            }
            else if (type == typeof(byte))
            {
                buffer = new byte[1] { (byte)value };
                ms.Write(buffer, 0, buffer.Length);
            }
            else //If the value is a simple/primitive || TODO: we should go before string
            {
                buffer = BitConverter.GetBytes(value); //Convert value to bytes
                ms.Write(buffer, 0, buffer.Length);
            }

            return -1;
        }

        public void WriteToFile(string path)
        {
            if (File.Exists(path))
                File.Delete(path);

            using (FileStream fs = new FileStream(path, System.IO.FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                ms.WriteTo(fs);
        }

        #endregion

        #endregion

        #region public properties

        public override long Position { get => ms.Position; set => ms.Position = value; }

        public override long Length => ms.Length;

        public override byte[] ToArray() => ms.ToArray();

        public bool EndOfStream => ms.Position >= ms.Length;

        public bool WillEndStream(int length) => ms.Position + length >= ms.Length;

        #endregion

        #region private properties

        byte peek(long offset = 0)
        {
            if (offset > 0)
                ms.Position = offset;

            if (WillEndStream(1))
                throw new Exception("Daedalus.Utilities.StreamIO.peek() cannot read beyond the end of stream!");

            byte retB = Read<byte>();
            ms.Seek(-1, SeekOrigin.Current);

            return retB;
        }

        bool is_eos
        {
            get
            {
                if (EndOfStream)
                    throw new EndOfStreamException();
                else
                    return false;
            }
        }

        #endregion
    }
}
