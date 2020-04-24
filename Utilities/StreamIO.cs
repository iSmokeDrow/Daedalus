using System;
using System.Text;
using System.IO;

namespace Daedalus.Utilities
{
    public class StreamIO
    {
        Encoding encoding = Encoding.Default;
        MemoryStream ms;
        byte[] buffer = new byte[default(int)];

        public StreamIO() { ms = new MemoryStream(); }

        public StreamIO(Encoding encoding)
        {
            this.encoding = encoding;
            ms = new MemoryStream();
        }

        public StreamIO(MemoryStream ms) { this.ms = ms; }

        public StreamIO(byte[] buffer) { ms = new MemoryStream(buffer, 0, buffer.Length, true); }

        public StreamIO(byte[] buffer, Encoding encoding)
        {
            this.encoding = encoding;
            ms = new MemoryStream(buffer, 0, buffer.Length, true);
        }

        public StreamIO(string path, FileMode fileMode, FileAccess fileAccess)
        {
            if (File.Exists(path))
                ms = new MemoryStream(FileIO.ReadAllBytes(path), true);
            else
                throw new FileNotFoundException("Daedalus.Utilities.StreamIO cannot initialize because the file cannot be found at path", path);
        }

        public StreamIO(string path, FileMode fileMode, FileAccess fileAccess, Encoding encoding)
        {
            if (File.Exists(path))
                ms = new MemoryStream(FileIO.ReadAllBytes(path), true);
            else
                throw new FileNotFoundException("Daedalus.Utilities.StreamIO cannot initialize because the file cannot be found at path", path);
        }

        public void Clear()
        {
            if (ms.Length > 0)
            {
                int len = (int)ms.Length;
                ms = new MemoryStream(len);
            }
        }

        #region Read

        public int ReadByte
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadInt16() cannot read beyond end of stream!");

                return ms.ReadByte();
            }
        }

        public byte[] ReadBytes(int count)
        {
            buffer = new byte[count];

            if (ms.Position == ms.Length)
                throw new Exception("Daedalus.Utilities.StreamIO.ReadBytes(int count) cannot read beyond end of stream!");

            ms.Read(buffer, 0, buffer.Length);

            return buffer;
        }

        public short ReadInt16
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadInt16() cannot read beyond end of stream!");

                if (buffer.Length != 2)
                    buffer = new byte[2];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToInt16(buffer, 0);
            }
        }

        public ushort ReadUInt16
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadUInt16() cannot read beyond end of stream!");

                if (buffer.Length != 2)
                    buffer = new byte[2];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToUInt16(buffer, 0);
            }
        }

        public int ReadInt32
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadInt32() cannot read beyond end of stream!");

                if (buffer.Length != 4)
                    buffer = new byte[4];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToInt32(buffer, 0);
            }
        }

        public uint ReadUInt32
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadUInt32() cannot read beyond end of stream!");

                if (buffer.Length != 4)
                    buffer = new byte[4];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToUInt32(buffer, 0);
            }
        }

        public long ReadInt64
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadInt64() cannot read beyond end of stream!");

                if (buffer.Length != 8)
                    buffer = new byte[8];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToInt64(buffer, 0);
            }
        }

        public ulong ReadUInt64
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadUInt64() cannot read beyond end of stream!");

                if (buffer.Length != 8)
                    buffer = new byte[8];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToUInt64(buffer, 0);
            }
        }

        public float ReadSingle
        {
            get { return ReadFloat32; }
        }

        public float ReadFloat
        {
            get { return ReadFloat32; }
        }

        public float ReadFloat32
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadFloat() cannot read beyond end of stream!");

                if (buffer.Length != 4)
                    buffer = new byte[4];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToSingle(buffer, 0);
            }
        }

        public double ReadDouble
        {
            get { return ReadFloat64; }
        }

        public double ReadFloat64
        {
            get
            {
                if (ms.Position == ms.Length)
                    throw new Exception("Daedalus.Utilities.StreamIO.ReadFloat64() cannot read beyond end of stream!");

                if (buffer.Length != 8)
                    buffer = new byte[8];

                ms.Read(buffer, 0, buffer.Length);

                return BitConverter.ToDouble(buffer, 0);
            }
        }

        public string ReadString(int length)
        {
            if (ms.Position == ms.Length)
                throw new Exception("Daedalus.Utilities.StreamIO.ReadString(int length) cannot read beyond end of stream!");

            buffer = new byte[length];
            ms.Read(buffer, 0, buffer.Length);

            return ByteConverterExt.ToString(buffer, encoding);
        }

        #endregion

        #region Write

        public void WriteToFile(string path)
        {
            if (File.Exists(path))
                File.Delete(path);

            using (FileStream fs = new FileStream(path, System.IO.FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                ms.WriteTo(fs);
        }

        public void WriteByte(byte b) { ms.Write(new byte[] { b }, 0, 1); }

        public void WriteBytes(byte[] b) { ms.Write(b, 0, b.Length); }

        public void WriteInt16(short s)
        {
            byte[] b = BitConverter.GetBytes(s);
            ms.Write(b, 0, b.Length);
        }

        public void WriteUInt16(ushort s)
        {
            byte[] b = BitConverter.GetBytes(s);
            ms.Write(b, 0, b.Length);
        }

        public void WriteInt32(int i)
        {
            byte[] b = BitConverter.GetBytes(i);
            ms.Write(b, 0, b.Length);
        }

        public void WriteUInt32(uint i)
        {
            byte[] b = BitConverter.GetBytes(i);
            ms.Write(b, 0, b.Length);
        }

        public void WriteInt64(long l)
        {
            byte[] b = BitConverter.GetBytes(l);
            ms.Write(b, 0, b.Length);
        }

        public void WriteUInt64(ulong l)
        {
            byte[] b = BitConverter.GetBytes(l);
            ms.Write(b, 0, b.Length);
        }

        public void WriteSingle(float f)
        {
            byte[] b = BitConverter.GetBytes(f);
            ms.Write(b, 0, b.Length);
        }

        public void WriteFloat(float f)
        {
            byte[] b = BitConverter.GetBytes(f);
            ms.Write(b, 0, b.Length);
        }

        public void WriteDouble(double d)
        {
            byte[] b = BitConverter.GetBytes(d);
            ms.Write(b, 0, b.Length);
        }

        public void WriteString(string s)
        {
            byte[] b = ByteConverterExt.ToBytes(s, Encoding.Default);
            ms.Write(b, 0, b.Length);
        }

        public void WriteString(string s, int l)
        {
            byte[] b = ByteConverterExt.ToBytes(s, Encoding.Default);
            ms.Write(b, 0, b.Length);

            int remainder = l - b.Length;        
            if (remainder > 0)
            {
                byte[] b2 = new byte[remainder];
                ms.Write(b2, 0, b2.Length);
            }
        }

        #endregion
    }
}
