using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Daedalus.Utilities
{
    public class ByteConverterExt
    {
        public static decimal ByteArrayToDecimal(byte[] src, int offset)
        {
            var i1 = BitConverter.ToInt32(src, offset);
            var i2 = BitConverter.ToInt32(src, offset + 4);
            var i3 = BitConverter.ToInt32(src, offset + 8);
            var i4 = BitConverter.ToInt32(src, offset + 12);

            return new decimal(new int[] { i1, i2, i3, i4 });
        }

        public static string ToString(byte[] b, Encoding encoding)
        {
            int num = 0;
            for (int i = 0; i < (int)b.Length && b[i] > 0; i++) { num++; }
            byte[] numArray = new byte[num];
            Array.Copy(b, 0, numArray, 0, num);

            return encoding.GetString(numArray);
        }

        public static Byte[] ToBytes(string str, Encoding encoding)
        {
            Byte[] bytes = null;
            bytes = encoding.GetBytes(str);
            return bytes;
        }

        public static Byte[] ToBytes(string str, int size)
        {
            Byte[] bytes = new Byte[size];
            byte[] buffer = Encoding.GetEncoding("ASCII").GetBytes(str);
            try
            {
                for (int i = 0; i < buffer.Length; i++)
                {
                    bytes[i] = buffer[i];
                }
            }
            catch
            {
                throw new Exception(Encoding.Default.GetString(buffer));
            }
            return bytes;
        }
    }
}
