using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;
using Daedalus.Enums;

namespace Daedalus.Objects
{
    public class DataObjectArray : DataObject    
    {
        public DataObjectArray(DataObjectType type, int capacity)
        {
            switch (type)
            {
                case DataObjectType.BYTE:
                case DataObjectType.DOUBLE:
                case DataObjectType.INT:
                case DataObjectType.SHORT:
                case DataObjectType.FLOAT:
                case DataObjectType.NONE:
                case DataObjectType.STRING:
                case DataObjectType.STRING_BY_LEN:
                case DataObjectType.TEMPLATE:
                    throw new Exception("Cannot create an array object with a non-array type!");

                case DataObjectType.BYTE_ARRAY:
                    Data = new List<char>(capacity);
                    break;

                case DataObjectType.DOUBLE_ARRAY:
                    Data = new List<double>(capacity);
                    break;

                case DataObjectType.INT_ARRAY:
                    Data = new List<int>(capacity);
                    break;

                case DataObjectType.UINT_ARRAY:
                    Data = new List<uint>(capacity);
                    break;

                case DataObjectType.SHORT_ARRAY:
                    Data = new List<short>(capacity);
                    break;

                case DataObjectType.USHORT_ARRAY:
                    Data = new List<ushort>(capacity);
                    break;

                case DataObjectType.FLOAT_ARRAY:
                    Data = new List<float>(capacity);
                    break;

                case DataObjectType.STRING_ARRAY:
                    Data = new List<string>(capacity);
                    break;

                default:
                    break;
            }
            
        }

        public dynamic this[int index]
        {
            get
            {
                IList data = ((IList)Data);

                if (data == null)
                    throw new InvalidDataException("data cannot be null!");

                if (index >= 0 && index < data.Count)
                    return data[index];
                else
                    throw new Exception("index outside of range!");
            }
            set
            {
                IList data = ((IList)Data);
                data[index] = value;
            }
        }
    }
}
