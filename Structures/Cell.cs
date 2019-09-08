using System;
using System.Collections.Generic;
using System.Text;
using Daedalus.Enums;

namespace Daedalus.Structures
{
    public struct Cell : ICloneable
    {
        public string Name;
        public CellType Type;
        public object Value;
        private int length;
        public int Length
        {
            get
            {
                if (Type == CellType.TYPE_STRING && Value != null || Type == CellType.TYPE_STRING_BY_LEN && Value != null)
                {
                    length = Value.ToString().Length;
                    return length;
                }

                if (length == 0 && Value != null)
                {
                    string type = Enum.GetName(typeof(CellType), Type);
                    return (int)Enum.Parse(typeof(CellLength), type);
                }
                else
                    return length;
            }
            set { length = value; }
        }
        public object Default;
        public string Dependency;
        public int Position;
        public FlagType Flag;
        public bool Visible;

        public Cell Clone()
        {
            return this;
        }

        object ICloneable.Clone()
        {
            return Clone();
        }
    }
}
