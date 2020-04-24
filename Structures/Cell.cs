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

        object value;

        public object Value
        {
            get
            {
                if (Type == CellType.TYPE_STRING && value != null && Default == null && !((string)value).EndsWith("\0"))
                {
                    string valStr = value as string;
                    valStr += '\0';
                    value = valStr;
                }

                return value;
            }
            set => this.value = value;
        }

        public object[] ConfigOptions;

        private int length;
        public int Length
        {
            get
            { 
                if (length == 0 && Value != null && Default == null)
                    length = (int)Enum.Parse(typeof(CellLength), Enum.GetName(typeof(CellType), Type));

                return length;
            }
            set => length = value;
        }
        public object Default;
        public string Dependency;
        public int Position;
        public FlagType Flag;
        public bool Visible;

        public override string ToString() => Value as string;

        public Cell Clone() => this;

        object ICloneable.Clone() => Clone();
    }
}
