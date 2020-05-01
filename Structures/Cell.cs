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
                string valStr = value as string;

                if (value != null && Default == null)
                {
                    switch (Type)
                    {
                        case CellType.TYPE_STRING:
                            if (!((string)value).EndsWith("\0"))
                            {
                                valStr += '\0';
                                value = valStr;
                            }
                            break;
                    }
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
                {
                    string typeName = Enum.GetName(typeof(CellType), Type);
                    CellLength cLength;

                    if (!string.IsNullOrEmpty(typeName))
                    {
                        if (Enum.TryParse(typeName, out cLength))
                            length = (int)cLength;
                        else // Cell does not have an auto len
                            length = -1;
                    }
                    else
                        throw new KeyNotFoundException($"Could not enumerate type: {Type}!");
                }
                else if (length == 0 && Type == CellType.TYPE_BYTE)
                    length = 1;

                return length;
            }
            set => length = value;
        }
        public object Default;
        public string Dependency;
        public bool HasDependency => Dependency != null;
        public int Position;
        public FlagType Flag;
        public bool Visible;

        public override string ToString() => Value as string;

        public Cell Clone() => this;

        object ICloneable.Clone() => Clone();
    }
}
