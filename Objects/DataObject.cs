using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;
using Daedalus.Enums;

namespace Daedalus.Objects
{
    public class DataObject
    {
        public virtual dynamic Data { get; set; } = null;

        public virtual String Name { get; set; } = null;

        public virtual Enum Type { get; set; } = DataObjectType.NONE;

        public virtual dynamic Value => null;

        public virtual dynamic GetValue(int index) => null;

        public bool HasData => Data != null;

        protected virtual int length => throw new NotImplementedException();
    }
}
