using System;
using System.Collections.Generic;
using System.Text;

namespace Daedalus.Structures
{
    public struct TraditionalHeader
    {
        public string DateTime;
        public byte[] Padding;
        public int RowCount;
    }
}
