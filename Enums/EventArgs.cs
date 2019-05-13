using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Daedalus.Enums
{
    public class ProgressMaxArgs : EventArgs
    {
        public int Maximum;

        public ProgressMaxArgs(int m) { Maximum = m; }
    }

    public class ProgressValueArgs : EventArgs
    {
        public int Value;
        public ProgressValueArgs(int v) { Value = v; }
    }

    public class MessageArgs : EventArgs
    {
        public string Message;
        public MessageArgs(string s) { Message = s; }
    }
}
