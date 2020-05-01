using System;
using System.Collections.Generic;
using System.Text;
using Daedalus.Enums;

namespace Daedalus.Objects
{
    public class TemplateObject : DataObject
    {
        public override string Name { get; set; }
        public override Enum Type => DataObjectType.TEMPLATE;
        public Guid GUID = Guid.Empty;
        public List<MemberObject> Members = new List<MemberObject>();

        public MemberObject this[int index]
        {
            get
            {
                if (index >= 0 && index < Members.Count)
                    return Members[index];

                return null;
            }
            set
            {
                if (index >= 0 && index < Members.Count)
                    Members[index] = value;
            }
        }

        public MemberObject this[string name]
        {
            get
            {
                int index = 0;

                if ((index = Members.FindIndex(m => m.Name == name)) != -1)
                    return Members[index];

                return null;
            }
            set
            {
                int index = 0;

                if ((index = Members.FindIndex(m => m.Name == name)) != -1)
                    Members[index] = Members[index];
            }
        }
    }
}
