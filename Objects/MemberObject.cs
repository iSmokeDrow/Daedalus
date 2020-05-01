using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Daedalus.Enums;

namespace Daedalus.Objects
{
    public class MemberObject : DataObject
    {
        public bool IsArray = false;

        public int ArrayLength = -1;

        public string DependsOn = null;

        public bool IsDependent => DependsOn != null;

        public TemplateObject Template = null;

        public bool HasTemplate => Template != null;

        public override string Name { get; set; }

        public override Enum Type { get; set; }

        public int Length
        {
            get
            {
                if (IsArray)
                    return ((IList)Data).Count;
                else
                    return -1;
            }
        }

        public override dynamic Value
        {
            get
            {
                if (IsArray)
                    throw new Exception("Cannot convert an array into a primitive!");
                else
                    return Data;
            }
        }

        public List<string> Comments = new List<string>();

        public MemberObject(DataObjectType type) => Type = type;

        public MemberObject(string name, DataObjectType type) 
        {
            Name = name;
            Type = type;
        }

        public override dynamic GetValue(int index)
        {
            if (!IsArray)
                throw new Exception("Cannot get an array element from a non-array object!");

            IList data = ((IList)Data);

            if (index >= 0 && index < data.Count)
                return data[index];
            else
                throw new IndexOutOfRangeException();
        }

        public void SetValue(int index, dynamic value)
        {
            if (!IsArray)
                throw new Exception("Cannot set array element value of non-array object!");

            IList data = ((IList)Data);

            if (index >= 0 && index < data.Count)
                data[index] = value;
            else
                throw new IndexOutOfRangeException();
        }
    }
}
