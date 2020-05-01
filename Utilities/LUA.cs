using System;
using System.Collections.Generic;
using Daedalus.Structures;
using MoonSharp.Interpreter;
using Daedalus.Enums;

namespace Daedalus.Utilities
{
    public class LUA
    {
        Script engine = null;
        private string scriptCode = null;
        private DynValue dynVal = null;

        public LUA(string scriptCode)
        {
            engine = new Script();
            this.scriptCode = scriptCode;
            addGlobals();
            UserData.RegisterType<Row>();
            UserData.RegisterType<Cell>();
            dynVal = engine.DoString(scriptCode);
        }

        public string FileName
        {
            get
            {
                var name = engine.Globals["fileName"];
                return (name != null) ? name.ToString() : null;
            }
        }

        public string TableName
        {
            get
            {
                var name = engine.Globals["tableName"];
                return (name != null) ? name.ToString() : null;
            }
        }

        public bool UseRowProcessor { get { return engine.Globals["ProcessRow"] != null; } }

        public bool UseSelectStatement { get { return engine.Globals["selectStatement"] != null; } }

        public string SelectStatement { get { return engine.Globals["selectStatement"].ToString(); } }

        public bool UseSqlColumns { get { return engine.Globals["sqlColumns"] != null; } }

        public string[] SqlColumns
        {
            get
            {
                try
                {
                    Table t = (Table)engine.Globals["sqlColumns"];

                    string[] names = new string[t.Length];

                    for (int i = 0; i < names.Length; i++)
                        names[i] = t.Get(i + 1).String;

                    return names;
                }
                catch (Exception ex) { throw new Exception(ex.Message, ex.InnerException); }
            }
        }

        public bool UseHeader
        {
            get
            {
                return engine.Globals[FieldsType.Header] != null;
            }
        }

        public bool SpecialCase { get { return engine.Globals["specialCase"] != null; } }

        public SpecialCase Case
        {
            get
            {
                int caseVal = Convert.ToInt32(engine.Globals["specialCase"]);
                return (SpecialCase)caseVal;
            }
        }

        private void addGlobals()
        {
            #region Type Globals

            engine.Globals["BYTE"] = 0;
            engine.Globals["BIT_VECTOR"] = 1;
            engine.Globals["BIT_FROM_VECTOR"] = 2;
            engine.Globals["INT16"] = 3;
            engine.Globals["SHORT"] = 3;
            engine.Globals["UINT16"] = 5;
            engine.Globals["USHORT"] = 6;
            engine.Globals["INT32"] = 7;
            engine.Globals["INT"] = 8;
            engine.Globals["UINT32"] = 9;
            engine.Globals["UINT"] = 10;
            engine.Globals["INT64"] = 11;
            engine.Globals["LONG"] = 12;
            engine.Globals["SINGLE"] = 13;
            engine.Globals["FLOAT"] = 14;
            engine.Globals["FLOAT32"] = 15;
            engine.Globals["DOUBLE"] = 16;
            engine.Globals["FLOAT64"] = 17;
            engine.Globals["DECIMAL"] = 18;
            engine.Globals["DATETIME"] = 19;
            engine.Globals["SID"] = 20;
            engine.Globals["STRING"] = 21;            
            engine.Globals["STRING_BY_LEN"] = 22;
            engine.Globals["STRING_HEADER_REF"] = 23;
            engine.Globals["STRING_LEN"] = 25;

            #endregion

            #region Direction Globals

            engine.Globals["READ"] = "read";
            engine.Globals["WRITE"] = "write";

            #endregion

            #region Special Case Globals

            engine.Globals["DOUBLELOOP"] = 1;
            engine.Globals["ROWCOUNT"] = 1;
            engine.Globals["LOOPCOUNTER"] = "2";

            #endregion

            #region Flag Type Globals

            engine.Globals["BIT_FLAG"] = 3;

            #endregion
        }

        public Cell[] GetFieldList(string tableName)
        {
            Table t = (Table)engine.Globals[tableName];
            Cell[] fields = new Cell[t.Length];

            for (int tIdx = 1; tIdx < t.Length + 1; tIdx++)
            {
                Table fieldT = t.Get(tIdx).Table;
                Cell field = new Cell();

                field.Name = fieldT.Get(1).String;
                field.Type = (CellType)fieldT.Get(2).Number;
                field.Length = (int)fieldT.Get("length").Number;
                field.Dependency = fieldT.Get("dependency").String;
                field.Default = (object)fieldT.Get("default").ToObject();
                field.Position = (int)fieldT.Get("bit_position").Number;
                int fVal = Convert.ToInt32(fieldT["flag"]);
                field.Flag = (FlagType)fVal;
                field.Visible = (fieldT.Get("show").ToObject() != null) ? Convert.ToBoolean(fieldT.Get("show").Number) : true;

                field.Flag = (FlagType)fieldT.Get("flag").Number;
                
                if (field.Flag == FlagType.BIT_FLAG)
                {
                    Table flagT = fieldT.Get("opt").Table;
                    if (flagT?.Length > 0)
                    {
                        field.ConfigOptions = new object[flagT.Length];
                        for (int k = 0; k < flagT.Length; ++k)
                            field.ConfigOptions[k] = flagT.Get(k + 1).String;
                    }
                }

                fields[tIdx - 1] = field;
            }

            return fields;
        }

        public void CallRowProcessor(string mode, Row row, int rowNum)
        {          
            DynValue res = null;

            try { res = engine.Call(engine.Globals["ProcessRow"], mode, row, rowNum); }
            catch (ScriptRuntimeException srEx) { throw new Exception(srEx.Message, srEx.InnerException);  }        
        }
    }    
}
