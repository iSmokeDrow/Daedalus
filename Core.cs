using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Text;
using Daedalus.Enums;
using Daedalus.Structures;
using Daedalus.Utilities;
using MoonSharp.Interpreter;
using System.Threading.Tasks;

namespace Daedalus
{
    //TODO: lua needs to move here!
    /// <summary>
    /// Provides low level access and manipulation of Rappelz .rdb data storage mediums
    /// </summary>
    public class Core
    {
        #region Private fields

        Script luaScript = new Script();
        TraditionalHeader tHeader;
        Cell[] dHeader_Template;
        Cell[] row_Template;
        public Row[] Rows;
        Row dHeader;
        string luaPath;
        string rdbPath;
        StreamIO sHelper;
        Encoding encoding = Encoding.Default;

        HeaderType headerType
        {
            get
            {
                if (luaScript == null)
                    return HeaderType.Undefined;
                else
                    return (UseHeader) ? HeaderType.Defined : HeaderType.Traditional;
            }
        }

        int prevRowIdx = 0;

        #endregion

        #region Public Properties

        /// <summary>
        /// Collection of cell information for the row template
        /// </summary>
        public Cell[] Cells(int index) => this[index].Cells;

        public Cell[] CellTemplate => row_Template.Clone() as Cell[];

        public Row RowTemplate => new Row(row_Template.Clone() as Cell[]);

        /// <summary>
        /// Collection of cell information for defined header
        /// </summary>
        public Cell[] HeaderTemplate => (Cell[])dHeader_Template.Clone();

        /// <summary>
        /// Amount of Cells contained in the row_Template
        /// </summary>
        public int CellCount => row_Template.Length;

        /// <summary>
        /// Amount of Row[] being stored in rows
        /// </summary>
        public int RowCount
        {
            get
            {
                if (headerType == HeaderType.Defined)
                    return dHeader.GetValueByFlag(FlagType.ROW_COUNT) as int? ?? default(int);
                else
                {
                    if (Rows != null  && tHeader.RowCount < Rows.Length)
                        tHeader.RowCount = Rows.Length;

                    return tHeader.RowCount;
                }
            }
            set
            {
                if (headerType == HeaderType.Defined)
                    dHeader[FlagType.ROW_COUNT] = value;
                else
                    tHeader.RowCount = value;
            }
        }

        /// <summary>
        /// Returns stored row at the provided index or null
        /// </summary>
        /// <param name="index">Zero-based ordinal location of the desired Row</param>
        /// <returns>Row object from rows[index] or null</returns>
        public Row this[int index] => Rows[index]; //TODO prolly gonna cause a problem

        /// <summary>
        /// Physical path to the LUA file containing structure definitions
        /// </summary>
        public string LuaPath
        {
            get { return luaPath ?? null; }
            set { luaPath = value; }
        }

        /// <summary>
        /// Physical path to the RDB file containing the data
        /// </summary>
        public string RdbPath
        {
            get { return rdbPath ?? null; }
            set { rdbPath = value; }
        }

        /// <summary>
        /// Filename (including extension) of the targeted RDB
        /// </summary>
        public string FileName
        {
            get
            {
                if (RdbPath != null)
                    return System.IO.Path.GetFileName(RdbPath);
                else
                    return luaScript.Globals["fileName"] as string;
            }
        }

        /// <summary>
        /// Name of the target Database table
        /// </summary>
        public string TableName => (luaScript.Globals["tableName"] != null) ? luaScript.Globals["tableName"] as string : string.Empty;

        /// <summary>
        /// Determines if the user has defined a Select statement for reading information from an SQL table.
        /// </summary>
        public bool UseSelectStatement => luaScript.Globals["selectStatement"] != null;

        /// <summary>
        /// User defined Select statement from the lua structure.
        /// </summary>
        public string SelectStatement => luaScript.Globals["selectStatement"].ToString();

        public bool UseSqlColumns => luaScript.Globals["sqlColumns"] != null;

        public string[] SqlColumns
        {
            get
            {
                try
                {
                    Table t = (Table)luaScript.Globals["sqlColumns"];

                    string[] names = new string[t.Length];

                    for (int i = 0; i < names.Length; i++)
                        names[i] = t.Get(i + 1).String;

                    return names;
                }
                catch (Exception ex) { throw new Exception(ex.Message, ex.InnerException); }
            }
        }

        /// <summary>
        /// All cell names in the row_Template
        /// </summary>
        public string[] CellNames => RowTemplate.CellNames;

        public string[] VisibleCellNames => RowTemplate.VisibleNames; //These three need to actually use a n 'RowTemplate' complete object

        public Cell[] VisibleCells => RowTemplate.VisibleCells;

        public bool IsSpecialCase => luaScript.Globals["specialCase"] != null;

        public SpecialCase Case
        {
            get
            {
                int caseVal = Convert.ToInt32(luaScript.Globals["specialCase"]);
                return (SpecialCase)caseVal;
            }
        }

        public bool UseRowProcessor => luaScript.Globals["ProcessRow"] != null;

        public bool UseHeader => luaScript.Globals[FieldsType.Header] != null;

        #endregion

        #region Constructors

        public Core() { }

        public Core(string luaPath, string rdbPath = null)
        {
            this.luaPath = luaPath;

            if (!string.IsNullOrEmpty(rdbPath))
                this.rdbPath = rdbPath;

           Initialize();
        }

        #endregion

        #region Events

        public event EventHandler<ProgressMaxArgs> ProgressMaxChanged;
        public event EventHandler<ProgressValueArgs> ProgressValueChanged;
        public event EventHandler<MessageArgs> MessageOccured;

        #endregion

        #region Event Delegates

        public void OnProgressMaxChanged(ProgressMaxArgs p) { ProgressMaxChanged?.Invoke(this, p); }
        public void OnProgressValueChanged(ProgressValueArgs p) { ProgressValueChanged?.Invoke(this, p); }
        public void OnMessageOccured(MessageArgs m) { MessageOccured?.Invoke(this, m); }

        #endregion

        #region Public methods

        public void Initialize(string luaPath)
        {
            this.luaPath = luaPath;
            Initialize();
        }

        public void Initialize(string luaPath, string rdbPath)
        {
            this.luaPath = luaPath;
            this.rdbPath = rdbPath;
            Initialize();
        }

        /// <summary>
        /// Initialize the script engine by iterating to lua structure file
        /// </summary>
        public void Initialize()
        {
            addGlobals();
            addUserData();

            luaScript.DoFile(luaPath);

            if (headerType == HeaderType.Defined)
                dHeader_Template = getCells(FieldsType.Header);

            row_Template = getCells("fields");           
        }

        public void ProcessRow(string mode, Row row, int rowNum)
        {
            DynValue res = null;

            try
            {
                res = luaScript.Call(luaScript.Globals["ProcessRow"], mode, row, rowNum);
            }
            catch (ScriptRuntimeException srEx)
            {
                throw new Exception(srEx.Message, srEx.InnerException);
            }
        }

        /// <summary>
        /// Process the data stored in a RDB buffer.
        /// </summary>
        /// <param name="buffer">Buffer to be processed</param>
        public void ParseBuffer(byte[] buffer)
        {
            try
            {
                sHelper = new StreamIO(buffer);
                parseHeader();
                parseContents();
            }
            catch (Exception ex) { OnMessageOccured(new MessageArgs($"An exception has occured!\nMessage: {ex.Message}\nStack-Trace: {ex.StackTrace}")); }
        }

        /// <summary>
        /// Write the data in the loaded rows to disk.
        /// </summary>
        public void Write()
        {
            try
            {
                if (sHelper == null) // If data was loaded from SQL
                    sHelper = new StreamIO(encoding);
                else
                    sHelper.Clear();

                writeHeader();
                writeContents();
            }
            catch (Exception ex) 
            { 
                OnMessageOccured(new MessageArgs($"An exception has occured!\nMessage: {ex.Message}\nStack-Trace: {ex.StackTrace}"));
            }
        }

        /// <summary>
        /// Define the Encoding which strings will be processed by
        /// </summary>
        /// <param name="encoding">Encoding for string processing</param>
        public void SetEncoding(Encoding encoding)
        {
            this.encoding = encoding;
        }

        /// <summary>
        /// Replaces stored rows object with provided rows object
        /// </summary>
        /// <param name="rows">Row[] object to be stored</param>
        public void SetData(Row[] rows)
        {
            tHeader.RowCount = rows.Length;
            this.Rows = rows;           
        }

        /// <summary>
        /// Clear the stored rows.
        /// </summary>
        public void ClearData()
        {
            Rows = new Row[0];
        }

        #endregion

        #region Private methods

        void addGlobals()
        {
            #region Type Globals

            luaScript.Globals["BYTE"] = 0;
            luaScript.Globals["BIT_VECTOR"] = 1;
            luaScript.Globals["BIT_FROM_VECTOR"] = 2;
            luaScript.Globals["INT16"] = 3;
            luaScript.Globals["SHORT"] = 3;
            luaScript.Globals["UINT16"] = 5;
            luaScript.Globals["USHORT"] = 6;
            luaScript.Globals["INT32"] = 7;
            luaScript.Globals["INT"] = 8;
            luaScript.Globals["UINT32"] = 9;
            luaScript.Globals["UINT"] = 10;
            luaScript.Globals["INT64"] = 11;
            luaScript.Globals["LONG"] = 12;
            luaScript.Globals["SINGLE"] = 13;
            luaScript.Globals["FLOAT"] = 14;
            luaScript.Globals["FLOAT32"] = 15;
            luaScript.Globals["DOUBLE"] = 16;
            luaScript.Globals["FLOAT64"] = 17;
            luaScript.Globals["DECIMAL"] = 18;
            luaScript.Globals["DATETIME"] = 19;
            luaScript.Globals["SID"] = 20;
            luaScript.Globals["STRING"] = 21;
            luaScript.Globals["STRING_BY_LEN"] = 22;
            luaScript.Globals["STRING_HEADER_REF"] = 23;
            luaScript.Globals["STRING_LEN"] = 25;

            #endregion

            #region Direction Globals

            luaScript.Globals["READ"] = "read";
            luaScript.Globals["WRITE"] = "write";

            #endregion

            #region Special Case Globals

            luaScript.Globals["DOUBLELOOP"] = 1;
            luaScript.Globals["ROWCOUNT"] = 1;
            luaScript.Globals["LOOPCOUNTER"] = "2";

            #endregion

            #region Flag Type Globals

            luaScript.Globals["BIT_FLAG"] = 3;

            #endregion
        }

        void addUserData()
        {
            UserData.RegisterType<Row>();
            UserData.RegisterType<Cell>();
        }

        Cell[] getCells(string tableName)
        {
            Table t = (Table)luaScript.Globals[tableName];
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

        /// <summary>
        /// Read and store information from the header section of the rdb file. 
        /// </summary>
        private void parseHeader()
        {
            switch (headerType)
            {
                case HeaderType.Traditional:

                    tHeader.DateTime = sHelper.Read<string>(8);
                    tHeader.Padding = sHelper.Read<byte[]>(120);
                    tHeader.RowCount = sHelper.Read<int>();

                    break;

                case HeaderType.Defined:

                    Row row = new Row(dHeader_Template);
                    populateRow(ref row, SenderType.PARSE_HEADER);
                    dHeader = row;

                    break;
            }
        }

        /// <summary>
        /// Read and store the data contents section of the rdb file based on user provided lua structure.
        /// </summary>
        private void parseContents()
        {
            Rows = new Row[RowCount];

            OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

            if (IsSpecialCase)
            {
                switch (Case)
                {
                    case SpecialCase.DOUBLE_LOOP:

                        List<Row> tRows = new List<Row>();

                        for (int r = 0; r < RowCount; r++)
                        {
                            int l = sHelper.Read<int>();

                            for (int i = 0; i < l; i++)
                            {
                                Row row = new Row(CellTemplate);
                                populateRow(ref row, SenderType.PARSE_ROW);

                                if (UseRowProcessor)
                                    ProcessRow(FileMode.Read, row, r);

                                tRows.Add(row);

                                if ((r * 100 / RowCount) != ((r - 1) * 100 / RowCount))
                                    OnProgressValueChanged(new ProgressValueArgs(r)); 
                            }
                        }

                        Rows = tRows.ToArray();

                        break;
                }
            }
            else
            {
                for (int r = 0; r < RowCount; r++)
                {
                    Row row = new Row(CellTemplate);
                    populateRow(ref row, SenderType.PARSE_ROW);

                    if (UseRowProcessor)
                        ProcessRow(FileMode.Read, row, r);

                    Rows[r] = row;

                    if ((r * 100 / RowCount) != ((r - 1) * 100 / RowCount))
                        OnProgressValueChanged(new ProgressValueArgs(r));
                }
            }

            OnProgressMaxChanged(new ProgressMaxArgs(100));
            OnProgressValueChanged(new ProgressValueArgs(0));
        }

        private void writeHeader()
        {
            switch (headerType)
            {
                case HeaderType.Traditional:
                    string newDate = string.Format("{0}{1}{2}", DateTime.Now.Year,
                                                                DateTime.Now.Month.ToString("D2"),
                                                                DateTime.Now.Day.ToString("D2"));
                    sHelper.Write(newDate);
                    sHelper.Write<string>(($"........RDB Written with Daedalus v{FileVersionInfo.GetVersionInfo("Daedalus.dll").FileVersion.Remove(0, 2)} by iSmokeDrow."), 120);

                    if (IsSpecialCase)
                    {
                        switch (Case)
                        {
                            case SpecialCase.DOUBLE_LOOP:
                                int pVal = 0;
                                int lCount = 0;

                                for (int r = 0; r < RowCount; r++)
                                {
                                    int cVal = (int)Rows[r].GetValueByFlag(FlagType.LOOP_COUNTER);

                                    if (pVal != cVal)
                                    {
                                        pVal = cVal;
                                        lCount++;
                                    }
                                }

                                sHelper.Write<int>(lCount);
                                break;
                        }
                    }
                    else
                        sHelper.Write<int>(RowCount);

                    break;

                case HeaderType.Defined:
                    writeRow(dHeader, SenderType.WRITE_HEADER);
                    break;
            }
        }

        private void writeContents()
        {
            OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

            if (IsSpecialCase)
            {
                switch (Case)
                {
                    case SpecialCase.DOUBLE_LOOP:

                        int pVal = 0;

                        for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                        {
                            Row row = Rows[rowIdx];
                            int cVal = (int)row.GetValueByFlag(FlagType.LOOP_COUNTER);

                            if (pVal != cVal)
                            {
                                string counterName = row.GetNameByFlag(FlagType.LOOP_COUNTER);
                                Row[] treeRows = FindAll(counterName, cVal);

                                sHelper.Write<int>(treeRows.Length);

                                for (int tR = 0; tR < treeRows.Length; tR++)
                                {
                                    if (UseRowProcessor)
                                        ProcessRow("write", Rows[tR], rowIdx);

                                    writeRow(treeRows[tR], SenderType.WRITE_ROW);
                                }

                                pVal = cVal;
                            }
                        }

                        break;
                }
            }
            else
            {
                for (int r = 0; r < RowCount; r++)
                {
                    Row i = Rows[r];
                    Row o = new Row(row_Template);

                    if (UseRowProcessor)
                    {
                        i.Clone(ref o);
                        ProcessRow(FileMode.Write, o, r);
                    }
                    else
                        o = i;

                    writeRow(o, SenderType.WRITE_ROW);

                    if ((r * 100 / RowCount) != ((r - 1) * 100 / RowCount))
                        OnProgressValueChanged(new ProgressValueArgs(r));
                }
            }

            OnMessageOccured(new MessageArgs(string.Format("Writing {0}", RdbPath)));
            sHelper.WriteToFile(RdbPath);

            OnProgressMaxChanged(new ProgressMaxArgs(100));
            OnProgressValueChanged(new ProgressValueArgs(0));
        }

        private void populateRow(ref Row row, SenderType sender)
        {
            int count = (sender == SenderType.PARSE_HEADER) ? dHeader_Template.Length : row.Length;

            for (int c = 0; c < count; c++)
            {
                Cell cell = (sender == SenderType.PARSE_HEADER) ? dHeader_Template[c] : row_Template[c];

                switch (cell.Type)
                {
                    case CellType.TYPE_SHORT:
                        goto case CellType.TYPE_SHORT;

                    case CellType.TYPE_INT_16:
                        row[c] = sHelper.Read<short>();
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_UINT_16;

                    case CellType.TYPE_UINT_16:
                        row[c] = sHelper.Read<ushort>();
                        break;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32:
                        row[c] = sHelper.Read<int>();
                        break;

                    case CellType.TYPE_UINT:
                        goto case CellType.TYPE_UINT_32;

                    case CellType.TYPE_UINT_32:
                        row[c] = sHelper.Read<uint>();
                        break;

                    case CellType.TYPE_INT_64:
                        goto case CellType.TYPE_LONG;

                    case CellType.TYPE_LONG:
                        row[c] = sHelper.Read<long>();
                        break;

                    //TODO: Implement TYPE_ULONG
                    //case CellType.TYPE_ULONG:
                    //    row[c] = sHelper.ReadInt64;
                    //    break;

                    case CellType.TYPE_DATETIME:
                        {
                            int secFromEpoch = sHelper.Read<int>();
                            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                            row[c] = epoch.AddSeconds(secFromEpoch);
                        }
                        break;

                    case CellType.TYPE_BYTE:
                        if (cell.Length > 1)
                            row[c] = sHelper.Read<byte[]>(cell.Length);
                        else
                            row[c] = sHelper.Read<byte>();
                        break;

                    case CellType.TYPE_BIT_VECTOR:
                        row[c] = new BitVector32(sHelper.Read<int>());
                        break;

                    case CellType.TYPE_BIT_FROM_VECTOR:
                        {
                            int bitPos = cell.Position;

                            string dependency = cell.Dependency;
                            if (dependency == null)
                                throw new ArgumentNullException(string.Format("{0} does not have a dependency listed!", ((Cell)row[c]).Name));

                            BitVector32 bitVector = (BitVector32)row[dependency];
                            row[c] = Convert.ToInt32(bitVector[1 << bitPos]);
                            break;
                        }

                    case CellType.TYPE_DECIMAL:
                        int v0 = sHelper.Read<int>();
                        decimal v1 = v0 / 100m;
                        row[c] = v1;
                        break;

                    case CellType.TYPE_FLOAT:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_FLOAT_32:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_SINGLE:
                        row[c] = sHelper.Read<float>();
                        break;

                    case CellType.TYPE_FLOAT_64:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_DOUBLE:
                        row[c] = sHelper.Read<double>();
                        break;

                    case CellType.TYPE_SID:
                        row[c] = prevRowIdx;
                        prevRowIdx++;
                        break;

                    case CellType.TYPE_STRING:
                        row[c] = ByteConverterExt.ToString(sHelper.Read<byte[]>(cell.Length), 
                                                                        Encoding.Default);                     
                        break;

                    case CellType.TYPE_STRING_BY_LEN:
                        {
                            string dependency = cell.Dependency;
                            int len = 0;
                            int.TryParse(row[dependency].ToString(), out len);

                            if (len < 0)
                                break;
                            else
                                row[c] = sHelper.Read<string>(len);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_HEADER_REF:
                        {
                            string dependency = cell.Dependency;
                            int len = dHeader[dependency] as int? ?? default(int);
                            row[c] = sHelper.Read<string>(len);
                        }
                        break;

                    case CellType.TYPE_STRING_LEN:
                        goto case CellType.TYPE_INT;
                }
            }
        }

        private void writeRow(Row row, SenderType sender)
        {
            int count = (sender == SenderType.WRITE_HEADER) ? dHeader_Template.Length : row.Length;

            for (int c = 0; c < count; c++)
            {
                Cell cell = (sender == SenderType.WRITE_HEADER) ? dHeader_Template[c] : row_Template[c];

                switch (cell.Type)
                {
                    case CellType.TYPE_INT_16:
                        goto case CellType.TYPE_SHORT;

                    case CellType.TYPE_SHORT:
                        {
                            short s = (short)row[c];
                            sHelper.Write<short>(s);
                        }
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_UINT_16;

                    case CellType.TYPE_UINT_16:
                        {
                            ushort s = (ushort)row[c];
                            sHelper.Write<ushort>(s);
                        }
                        break;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32: //TODO: all these shits should use tryparse
                        {
                            int i = Convert.ToInt32(row[c]);
                            sHelper.Write<int>(i);
                        }
                        break;

                    case CellType.TYPE_UINT_32:
                        {
                            uint i = Convert.ToUInt32(row[c]);
                            sHelper.Write<uint>(i);
                        }
                        break;

                    case CellType.TYPE_INT_64:
                        goto case CellType.TYPE_LONG;

                    case CellType.TYPE_LONG:
                        {
                            long l = (long)row[c];
                            sHelper.Write<double>(l);
                        }
                        break;

                    //TODO: Implement TYPE_ULONG
                    //case CellType.TYPE_ULONG:
                    //    break;

                    case CellType.TYPE_DATETIME:
                        {
                            DateTime dt = (DateTime)row[c];
                            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                            sHelper.Write<int>(Convert.ToInt32((dt - epoch).TotalSeconds));
                        }
                        break;

                    case CellType.TYPE_BYTE:
                        if (cell.Length > 0)
                        {
                            byte[] b = new byte[cell.Length];
                            sHelper.Write<byte[]>(b);
                        }
                        else
                        {
                            byte b = row[c] as byte? ?? default(byte);
                            sHelper.Write<byte>(b);
                        }
                        break;

                    case CellType.TYPE_BIT_VECTOR:
                        {
                            int i = BitConverter.ToInt32(generateBitVector(row, cell.Name), 0);
                            sHelper.Write<int>(i);
                        }
                        break;

                    case CellType.TYPE_DECIMAL:
                        decimal v0 = row[c] as decimal? ?? default(decimal);
                        int v1 = Convert.ToInt32(v0 * 100);
                        sHelper.Write<int>(v1);
                        break;

                    case CellType.TYPE_FLOAT:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_FLOAT_32:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_SINGLE:
                        {
                            float s = (float)row[c];
                            sHelper.Write<float>(s);
                        }
                        break;

                    case CellType.TYPE_FLOAT_64:
                        goto case CellType.TYPE_DOUBLE;

                    case CellType.TYPE_DOUBLE:
                        {
                            double d = (double)row[c];
                            sHelper.Write<double>(d);
                        }
                        break;

                    case CellType.TYPE_STRING:
                        {
                            Cell tCell = row.GetCell(c);

                            string s = tCell.Value as string;
                            sHelper.Write<string>(s, tCell.Length);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_LEN:
                        {
                            Cell tCell = row.GetCell(c);
                            string dep = tCell.Dependency;
                            Cell dCell = row.GetCell(dep);

                            string s = tCell.Value as string; //TODO: update cells to be a generic class
                            sHelper.Write<string>(s, (int)dCell.Value);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_HEADER_REF:
                        {
                            byte[] buffer = ByteConverterExt.ToBytes(row[c].ToString(), Encoding.Default);
                            string refName = cell.Dependency;
                            int remainder = Convert.ToInt32(dHeader[refName]) - buffer.Length;
                            sHelper.Write<byte[]>(buffer);
                            sHelper.Write<byte[]>(new byte[remainder]);
                        }
                        break;

                    case CellType.TYPE_STRING_LEN:
                        {
                            string cellName = row.GetNameByDependency(cell.Name);
                            int valLen = row[cellName].ToString().Length + 1;
                            sHelper.Write<int>(valLen);
                        }
                        break;
                }                   
            }
        }

        int getMatchCount(string key, object value)
        {
            int c = 0;

            for (int r = 0; r < RowCount; r++)
            {
                Row row = Rows[r];
                var v = row[key];

                if ((int)v == (int)value)
                    c++;
            }

            return c;
        }

        public Row[] FindAll(string key, int value)
        {
            List<Row> results = new List<Row>();

            for (int r = 0; r < Rows.Length; r++)
            {
                Row row = Rows[r];
                Cell cell = row.GetCell(key);

                if ((int)cell.Value == value)
                    results.Add(row);
            }

            return results.ToArray();
        }

        public DbCommand GenerateInsert()
        {
            DbCommand cmd = new SqlCommand();
            string[] names = (UseSqlColumns) ? SqlColumns : CellNames;
            int len = names.Length;

            string columns = string.Empty;
            string parameterStr = string.Empty;
            List<DbParameter> parameters = new List<DbParameter>();     

            for (int c = 0; c < len; c++)
            {
                string val = names[c];
                Cell cell = Rows[0].GetCell(val);
                CellType columnType = cell.Type;

                if (cell.Visible)
                {
                    columns += string.Format("[{0}]{1},", val, string.Empty);
                    parameterStr += string.Format("@{0}{1},", val, string.Empty);
                    SqlParameter sqlParam = new SqlParameter() { ParameterName = val };

                    switch (columnType)
                    {
                        case CellType.TYPE_SHORT:
                            goto case CellType.TYPE_INT_16;

                        case CellType.TYPE_INT_16:
                            sqlParam.SqlDbType = SqlDbType.SmallInt;
                            break;

                        case CellType.TYPE_USHORT:
                            goto case CellType.TYPE_INT_16;

                        case CellType.TYPE_UINT_16:
                            goto case CellType.TYPE_INT_16;

                        case CellType.TYPE_INT:
                            goto case CellType.TYPE_INT_32;

                        case CellType.TYPE_INT_32:
                            sqlParam.SqlDbType = SqlDbType.Int;
                            break;

                        case CellType.TYPE_UINT:
                            goto case CellType.TYPE_INT_32;

                        case CellType.TYPE_UINT_32:
                            goto case CellType.TYPE_INT_32;

                        case CellType.TYPE_INT_64:
                            sqlParam.SqlDbType = SqlDbType.BigInt;
                            break;

                        case CellType.TYPE_LONG:
                            goto case CellType.TYPE_INT_64;

                        case CellType.TYPE_BYTE:
                            sqlParam.SqlDbType = SqlDbType.TinyInt;
                            break;

                        case CellType.TYPE_DATETIME:
                            sqlParam.SqlDbType = SqlDbType.DateTime;
                            break;

                        case CellType.TYPE_DECIMAL:
                            sqlParam.SqlDbType = SqlDbType.Decimal;
                            break;

                        case CellType.TYPE_FLOAT: case CellType.TYPE_FLOAT_32:
                            goto case CellType.TYPE_SINGLE;

                        case CellType.TYPE_SINGLE:
                            sqlParam.SqlDbType = SqlDbType.Real;
                            break;

                        case CellType.TYPE_DOUBLE:
                            sqlParam.SqlDbType = SqlDbType.Float;
                            break;

                        case CellType.TYPE_STRING:
                            sqlParam.SqlDbType = SqlDbType.VarChar;
                            break;

                        case CellType.TYPE_STRING_BY_LEN:
                            goto case CellType.TYPE_STRING;

                        case CellType.TYPE_STRING_BY_REF:
                            goto case CellType.TYPE_STRING;
                    }
                    
                    parameters.Add(sqlParam as DbParameter);
                }
            }      

            cmd.CommandText = string.Format("INSERT INTO <tableName> ({0}) VALUES ({1})", columns.Remove(columns.Length - 1, 1), parameterStr.Remove(parameterStr.Length - 1, 1));
            cmd.Parameters.AddRange(parameters.ToArray());
            return cmd;
        }

        byte[] generateBitVector(Row row, string fieldName)
        {
            Cell[] cells = row.GetBitFields(fieldName);
            BitVector32 bitVector = row.GetBitVector(fieldName);

            foreach (Cell cell in cells)
                bitVector[1 << cell.Position] = Convert.ToBoolean(cell.Value);

            return BitConverter.GetBytes(bitVector.Data);
        }

        #endregion
    }
}
