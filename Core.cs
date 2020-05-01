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

namespace Daedalus
{
    //TODO: lua needs to move here!
    //TODO: InsertStatement: mysql/mssql both can be downcast to DbCommand!

    /// <summary>
    /// Provides low level access and manipulation of Rappelz .rdb data storage mediums
    /// </summary>
    public class Core
    {
        #region Private fields

        TraditionalHeader tHeader;
        private Cell[] dHeader_Template;
        private Cell[] row_Template;
        private Row[] rows;
        private Row dHeader;
        LUA lua;
        string luaPath;
        string rdbPath;
        StreamIO sHelper;
        private Encoding encoding;

        HeaderType headerType
        {
            get
            {
                if (lua == null)
                    return HeaderType.Undefined;
                else
                    return (lua.UseHeader) ? HeaderType.Defined : HeaderType.Traditional;
            }
        }

        int prevRowIdx = 0;

        #endregion

        #region Public fields

        /// <summary>
        /// Collection of cell information for the row template
        /// </summary>
        public Cell[] CellTemplate
        {
            get { return (Cell[])row_Template.Clone(); }
        }

        /// <summary>
        /// Collection of cell information for defined header
        /// </summary>
        public Cell[] HeaderTemplate
        {
            get { return (Cell[])dHeader_Template.Clone(); }
        }

        /// <summary>
        /// Amount of Cells contained in the row_Template
        /// </summary>
        public int CellCount
        {
            get { return row_Template.Length; }
        }

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
                    if (rows != null  && tHeader.RowCount < rows.Length)
                        tHeader.RowCount = rows.Length;

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
        /// Collection of loaded Rows
        /// </summary>
        public Row[] Rows
        {
            get { return rows; }
            set { rows = value; }
        }

        /// <summary>
        /// Returns stored row at the provided index or null
        /// </summary>
        /// <param name="index">Zero-based ordinal location of the desired Row</param>
        /// <returns>Row object from rows[index] or null</returns>
        public Row this[int index]
        {
            get { return rows[index]; }
        }

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
                    return lua.FileName;
            }
        }

        /// <summary>
        /// Name of the target Database table
        /// </summary>
        public string TableName
        {
            get { return lua.TableName; }
        }

        /// <summary>
        /// Determines if the user has defined a Select statement for reading information from an SQL table.
        /// </summary>
        public bool UseSelectStatement
        {
            get { return lua.UseSelectStatement; }
        }

        /// <summary>
        /// User defined Select statement from the lua structure.
        /// </summary>
        public string SelectStatement
        {
            get { return lua.SelectStatement; }
        }

        /// <summary>
        /// All cell names in the row_Template
        /// </summary>
        public string[] CellNames
        {
            get { return rows[0].CellNames; }
        }

        public string[] VisibleCellNames
        {
            get { return rows[0].VisibleNames; }      
        }

        public Cell[] VisibleCells => new Row(CellTemplate).VisibleCells;

        #endregion

        #region Constructors

        /// <summary>
        /// Dummy constructor
        /// </summary>
        public Core() { }

        /// <summary>
        /// Initialize the core with paths and encoding
        /// </summary>
        /// <param name="luaPath">Physical path to the lua structure file</param>
        /// <param name="rdbPath">Physical path to the rdb data file</param>
        /// <param name="encoding">Encoding which to read and write strings</param>
        public Core(string luaPath, string rdbPath, Encoding encoding)
        {
            this.luaPath = luaPath;
            this.rdbPath = rdbPath;
            this.encoding = encoding;
        }

        /// <summary>
        /// Initialize the core with paths
        /// </summary>
        /// <param name="luaPath">Physical path to the lua structure file</param>
        /// <param name="rdbPath">Physical path to the rdb data file</param>
        public Core(string luaPath, string rdbPath)
        {
            this.luaPath = luaPath;
            this.rdbPath = rdbPath;
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

        /// <summary>
        /// Initialize the script engine by iterating to lua structure file
        /// </summary>
        public void Initialize()
        {
            lua = new LUA(FileIO.ReadAllText(luaPath));

            if (headerType == HeaderType.Defined)
                dHeader_Template = lua.GetFieldList(FieldsType.Header);

            row_Template = lua.GetFieldList("fields");
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
            this.rows = rows;           
        }

        #endregion

        #region Private methods

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
            rows = new Row[RowCount];

            OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

            if (lua.SpecialCase)
            {
                switch (lua.Case)
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

                                if (lua.UseRowProcessor)
                                    lua.CallRowProcessor(FileMode.Read, row, r);

                                tRows.Add(row);

                                if ((r * 100 / RowCount) != ((r - 1) * 100 / RowCount))
                                    OnProgressValueChanged(new ProgressValueArgs(r)); 
                            }
                        }

                        rows = tRows.ToArray();

                        break;
                }
            }
            else
            {
                for (int r = 0; r < RowCount; r++)
                {
                    Row row = new Row(CellTemplate);
                    populateRow(ref row, SenderType.PARSE_ROW);

                    if (lua.UseRowProcessor)
                        lua.CallRowProcessor(FileMode.Read, row, r);

                    rows[r] = row;

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

                    if (lua.SpecialCase)
                    {
                        switch (lua.Case)
                        {
                            case SpecialCase.DOUBLE_LOOP:
                                int pVal = 0;
                                int lCount = 0;

                                for (int r = 0; r < RowCount; r++)
                                {
                                    int cVal = (int)rows[r].GetValueByFlag(FlagType.LOOP_COUNTER);

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

            if (lua.SpecialCase)
            {
                switch (lua.Case)
                {
                    case SpecialCase.DOUBLE_LOOP:

                        int pVal = 0;

                        for (int rowIdx = 0; rowIdx < RowCount; rowIdx++)
                        {
                            Row row = rows[rowIdx];
                            int cVal = (int)row.GetValueByFlag(FlagType.LOOP_COUNTER);

                            if (pVal != cVal)
                            {
                                string counterName = row.GetNameByFlag(FlagType.LOOP_COUNTER);
                                Row[] treeRows = FindAll(counterName, cVal);

                                sHelper.Write<int>(treeRows.Length);

                                for (int tR = 0; tR < treeRows.Length; tR++)
                                {
                                    if (lua.UseRowProcessor)
                                        lua.CallRowProcessor("write", rows[tR], rowIdx);

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
                    Row i = rows[r];
                    Row o = new Row(row_Template);

                    if (lua.UseRowProcessor)
                    {
                        i.Clone(ref o);
                        lua.CallRowProcessor(FileMode.Write, o, r);
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
                            short s = row[c] as short? ?? default(short);
                            sHelper.Write<short>(s);
                        }
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_UINT_16;

                    case CellType.TYPE_UINT_16:
                        {
                            ushort s = row[c] as ushort? ?? default(ushort);
                            sHelper.Write<ushort>(s);
                        }
                        break;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32:
                        {
                            int i = row[c] as int? ?? default(int);
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
                            long l = row[c] as long? ?? default(long);
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
                            float s = row[c] as float? ?? default(float);
                            sHelper.Write<float>(s);
                        }
                        break;

                    case CellType.TYPE_FLOAT_64:
                        goto case CellType.TYPE_DOUBLE;

                    case CellType.TYPE_DOUBLE:
                        {
                            double d = row[c] as double? ?? default(double);
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
                Row row = rows[r];
                var v = row[key];

                if ((int)v == (int)value)
                    c++;
            }

            return c;
        }

        public Row[] FindAll(string key, int value)
        {
            List<Row> results = new List<Row>();

            for (int r = 0; r < rows.Length; r++)
            {
                Row row = rows[r];
                Cell cell = row.GetCell(key);

                if ((int)cell.Value == value)
                    results.Add(row);
            }

            return results.ToArray();
        }

        public DbCommand GenerateInsert()
        {
            DbCommand cmd = new SqlCommand();
            string[] names = (lua.UseSqlColumns) ? lua.SqlColumns : CellNames;
            int len = names.Length;

            string columns = string.Empty;
            string parameterStr = string.Empty;
            List<DbParameter> parameters = new List<DbParameter>();     

            for (int c = 0; c < len; c++)
            {
                string val = names[c];
                Cell cell = rows[0].GetCell(val);
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

        /// <summary>
        /// Clear the stored rows.
        /// </summary>
        public void ClearData()
        {
            rows = new Row[0];
        }
    }
}
