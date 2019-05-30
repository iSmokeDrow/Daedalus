using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Daedalus.Enums;
using Daedalus.Structures;
using Daedalus.Utilities;

namespace Daedalus
{
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

        public Cell[] CellTemplate
        {
            get { return (Cell[])row_Template.Clone(); }
        }

        public int CellCount
        {
            get { return row_Template.Length; }
        }

        public int RowCount
        {
            get
            {
                int v = 0;

                if (headerType == HeaderType.Defined)
                    v = dHeader.GetValueByFlag(FlagType.ROW_COUNT) as int? ?? default(int);
                else
                    v = (rows != null) ? rows.Length : tHeader.RowCount;

                return v;
            }
        }

        public Row[] Rows
        {
            get { return rows; }
            set { rows = value; }
        }

        public Row this[int index]
        {
            get { return rows[index]; }
        }

        public string LuaPath
        {
            get { return luaPath ?? null; }
            set { luaPath = value; }
        }

        public string RdbPath
        {
            get { return rdbPath ?? null; }
            set { rdbPath = value; }
        }

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

        public string TableName
        {
            get { return lua.TableName; }
        }

        public SqlCommand InsertStatement
        {
            get { return generateInsert(); }
        }

        public bool UseSelectStatement
        {
            get { return lua.UseSelectStatement; }
        }

        public string SelectStatement
        {
            get { return lua.SelectStatement; }
        }

        public string[] ColumnNames
        {
            get
            {
                if (rows.Length == 0)
                    return null;

                return rows[0].ColumnNames;
            }
        }

        #endregion

        #region Constructors

        public Core() { }

        public Core(string luaPath, string rdbPath, Encoding encoding)
        {
            this.luaPath = luaPath;
            this.rdbPath = rdbPath;
            this.encoding = encoding;
        }

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

        public void Initialize()
        {
            lua = new LUA(FileIO.ReadAllText(luaPath));

            if (headerType == HeaderType.Defined)
                dHeader_Template = lua.GetFieldList(FieldsType.Header);

            row_Template = lua.GetFieldList("fields");
        }

        public void ParseBuffer(byte[] buffer)
        {
            sHelper = new StreamIO(buffer);
            parseHeader();
            parseContents();
        }

        public void Write()
        {
            if (sHelper == null) // If data was loaded from SQL
                sHelper = new StreamIO(encoding);
            else
                sHelper.Clear();

            writeHeader();
            writeContents();
        }

        public void SetEncoding(Encoding encoding)
        {
            this.encoding = encoding;
        }

        public void SetData(Row[] rows)
        {
            this.rows = rows;
        }

        #endregion

        #region Private methods

        private void parseHeader()
        {
            switch (headerType)
            {
                case HeaderType.Traditional:

                    tHeader.DateTime = sHelper.ReadString(8);
                    tHeader.Padding = sHelper.ReadBytes(120);
                    tHeader.RowCount = sHelper.ReadInt32;

                    break;

                case HeaderType.Defined:

                    Row row = new Row(dHeader_Template);
                    populateRow(ref row, SenderType.PARSE_HEADER);
                    dHeader = row;

                    break;
            }
        }

        private void parseContents()
        {
            rows = new Row[RowCount];

            OnProgressMaxChanged(new ProgressMaxArgs(RowCount));

            if (lua.SpecialCase)
            {
                switch (lua.Case)
                {
                    case SpecialCase.DOUBLE_LOOP:

                        for (int r = 0; r < RowCount; r++)
                        {
                            Row row = new Row(CellTemplate);
                            int loopCount = sHelper.ReadInt32;

                            for (int l = 0; l < loopCount; l++)
                            {
                                populateRow(ref row, SenderType.PARSE_ROW);

                                if (lua.UseRowProcessor)
                                    lua.CallRowProcessor(FileMode.Read, row, r);

                                rows[r] = row;

                                if ((r * 100 / RowCount) != ((r - 1) * 100 / RowCount))
                                    OnProgressValueChanged(new ProgressValueArgs(r));
                            }
                        }

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
                    sHelper.WriteBytes(ByteConverterExt.ToBytes(newDate, Encoding.Default));
                    sHelper.WriteString(" Daedalus 1.0", 120);
                    sHelper.WriteInt32(RowCount);
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

                        int previousVal = 0;

                        for (int r = 0; r < RowCount; r++)
                        {
                            Row row = rows[r];
                            int currentVal = row.GetValueByFlag(FlagType.LOOP_COUNTER) as int? ?? default(int);

                            if (previousVal != currentVal)
                            {
                                string counterName = row.GetNameByFlag(FlagType.LOOP_COUNTER);
                                int count = getMatchCount(counterName, currentVal);

                                sHelper.WriteInt32(count);

                                for (int i = 0; i < count; i++)
                                    writeRow(row, SenderType.WRITE_ROW);

                                previousVal = currentVal;
                            }

                            if (((r * 100) / RowCount) != ((r - 1) * 100 / RowCount))
                                OnProgressValueChanged(new ProgressValueArgs(r));
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

        int getMatchCount(string key, object value)
        {
            int c = 0;

            for (int r = 0; r < RowCount; r++)
            {
                Row row = rows[r];
                var v = row[key];

                if (v == value)
                    c++;
            }

            return c;
        }

        #region Common

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
                        row[c] = sHelper.ReadInt16;
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_UINT_16;

                    case CellType.TYPE_UINT_16:
                        row[c] = sHelper.ReadUInt16;
                        break;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32:
                        row[c] = sHelper.ReadInt32;
                        break;

                    case CellType.TYPE_UINT:
                        goto case CellType.TYPE_UINT_32;

                    case CellType.TYPE_UINT_32:
                        row[c] = sHelper.ReadUInt32;
                        break;

                    case CellType.TYPE_INT_64:
                        goto case CellType.TYPE_LONG;

                    case CellType.TYPE_LONG:
                        row[c] = sHelper.ReadInt64;
                        break;

                    //TODO: Implement TYPE_ULONG
                    //case CellType.TYPE_ULONG:
                    //    row[c] = sHelper.ReadInt64;
                    //    break;

                    case CellType.TYPE_BYTE:
                        if (cell.Length > 1)
                            row[c] = sHelper.ReadBytes(cell.Length);
                        else
                            row[c] = sHelper.ReadByte;
                        break;

                    case CellType.TYPE_BIT_VECTOR:
                        row[c] = new BitVector32(sHelper.ReadInt32);
                        break;

                    case CellType.TYPE_BIT_FROM_VECTOR:
                        {
                            int bitPos = cell.Position;
                            string dependency = cell.Dependency;
                            BitVector32 bitVector = (BitVector32)row[dependency];
                            row[c] = Convert.ToInt32(bitVector[1 << bitPos]);
                            break;
                        }

                    case CellType.TYPE_DECIMAL:
                        int v0 = sHelper.ReadInt32;
                        decimal v1 = v0 / 100m;
                        row[c] = v1;
                        break;

                    case CellType.TYPE_FLOAT:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_FLOAT_32:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_SINGLE:
                        row[c] = sHelper.ReadSingle;
                        break;

                    case CellType.TYPE_FLOAT_64:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_DOUBLE:
                        row[c] = sHelper.ReadDouble;
                        break;

                    case CellType.TYPE_SID:
                        row[c] = prevRowIdx;
                        prevRowIdx++;
                        break;

                    case CellType.TYPE_STRING:
                        {
                            row[c] = ByteConverterExt.ToString(sHelper.ReadBytes(cell.Length), 
                                                                            Encoding.Default);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_LEN:
                        {
                            string dependency = cell.Dependency;
                            int len = 0;
                            int.TryParse(row[dependency].ToString(), out len);

                            if (len < 0)
                                break;
                            else
                                row[c] = sHelper.ReadString(len);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_HEADER_REF:
                        {
                            string dependency = cell.Dependency;
                            int len = dHeader[dependency] as int? ?? default(int);
                            row[c] = sHelper.ReadString(len);
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
                            sHelper.WriteInt16(s);
                        }
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_UINT_16;

                    case CellType.TYPE_UINT_16:
                        {
                            ushort s = row[c] as ushort? ?? default(ushort);
                            sHelper.WriteUInt16(s);
                        }
                        break;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32:
                        {
                            int i = row[c] as int? ?? default(int);
                            sHelper.WriteInt32(i);
                        }
                        break;

                    case CellType.TYPE_UINT_32:
                        {
                            uint i = Convert.ToUInt32(row[c]);
                            sHelper.WriteUInt32(i);
                        }
                        break;

                    case CellType.TYPE_INT_64:
                        goto case CellType.TYPE_LONG;

                    case CellType.TYPE_LONG:
                        {
                            long l = row[c] as long? ?? default(long);
                            sHelper.WriteDouble(l);
                        }
                        break;

                    //TODO: Implement TYPE_ULONG
                    //case CellType.TYPE_ULONG:
                    //    break;

                    case CellType.TYPE_BYTE:
                        if (cell.Length > 0)
                        {
                            byte[] b = new byte[cell.Length];
                            sHelper.WriteBytes(b);
                        }
                        else
                        {
                            byte b = row[c] as byte? ?? default(byte);
                            sHelper.WriteByte(b);
                        }
                        break;

                    case CellType.TYPE_BIT_VECTOR:
                        {
                            int i = row[c] as int? ?? default(int);
                            sHelper.WriteInt32(i);
                        }
                        break;

                    case CellType.TYPE_DECIMAL:
                        //int v0 = sHelper.ReadInt32;
                        //decimal v1 = v0 / 100m;
                        //row[c] = v1;

                        decimal v0 = row[c] as decimal? ?? default(decimal);
                        int v1 = Convert.ToInt32(v0 * 100);
                        sHelper.WriteInt32(v1);
                        break;

                    case CellType.TYPE_FLOAT:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_FLOAT_32:
                        goto case CellType.TYPE_SINGLE;

                    case CellType.TYPE_SINGLE:
                        {
                            float s = row[c] as float? ?? default(float);
                            sHelper.WriteSingle(s);
                        }
                        break;

                    case CellType.TYPE_FLOAT_64:
                        goto case CellType.TYPE_DOUBLE;

                    case CellType.TYPE_DOUBLE:
                        {
                            double d = row[c] as double? ?? default(double);
                            sHelper.WriteDouble(d);
                        }
                        break;

                    case CellType.TYPE_STRING:
                        {
                            string s = row[c].ToString() + '\0';
                            sHelper.WriteString(s, s.Length);
                        }
                        break;

                    case CellType.TYPE_STRING_BY_LEN:
                        goto case CellType.TYPE_STRING;

                    case CellType.TYPE_STRING_BY_HEADER_REF:
                        {
                            byte[] buffer = ByteConverterExt.ToBytes(row[c].ToString(), Encoding.Default);
                            string refName = cell.Dependency;
                            int remainder = Convert.ToInt32(dHeader[refName]) - buffer.Length;
                            sHelper.WriteBytes(buffer);
                            sHelper.WriteBytes(new byte[remainder]);
                        }
                        break;

                    case CellType.TYPE_STRING_LEN:
                        {
                            string cellName = row.GetNameByDependency(cell.Name);
                            int valLen = row[cellName].ToString().Length + 1;
                            sHelper.WriteInt32(valLen);
                        }
                        break;
                }
            }
        }

        private SqlCommand generateInsert()
        {
            SqlCommand sqlCmd = new SqlCommand();
            string columns = string.Empty;
            string parameters = string.Empty;

            string[] names = (lua.UseSqlColumns) ? lua.SqlColumns : ColumnNames;
            int len = names.Length;

            OnProgressMaxChanged(new ProgressMaxArgs(len));

            for (int c = 0; c < len; c++)
            {
                string val = names[c];
                Cell cell = (Cell)rows[0][val];
                CellType columnType = cell.Type;

                columns += string.Format("{0}{1},", val, string.Empty);
                parameters += string.Format("@{0}{1},", val, string.Empty);
                SqlDbType paramType = SqlDbType.Int;

                switch (columnType)
                {
                    case CellType.TYPE_SHORT:
                        goto case CellType.TYPE_INT_16;

                    case CellType.TYPE_INT_16:
                        paramType = SqlDbType.SmallInt;
                        break;

                    case CellType.TYPE_USHORT:
                        goto case CellType.TYPE_INT_16;

                    case CellType.TYPE_UINT_16:
                        goto case CellType.TYPE_INT_16;

                    case CellType.TYPE_INT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_32:
                        paramType = SqlDbType.Int;
                        break;

                    case CellType.TYPE_UINT:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_UINT_32:
                        goto case CellType.TYPE_INT_32;

                    case CellType.TYPE_INT_64:
                        paramType = SqlDbType.BigInt;
                        break;

                    case CellType.TYPE_LONG:
                        goto case CellType.TYPE_INT_64;

                    case CellType.TYPE_BYTE:
                        paramType = SqlDbType.TinyInt;
                        break;

                    case CellType.TYPE_DATETIME:
                        paramType = SqlDbType.DateTime;
                        break;

                    case CellType.TYPE_DECIMAL:
                        paramType = SqlDbType.Decimal;
                        break;

                    case CellType.TYPE_SINGLE:
                        paramType = SqlDbType.Real;
                        break;

                    case CellType.TYPE_DOUBLE:
                        paramType = SqlDbType.Float;
                        break;

                    case CellType.TYPE_STRING:
                        paramType = SqlDbType.VarChar;
                        break;

                    case CellType.TYPE_STRING_BY_REF:
                        paramType = SqlDbType.VarChar;
                        break;
                }
                sqlCmd.Parameters.Add(val, paramType);

                if ((c * 100 / len) != ((c - 1) * 100 / len))
                    OnProgressValueChanged(new ProgressValueArgs(c));
            }      

            OnProgressValueChanged(new ProgressValueArgs(0));
            OnProgressMaxChanged(new ProgressMaxArgs(100));

            sqlCmd.CommandText = string.Format("INSERT INTO <tableName> ({0}) VALUES ({1})", columns.Remove(columns.Length - 1, 1), parameters.Remove(parameters.Length - 1, 1));
            return sqlCmd;
        }

        #endregion

        #endregion

        public void ClearData()
        {
            rows = new Row[0];
        }
    }
}
