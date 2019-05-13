using System;
using System.Collections.Generic;
using System.Text;
using Daedalus.Enums;

namespace Daedalus.Structures
{
    public struct Row
    {
        readonly Cell[] cells;

        public Row(Cell[] cells) { this.cells = cells; }

        public int Length
        {
            get { return cells.Length; }
        }

        public object this[int idx]
        {
            get { return cells[idx].Value; }
            set { cells[idx].Value = value; }
        }

        public object this[string key]
        {
            get
            {
                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Name == key)
                        return cells[i].Value;
                }

                return null;
            }
            set
            {
                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Name == key)
                        cells[i].Value = value;
                }
            }
        }

        public Cell GetCell(int index)
        {
            return cells[index];
        }

        public Cell GetCell(string key)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Name == key)
                    return cells[i];
            }

            throw new Exception(string.Format("Cell with key: {0} does not exist in cells!", key));
        }

        public object GetValueByFlag(FlagType flag)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Flag == flag)
                    return cells[i].Value;
            }

            return null;
        }

        public object GetValueByCellType(CellType type)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Type == type)
                    return cells[i].Value;
            }

            return null;
        }

        public string GetNameByFlag(FlagType flag)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Flag == flag)
                    return cells[i].Value.ToString();
            }

            return null;
        }

        public string GetNameByDependency(string dependency)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Dependency == dependency)
                    return cells[i].Name;
            }

            return null;
        }

        public object GetShownValue(string key)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                Cell c = cells[i];
                if (c.Name == key && c.Visible)
                    return c.Value;
            }

            return null;
        }

        public string[] ColumnNames
        {
            get
            {
                if (cells.Length == 0)
                    return null;

                string[] names = new string[cells.Length];

                for (int i = 0; i < cells.Length; i++)
                    names[i] = cells[i].Name;

                return names;
            }
        }

        public bool KeyIsDuplicate(string key)
        {
            int c = 0;

            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Name == key)
                    c++;
            }

            return c > 1;
        }

        public void Clone(ref Row output)
        {
            if (cells == null)
                return;

            for (int i = 0; i < output.Length; i++)
                output[i] = cells[i].Value;
        }

    }
}
