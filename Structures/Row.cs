using System;
using System.Collections.Specialized;
using System.Text;
using Daedalus.Enums;

namespace Daedalus.Structures
{
    /// <summary>
    /// Storage medium for a 'row' of data loaded from an .rdb file
    /// </summary>
    public struct Row
    {
        readonly Cell[] cells;

        /// <summary>
        /// Instantiate the Row with provided cells
        /// </summary>
        /// <param name="cells">template containing a per cell schematic (MAY contain values)</param>
        public Row(Cell[] cells) { this.cells = cells; }

        /// <summary>
        /// Length of the cells collection
        /// </summary>
        public int Length
        {
            get { return cells.Length; }
        }

        /// <summary>
        /// Cell property access, gets and sets the cells[idx].Value
        /// </summary>
        /// <param name="idx">Index of the desired cell element</param>
        /// <returns>cells[idx].Value</returns>
        public object this[int idx]
        {
            get { return cells[idx].Value; }
            set { cells[idx].Value = value; }
        }

        /// <summary>
        /// CEll property access, gets and sets the cell[key].Value
        /// </summary>
        /// <param name="key">Strongly Typed name of the desired cell element</param>
        /// <returns>cells[key].Value</returns>
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

        public object this[FlagType flag]
        {
            get
            {
                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Flag == flag)
                        return cells[i].Value;
                }

                return null;
            }
            set
            {
                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Flag == flag)
                        cells[i].Value = value;
                }
            }
        }

        /// <summary>
        /// Get a cell from the cells[] collection by its ordinal position.
        /// </summary>
        /// <param name="index">Index of the desired cell element</param>
        /// <returns>Cell at given index</returns>
        public Cell GetCell(int index)
        {
            return cells[index];
        }

        /// <summary>
        /// Gets a cell from the cells[] collection by its key<->name value
        /// </summary>
        /// <param name="key">Name of the desired cell</param>
        /// <returns>First Cell elementing with Name == key</returns>
        public Cell GetCell(string key)
        {
            if (key == null)
                throw new Exception("key is null!");

            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Name == key)
                    return cells[i];
            }

            throw new Exception(string.Format("Cell with key: {0} does not exist in cells!", key));
        }

        /// <summary>
        /// Gets the first Cell.Value object bearing the given flag
        /// </summary>
        /// <param name="flag">Desired FlagType</param>
        /// <returns>Object representing Cell.Value</returns>
        public object GetValueByFlag(FlagType flag)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Flag == flag)
                    return cells[i].Value;
            }

            return null;
        }

        /// <summary>
        /// Gets the first Cell.Value object bearing the given type
        /// </summary>
        /// <param name="type">Desired CellType</param>
        /// <returns>Object representing Cell.Value</returns>
        public object GetValueByCellType(CellType type)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Type == type)
                    return cells[i].Value;
            }

            return null;
        }

        /// <summary>
        /// Gets the first Cell.Name string whose element bears the given flag
        /// </summary>
        /// <param name="flag">Desired FlagType</param>
        /// <returns>String representing the desired Cell.Name</returns>
        public string GetNameByFlag(FlagType flag)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Flag == flag)
                    return cells[i].Name;
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

        public Cell[] GetBitFields(string fieldName)
        {
            Cell[] fields = new Cell[bitField_length];
            int fPos = 0;

            for (int i = 0; i < cells.Length; i++)
            {
                Cell c = cells[i];

                if (c.Dependency == fieldName)
                {
                    fields[fPos] = c;
                    fPos++;
                }
            }

            return fields;
        }

        public BitVector32 GetBitVector(string name)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Type == CellType.TYPE_BIT_VECTOR)
                    return cells[i].Value as BitVector32? ?? default(BitVector32);
            }

            return new BitVector32(0);
        }

        public Cell[] BitFields
        {
            get
            {
                Cell[] fields = new Cell[bitField_length];
                int fPos = 0;

                if (fields.Length == 0)
                    return null;

                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Type == CellType.TYPE_BIT_FROM_VECTOR)
                    {
                        fields[fPos] = cells[i];
                        fPos++;
                    }
                }

                return fields;
            }
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

        int bitField_length
        {
            get
            {
                int o = 0;

                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Type == CellType.TYPE_BIT_FROM_VECTOR)
                        o++;
                }

                return o;
            }
        }

    }
}
