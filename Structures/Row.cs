using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using Daedalus.Enums;

namespace Daedalus.Structures
{
    /// <summary>
    /// Storage medium for a 'row' of data loaded from an .rdb file
    /// </summary>
    public struct Row
    {
        /// <summary>
        /// Collection of cell descriptors and values.
        /// </summary>
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

        public int VisibleLength
        {
            get
            {
                int o = 0;

                foreach (Cell c in cells)
                    if (c.Visible)
                        o++;

                return o;
            }
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
        /// Cell property access, gets and sets the cell[key].Value
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

        /// <summary>
        /// Cell property accessor for cells[idx].Value
        /// </summary>
        /// <param name="flag">Flag enumeration of the cells[idx].Flag</param>
        /// <returns></returns>
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
        /// Collects all cells marked Visible and returns them as a Cell[]
        /// </summary>
        public Cell[] VisibleCells
        {
            get
            {
                List<Cell> vCells = new List<Cell>();

                foreach (Cell c in cells)
                    if (c.Visible)
                        vCells.Add(c);

                return vCells.ToArray();
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

        /// <summary>
        /// Gets the name of the parent field of a dependency field.
        /// </summary>
        /// <param name="dependency">Name of the dependency field in question</param>
        /// <returns>Name of the parent field</returns>
        public string GetNameByDependency(string dependency)
        {
            for (int i = 0; i < cells.Length; i++)
                if (cells[i].Dependency == dependency)
                    return cells[i].Name;

            return null;
        }

        /// <summary>
        /// Gets a fields value if that field is currently marked as visible
        /// </summary>
        /// <param name="key">Key (name) for the desired field</param>
        /// <returns>Shown value as object or null</returns>
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

        /// <summary>
        /// Gets all fields with a dependency of fieldName and type of BIT_FROM_VECTOR
        /// </summary>
        /// <param name="fieldName">Target bit_vector field</param>
        /// <returns></returns>
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

        /// <summary>
        /// Gets the BitVector32 representation of an int value by field name
        /// </summary>
        /// <param name="name">Name of the target cell</param>
        /// <returns>Field value as a BitVector32</returns>
        public BitVector32 GetBitVector(string name)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                if (cells[i].Name == name && cells[i].Type == CellType.TYPE_BIT_VECTOR)
                    return cells[i].Value as BitVector32? ?? default(BitVector32);
            }

            return new BitVector32(0);
        }

        /// <summary>
        /// Gets all fields with BIT_FROM_VECTOR and assigned dependency and returns them as a Cell[]
        /// </summary>
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

        /// <summary>
        /// Gets the name  of all cells in the collection and returns them as a string[]
        /// </summary>
        public string[] CellNames
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

        public string[] VisibleNames
        {
            get
            {
                if (cells.Length == 0)
                    return null;

                List<string> names = new List<string>(VisibleLength);

                for (int i = 0; i < cells.Length; i++)
                {
                    if (cells[i].Visible)
                        names.Add(cells[i].Name);
                }

                return names.ToArray();
            }
        }

        /// <summary>
        /// Determines if more than one cell with the key exists
        /// </summary>
        /// <param name="key">Target cell name</param>
        /// <returns>True or false</returns>
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

        /// <summary>
        /// Makes a reference independent clone of the the cells collection by writing existing values into a referenced Row
        /// </summary>
        /// <param name="output">Row to have contents copied too</param>
        public void Clone(ref Row output)
        {
            if (cells == null)
                return;

            for (int i = 0; i < output.Length; i++)
                output[i] = cells[i].Value;
        }

        /// <summary>
        /// Amount of fields with the type BIT_FROM_VECTOR
        /// </summary>
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
