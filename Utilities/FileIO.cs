using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Daedalus.Utilities
{
    public static class FileIO
    {
        public static byte[] ReadAllBytes(string path)
        {
            if (File.Exists(path))
                return File.ReadAllBytes(path);
            else
                throw new FileNotFoundException(string.Format("File not found at path: {0}", path));
        }

        public static string ReadAllText(string path)
        {
            try
            {
                if (File.Exists(path))
                    return File.ReadAllText(path);
            }
            catch (Exception ex)
            { } //Send to calling program

            return null;
        }
    }
}
