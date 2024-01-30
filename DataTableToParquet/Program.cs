using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Parquet.Schema;
using Array = System.Array;
using DataColumn = System.Data.DataColumn;
using DBNull = System.DBNull;

namespace DataTableToParquet
{
    public class Program
    {
        // The size of a row group, this is pretty low but is simply for demonstration
        private const int RowGroupSize = 100;

        private const string OutputFilePath = "example.parquet";

        public static async Task Main(string[] args)
        {
            var dt = GenerateTestData();
            await ExportToParquet(dt, OutputFilePath);
        }

        private static async Task ExportToParquet(DataTable dataTable, string filePath)
        {
            DataField[] fields = GetSchema(dataTable);
            ParquetSchema paquetSchema = new ParquetSchema(fields);
            using (Stream fileStream = System.IO.File.OpenWrite(filePath))
            {
                using (ParquetWriter parquetWriter =
                       await ParquetWriter.CreateAsync(paquetSchema, fileStream).ConfigureAwait(false))
                {
                    parquetWriter.CompressionMethod = CompressionMethod.Gzip;
                    parquetWriter.CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;

                    var startRow = 0;
                    // Keep on creating row groups until we run out of data
                    while (startRow < dataTable.Rows.Count)
                    {
                        using (var rgw = parquetWriter.CreateRowGroup())
                        {
                            // Data is written to the row group column by column
                            for (var i = 0; i < dataTable.Columns.Count; i++)
                            {
                                var columnIndex = i;

                                // Determine the target data type for the column
                                var targetType = dataTable.Columns[columnIndex].DataType;
                                if (targetType == typeof(DateTime)) targetType = typeof(DateTime);

                                // Generate the value type, this is to ensure it can handle null values
                                var valueType = targetType.IsClass
                                    ? targetType
                                    : typeof(Nullable<>).MakeGenericType(targetType);

                                // Create a list to hold values of the required type for the column
                                IList list = (IList)typeof(List<>)
                                    .MakeGenericType(valueType)
                                    .GetConstructor(Type.EmptyTypes)!
                                    .Invoke(null);

                                // Get the data to be written to the parquet stream
                                foreach (var row in dataTable.AsEnumerable())
                                {
                                    // Check if value is null, if so then add a null value
                                    if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
                                    {
                                        list.Add(null);
                                    }
                                    else
                                    {
                                        // Add the value to the list, but if it's a DateTime then create it as a DateTime
                                        list.Add(dataTable.Columns[columnIndex].DataType == typeof(DateTime)
                                            ? (DateTime)row[columnIndex]
                                            : row[columnIndex]);
                                    }
                                }

                                // Copy the list values to an array of the same type as the WriteColumn method expects
                                // and Array
                                Array valuesArray = Array.CreateInstance(valueType, list.Count);
                                list.CopyTo(valuesArray, 0);

                                // Write the column
                                await rgw.WriteColumnAsync(new Parquet.Data.DataColumn(paquetSchema.DataFields[i],
                                    valuesArray)).ConfigureAwait(false);
                            }
                        }

                        startRow += RowGroupSize;
                    }
                }
            }
        }

        public static DataField[] GetSchema(DataTable dataTable)
        {
            var fields = new List<DataField>();
            foreach (DataColumn column in dataTable.Columns)
            {
                if (column.DataType == typeof(byte[]))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(string),true));
                }
                else if(column.DataType== typeof(int))
                {
                    // nullable int
                    fields.Add(new DataField(column.ColumnName, typeof(int),true));
                }
                else if (column.DataType == typeof(string))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(string),true));
                }
                else if (column.DataType == typeof(double))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(double),true));
                }
                else if (column.DataType == typeof(bool))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(bool),true));
                }
                else if (column.DataType == typeof(Int32))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(Int32),true));
                }
                else if (column.DataType == typeof(Int64))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(Int64),true));
                }
                else if (column.DataType == typeof(DateTime))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(DateTime),true));
                }
                // null
                else if (column.DataType == typeof(DBNull))
                {
                    fields.Add(new DataField(column.ColumnName, typeof(string),true));
                }

            }

            return fields.ToArray();
        }

        /// <summary>
        /// Generates a test data set
        /// </summary>
        /// <returns></returns>
        private static DataTable GenerateTestData()
        {
            var dt = new DataTable("demo");
            dt.Columns.AddRange(new[]
            {
                new DataColumn("id", typeof(int)),
                new DataColumn("name", typeof(string)),
                new DataColumn("age", typeof(int)),
                new DataColumn("lastseen", typeof(DateTime)),
                new DataColumn("score", typeof(double))
            });

            var rand = new Random();

            var forenames = new List<string>
            {
                "Celaena", "Gabe", "Rose", "Chris", "Girton", "Nick", "Aiana", "Julia", "Curtis", "Manuela", "Spike"
            };
            var surnames = new List<string> { "Smith", "Curts", "Power", "Stark", "Robinson", "Askew", "Maximus" };
            var ages = new List<int?> { 19, 23, 47, 71, null, 27 };
            var lastSeen = new List<DateTime>
                { new DateTime(2018, 9, 14), new DateTime(2019, 7, 13), new DateTime(1418, 5, 21) };
            var scores = Enumerable.Range(1, 10).Select(_ => rand.NextDouble()).ToList();

            var sampleData = from f in forenames
                from s in surnames
                from a in ages
                from ls in lastSeen
                from sc in scores
                select new
                {
                    Name = $"{f} {s}",
                    Age = a,
                    LastSeen = ls,
                    Score = sc
                };

            var index = 1;

            foreach (var item in sampleData)
            {
                dt.Rows.Add(dt.NewRow().ItemArray = new object[]
                    { index++, item.Name, item.Age, item.LastSeen, item.Score });
            }

            return dt;
        }
    }
}