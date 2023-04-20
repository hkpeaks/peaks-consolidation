using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class dataTable2BinaramSetting
    {    
        public int columnThread = 100;        
    }   
    public class dataTable2BinaramdataFlow
    {
        public binaram dataTable2BinaramMultithread(DataTable currentInput, dataTable2BinaramSetting currentSetting)
        {
            binaram currentOutput = new binaram();
            Dictionary<int, string> dataType = new Dictionary<int, string>();
            Dictionary<int, string> columnName = new Dictionary<int, string>();
            Dictionary<int, List<double>> factTable = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> key2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> value2Key = new Dictionary<int, Dictionary<string, double>>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, csv2BinaramDataFlow> writeColumnThread = new ConcurrentDictionary<int, csv2BinaramDataFlow>();

            for (int x = 0; x < currentInput.Columns.Count; x++)
            {
                columnName.Add(x, currentInput.Columns[x].ColumnName);

                if (currentInput.Columns[x].DataType == Type.GetType("System.String"))
                    dataType.Add(x, "Text");
                else
                    dataType.Add(x, "Number");

            }

            for (int x = 0; x < currentInput.Columns.Count; x++)
            {
                if (columnName[x].ToUpper().Contains("DATE"))
                    dataType[x] = "Date";
            } 

            for (int i = 0; i < dataType.Count; i++)
            {
                factTable.Add(i, new List<double>());
                key2Value.Add(i, new Dictionary<double, string>());
                value2Key.Add(i, new Dictionary<string, double>());
            }

            for (int worker = 0; worker < dataType.Count; worker++)
                writeColumnThread.TryAdd(worker, new csv2BinaramDataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.columnThread
            };

            Parallel.For(0, dataType.Count, options, x =>
            {
                if (dataType[x] == "Number")
                {
                    factTable[x] = createBinaramMeasure(x, checkThreadCompleted, currentInput);

                    if (factTable[x].Count == 0)
                        (factTable[x], key2Value[x], value2Key[x]) = createBinaramDimensionKey(x, checkThreadCompleted, currentInput);
                }
                else
                    (factTable[x], key2Value[x], value2Key[x]) = createBinaramDimensionKey(x, checkThreadCompleted, currentInput);
            });

            do
            {
                Thread.Sleep(2);

            } while (checkThreadCompleted.Count < dataType.Count);            

            currentOutput.factTable = new Dictionary<int, List<double>>(factTable);
            currentOutput.key2Value = new Dictionary<int, Dictionary<double, string>>(key2Value);
            currentOutput.value2Key = new Dictionary<int, Dictionary<string, double>>(value2Key);
            currentOutput.columnName = columnName;
            currentOutput.dataType = dataType;

            return currentOutput;
        }

        public (List<double> factTable, Dictionary<double, string> key2Value, Dictionary<string, double> value2Key) createBinaramDimensionKey(int columnID, ConcurrentQueue<int> checkThreadCompleted, DataTable currentInput)
        {
            List<double> factTable = new List<double>();
            StringBuilder cellValue = new StringBuilder();
            factTable.Add(columnID); // first record is column id              
            double count;
            Dictionary<double, string> key2Value = new Dictionary<double, string>();
            Dictionary<string, double> value2Key = new Dictionary<string, double>();

            for (int y = 0; y < currentInput.Rows.Count; y++)
            {
                string text = currentInput.Rows[y].Field<string>(columnID);

                if (text.Length == 0)
                    cellValue.Append("null");

                if (value2Key.ContainsKey(text)) // same master record
                    factTable.Add(value2Key[text]);

                else // add new master record
                {
                    count = value2Key.Count;
                    key2Value.Add(count, text);
                    value2Key.Add(text, count);
                    factTable.Add(count);
                }
            }

            checkThreadCompleted.Enqueue(columnID);
            return (factTable, key2Value, value2Key);
        }
        public List<double> createBinaramMeasure(int columnID, ConcurrentQueue<int> checkThreadCompleted, DataTable currentInput)
        {
            List<double> factTable = new List<double>();            
            factTable.Add(columnID); // first record is column id    

            for (int y = 0; y < currentInput.Rows.Count; y++)
            {
                factTable.Add(currentInput.Rows[y].Field<double>(columnID));
            }
            checkThreadCompleted.Enqueue(columnID);
            return factTable;
        }     
    }
}
