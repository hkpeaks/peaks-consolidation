using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class binaram2DataTablesetting
    {       
        public int rowThread = 100;     
    }

    public class binaram2DataTabledataFlow
    {
        public DataTable binaram2DataTableMultithread(binaram currentInput, binaram2DataTablesetting currentSetting)
        {
            ConcurrentDictionary<int, DataTable> dtMultithread = new ConcurrentDictionary<int, DataTable>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, binaram2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, binaram2CSVdataFlow>();
            DataTable currentOutput = new DataTable();            

            List<int> rowSegment = new List<int>();
            StringBuilder dataTableString = new StringBuilder();
            List<string> dataTableColumnName = new List<string>();

            rowSegment.Add(1);
            if (currentInput.factTable[0].Count > 1000)
            {
                int rowSegmentLength = Convert.ToInt32(Math.Round((double)((currentInput.factTable[0].Count - 1) / currentSetting.rowThread), 0));

                for (int y = 1; y < currentSetting.rowThread; y++)
                    rowSegment.Add(rowSegmentLength * y);

                rowSegment.Add(currentInput.factTable[0].Count);
            }
            else
            {
                rowSegment.Add(currentInput.factTable[0].Count);
            }          

            for (int worker = 0; worker < rowSegment.Count - 1; worker++) concurrentRowSegment.TryAdd(worker, new binaram2CSVdataFlow());

            var options = new ParallelOptions()
            {
                MaxDegreeOfParallelism = currentSetting.rowThread
            };            

            Parallel.For(0, rowSegment.Count - 1, options, currentSegment =>
            {              
                dtMultithread[currentSegment] = binaram2DataTable(rowSegment, currentSegment, checkSegmentThreadCompleted, dataTableColumnName, currentInput, currentSetting);               
            });         

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);
           

            DataTable dt = new DataTable();

            for (int x = 0; x < currentInput.columnName.Count; x++)
            {
                if (currentInput.dataType[x] == "Number")
                    dt.Columns.Add(currentInput.columnName[x], typeof(double));
                else
                    dt.Columns.Add(currentInput.columnName[x], typeof(string));
            }

            for (int i = 0; i < rowSegment.Count - 1; i++)
               dt.Merge(dtMultithread[i]);

            currentOutput = dt;

            return currentOutput;
        }

        public DataTable binaram2DataTable(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> dataTableColumnName, binaram currentInput, binaram2DataTablesetting currentSetting)
        {  
            int maxColumn = currentInput.columnName.Count;
            DataTable dt = new DataTable();

            for (int x = 0; x < currentInput.columnName.Count; x++)
            {
                if (currentInput.dataType[x] == "Number")
                    dt.Columns.Add(currentInput.columnName[x], typeof(double));
                else
                    dt.Columns.Add(currentInput.columnName[x], typeof(string));
            }            

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                DataRow dr = dt.NewRow();
                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentInput.dataType[x] == "Number")
                        dr[x] = currentInput.factTable[x][y];
                    else
                        dr[x] = currentInput.key2Value[x][currentInput.factTable[x][y]];                  
                }
                dt.Rows.Add(dr);
            }         

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return dt;
        }
    }
}
