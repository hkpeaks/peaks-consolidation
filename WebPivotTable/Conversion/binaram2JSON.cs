using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class binaram2JSONsetting
    {
        public int rowThread = 100; 
        public string tableName { get; set; }
    }

    public class binaram2JSONoutput
    {
        public StringBuilder jsonString { get; set; }
    }

    public class binaram2JSONdataFlow
    {
        public StringBuilder binaram2JSONMultithread(binaram currentInput, binaram2JSONsetting currentSetting)
        {
            ConcurrentDictionary<int, StringBuilder> jsonStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, binaram2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, binaram2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder jsonString = new StringBuilder();            
            jsonString.Append("{" + Environment.NewLine);
            jsonString.Append("    \"" + currentSetting.tableName + "\": [" + Environment.NewLine);

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
               jsonStringMultithread[currentSegment] = binaram2JSON(rowSegment, currentSegment, checkSegmentThreadCompleted, currentInput, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                jsonString.Append(jsonStringMultithread[i]);

            jsonString.Append("  ]" + Environment.NewLine);
            jsonString.Append("}" + Environment.NewLine);                           
           
            return jsonString;
        }

        public StringBuilder binaram2JSON(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, binaram currentInput, binaram2JSONsetting currentSetting)
        {
            StringBuilder jsonString = new StringBuilder();
            int maxRow = currentInput.factTable[0].Count;
            int maxColumn = currentInput.columnName.Count;           

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                jsonString.Append("    {" + Environment.NewLine);
                jsonString.Append("     \"id\": " + y + "," + Environment.NewLine);

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentInput.dataType[x] == "Number")
                    {
                        if (x != maxColumn - 1)
                            jsonString.Append("     \"" + currentInput.columnName[x] + "\": " + currentInput.factTable[x][y] + "," + Environment.NewLine);
                        else
                            jsonString.Append("     \"" + currentInput.columnName[x] + "\": " + currentInput.factTable[x][y] + Environment.NewLine);
                    }
                    else
                    {
                        if (x != maxColumn - 1)
                            jsonString.Append("     \"" + currentInput.columnName[x] + "\": " + "\"" + currentInput.key2Value[x][currentInput.factTable[x][y]] + "\"," + Environment.NewLine);
                        else
                            jsonString.Append("     \"" + currentInput.columnName[x] + "\": " + "\"" + currentInput.key2Value[x][currentInput.factTable[x][y]] + "\"" + Environment.NewLine);
                    }
                }
                if (y != maxRow - 1)
                    jsonString.Append("    }," + Environment.NewLine);
                else
                    jsonString.Append("    }" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return jsonString;
        }
    }
}
