using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class binaram2CSVsetting
    {
        public string separator = ",";
        public int rowThread = 100;       
    }    

    public class binaram2CSVdataFlow
    {
        public StringBuilder binaram2CSVMultithread(binaram currentInput, binaram2CSVsetting currentSetting)
        {
            ConcurrentDictionary<int, StringBuilder> csvStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, binaram2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, binaram2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder csvString = new StringBuilder();
            string separator = currentSetting.separator;    

            for (int x = 0; x < currentInput.columnName.Count; x++)
            {
                if (x > 0) csvString.Append(separator);

                if (currentInput.columnName[x].Contains(separator))
                    csvString.Append((char)34 + currentInput.columnName[x] + (char)34);
                else
                    csvString.Append(currentInput.columnName[x]);
            }
            csvString.Append(Environment.NewLine);

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
                 csvStringMultithread[currentSegment] = binaram2CSV(rowSegment, currentSegment, checkSegmentThreadCompleted, currentInput, currentSetting);
            });           

            do
            {
                Thread.Sleep(10);             
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);            

            for (int i = 0; i < rowSegment.Count - 1; i++)
                csvString.Append(csvStringMultithread[i]);            

            return csvString;
        }

        public StringBuilder binaram2CSV(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, binaram currentInput, binaram2CSVsetting currentSetting)
        {
            StringBuilder csvString = new StringBuilder();
            string separator = currentSetting.separator;
            string currentCell;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {              
                for (int x = 0; x < currentInput.columnName.Count; x++)
                {
                    if (currentInput.dataType[x] == "Number")
                        currentCell = currentInput.factTable[x][y].ToString();
                    else               
                        currentCell = currentInput.key2Value[x][currentInput.factTable[x][y]].ToString();                  

                    if (x > 0) csvString.Append(separator);

                    if (currentCell.Contains(separator))
                        csvString.Append((char)34 + currentCell + (char)34);
                    else
                        csvString.Append(currentCell);
                }
                csvString.Append(Environment.NewLine);
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return csvString;
        }
    }
}
