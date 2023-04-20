using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class binaram2XMLsetting
    {
        public int rowThread = 100;
        public string tableName { get; set; }
    }  

    public class binaram2XMLdataFlow
    {
        public StringBuilder binaram2XMLMultithread(binaram currentInput, binaram2XMLsetting currentSetting)
        {
            ConcurrentDictionary<int, StringBuilder> xmlStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, binaram2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, binaram2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder xmlString = new StringBuilder();
            List<string>  xmlColumnName = new List<string>();
            string tempColumnName;

            for (int x = 0; x < currentInput.columnName.Count; x++)
            {
                tempColumnName = currentInput.columnName[x].Replace(" ", "_x0020_");
                tempColumnName = tempColumnName.Replace("/", "_x002F_");
                xmlColumnName.Add(tempColumnName);
            }

            xmlString.Append("<?xml version=\"1.0\" standalone=\"yes\"?>" + Environment.NewLine);
            xmlString.Append("<NewDataSet>" + Environment.NewLine);

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
                xmlStringMultithread[currentSegment] = binaram2XML(rowSegment, currentSegment, checkSegmentThreadCompleted, xmlColumnName, currentInput, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                xmlString.Append(xmlStringMultithread[i]);
            
            xmlString.Append("</NewDataSet>" + Environment.NewLine);           
           
            return xmlString;
        }

        public StringBuilder binaram2XML(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> xmlColumnName, binaram currentInput, binaram2XMLsetting currentSetting)
        {
            StringBuilder xmlString = new StringBuilder();
            int maxRow = currentInput.factTable[0].Count;
            int maxColumn = currentInput.columnName.Count;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                xmlString.Append("   <" + currentSetting.tableName + ">" + Environment.NewLine);                

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentInput.dataType[x] == "Number")
                        xmlString.Append("     <" + xmlColumnName[x] + ">" + currentInput.factTable[x][y] + "</" + xmlColumnName[x] + ">"  + Environment.NewLine);                                         
                    else
                        xmlString.Append("     <" + xmlColumnName[x] + ">" + currentInput.key2Value[x][currentInput.factTable[x][y]] + "</" + xmlColumnName[x] + ">" + Environment.NewLine);
                }
                xmlString.Append("   </" + currentSetting.tableName + ">" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return xmlString;
        }
    }
}
