using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFastConversion
{
    public class binaram2HTMLsetting
    {
        public int rowThread = 100;
    }

    public class binaram2HTMLdataFlow
    {
        public StringBuilder binaram2HTMLMultithread(binaram currentInput, binaram2HTMLsetting currentSetting)
        {
            ConcurrentDictionary<int, StringBuilder> htmlStringMultithread = new ConcurrentDictionary<int, StringBuilder>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentDictionary<int, binaram2CSVdataFlow> concurrentRowSegment = new ConcurrentDictionary<int, binaram2CSVdataFlow>();
            List<int> rowSegment = new List<int>();
            StringBuilder htmlString = new StringBuilder();
            List<string> htmlColumnName = new List<string>();                       

            htmlString.Append("<table class=\"table\">" + Environment.NewLine);
            htmlString.Append("    <thead>" + Environment.NewLine);
            htmlString.Append("      <tr>" + Environment.NewLine);

            for (int x = 0; x < currentInput.columnName.Count; x++)
                htmlString.Append("        <th>" + currentInput.columnName[x] + "</th>" + Environment.NewLine);

            htmlString.Append("      </tr>" + Environment.NewLine);
            htmlString.Append("    </thead>" + Environment.NewLine);
            htmlString.Append("    <tbody>" + Environment.NewLine);

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
                htmlStringMultithread[currentSegment] = binaram2HTML(rowSegment, currentSegment, checkSegmentThreadCompleted, htmlColumnName, currentInput, currentSetting);
            });

            do
            {
                Thread.Sleep(10);
            } while (checkSegmentThreadCompleted.Count < rowSegment.Count - 1);

            for (int i = 0; i < rowSegment.Count - 1; i++)
                htmlString.Append(htmlStringMultithread[i]);
           
            htmlString.Append("    </tbody>" + Environment.NewLine);
            htmlString.Append("</table>" + Environment.NewLine);           
            
            return htmlString;
        }

        public StringBuilder binaram2HTML(List<int> rowSegment, int currentSegment, ConcurrentQueue<int> checkSegmentThreadCompleted, List<string> htmlColumnName, binaram currentInput, binaram2HTMLsetting currentSetting)
        {
            StringBuilder htmlString = new StringBuilder();           
            int maxColumn = currentInput.columnName.Count;

            for (int y = rowSegment[currentSegment]; y < rowSegment[currentSegment + 1]; y++)
            {
                htmlString.Append("        <tr>" + Environment.NewLine);               

                for (int x = 0; x < maxColumn; x++)
                {
                    if (currentInput.dataType[x] == "Number")
                        htmlString.Append("          <td>" + currentInput.factTable[x][y] + "</td>" + Environment.NewLine);                   
                    else
                        htmlString.Append("          <td>" + currentInput.key2Value[x][currentInput.factTable[x][y]] + "</td>" + Environment.NewLine);                    
                }
                htmlString.Append("        </tr>" + Environment.NewLine);
            }

            checkSegmentThreadCompleted.Enqueue(currentSegment);

            return htmlString;
        }
    }
}
