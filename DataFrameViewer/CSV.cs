using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace youFast
{
    public class Import
    {
        public (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) CSV2ramDetail(string[] readFile, byte csvReadSeparator)
        {           
            ConcurrentDictionary<int, List<double>> ramDetail = new ConcurrentDictionary<int, List<double>>();
            ConcurrentDictionary<int, Dictionary<double, string>> remK2V = new ConcurrentDictionary<int, Dictionary<double, string>>();
            ConcurrentDictionary<int, Dictionary<string, double>> ramV2K = new ConcurrentDictionary<int, Dictionary<string, double>>();
            Dictionary<string, int> csvInfo = new Dictionary<string, int>();
            ConcurrentDictionary<int, bool> isNumType = new ConcurrentDictionary<int, bool>();            
            Dictionary<int, List<int>> byteAddress = new Dictionary<int, List<int>>();
            ConcurrentDictionary<decimal, Thread> writeColumn = new ConcurrentDictionary<decimal, Thread>();
            ConcurrentQueue<int> checkThreadCompleted = new ConcurrentQueue<int>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            List<int> abc123Segment = new List<int>();
            StringBuilder cellValue = new StringBuilder();
            StringBuilder filePath = new StringBuilder();
            List<int> nextRow = new List<int>();
            List<string> stringColumn = new List<string>();
            
            byte separator = csvReadSeparator;
            int openCloseDoubleQuote = 1;
            bool useDoubleQuote = false;
            int column = 0;
            int maxColumn = 0;
            int nextRowChar = 0;            

            foreach (string value in readFile) 
                filePath.Append(value);

            var start = DateTime.Now;
            byte[] abc123 = File.ReadAllBytes(filePath.ToString());
            var end = DateTime.Now;
            Console.WriteLine("read byte Time : " + String.Format("{0:F3}", (end - start).TotalSeconds) + " seconds");

            int csvLength = abc123.Length;          

            if (abc123[csvLength - 2] != 13 && abc123[csvLength - 1] != 10) // check exist of 10 and/or 13 at the end of file
            {                
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 13;
                csvLength = csvLength + 1;
                Array.Resize(ref abc123, csvLength);
                abc123[abc123.GetUpperBound(0)] = 10;
            }

            int s = 0;
            do // determine max number of column
            {
                if (abc123[s] == 34) // space char
                {
                    useDoubleQuote = true;
                    openCloseDoubleQuote = openCloseDoubleQuote * -1;
                }

                if (openCloseDoubleQuote == 1)
                    separator = csvReadSeparator;
                else
                    separator = 127; // delete key     

                if (abc123[s] == separator)               
                    column++;
                s++;
            } while (!(abc123[s] == 10 || abc123[s] == 13) && s < csvLength);

            if (!nextRow.Contains(abc123[s]))           
                nextRow.Add(abc123[s]);

            if (abc123[s+1] == 10 || abc123[s+1] == 13)
                if (!nextRow.Contains(abc123[s+1]))
                   nextRow.Add(abc123[s+1]);

            maxColumn = column + 1;

            for (int i = 0; i <= maxColumn; i++)           
                byteAddress.Add(i, new List<int>());

            column = 1;
            nextRowChar = nextRow.Count; // use double quote to include separator     

            if(csvLength > 10000)
            { 
                Import multiSegment = new Import();
                multiSegment.multiThreadBySegmentAddress(csvLength, abc123Segment, checkSegmentThreadCompleted, byteAddress, abc123, nextRow, useDoubleQuote, openCloseDoubleQuote, column, maxColumn, nextRowChar, separator, csvReadSeparator);                                                         
            }
            else
            {
                abc123Segment.Add(0);
                abc123Segment.Add(csvLength); 
                Import singleSegment = new Import();
                byteAddress = singleSegment.findAddress(checkSegmentThreadCompleted, 0, abc123Segment, useDoubleQuote, abc123, column, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote);
            }          

            byteAddress[0].RemoveAt(byteAddress[0].Count - 1);

            stringColumn.Add("DATE"); stringColumn.Add("YEAR"); stringColumn.Add("PERIOD"); stringColumn.Add("ACCOUNT");

            for (int i = 0; i < maxColumn; i++) // add column name
            {
                ramDetail.TryAdd(i, new List<double>());
                remK2V.TryAdd(i, new Dictionary<double, string>());
                ramV2K.TryAdd(i, new Dictionary<string, double>());

                cellValue.Clear();

                for (int j = byteAddress[i][0]; j < byteAddress[i + 1][0] - 1; j++)
                    cellValue.Append((char)abc123[j]);

                string columnName = cellValue.ToString().ToUpper();
                remK2V[i].Add(0, cellValue.ToString().Trim());
                ramV2K[i].Add(cellValue.ToString().Trim(), 0);
                ramDetail[i].Add(0);

                for (int k = 0; k < stringColumn.Count; k++)
                    if (columnName.Contains(stringColumn[k]))
                        isNumType.TryAdd(i, false);
            }

            Parallel.For(0, maxColumn, x =>
            {
                try // new a thread to manage queue job
                {
                    Import next = new Import();
                    writeColumn.TryAdd(x, new Thread(() => next.writeOneColumn(checkThreadCompleted, abc123, x, maxColumn, byteAddress, writeColumn, ramDetail, remK2V, ramV2K, isNumType)));
                    writeColumn[x].Start();
                }

                catch (Exception e)
                {
                    Console.WriteLine($"Thread fail '{e}'");
                }
            });

            do
            {
                Thread.Sleep(10);

            } while (checkThreadCompleted.Count < maxColumn);

            Dictionary<int, List<double>> ramDetailnew = new Dictionary<int, List<double>>(ramDetail);
            Dictionary<int, Dictionary<double, string>> remK2Vnew = new Dictionary<int, Dictionary<double, string>>(remK2V);            
            
            csvInfo.Add("Column", maxColumn);
            csvInfo.Add("Row", byteAddress[0].Count - 1);
            csvInfo.Add("Byte", csvLength);

            return (ramDetailnew, remK2Vnew, csvInfo);
        }
        public void multiThreadBySegmentAddress(int csvLength, List<int> abc123Segment, ConcurrentQueue<int> checkSegmentThreadCompleted, Dictionary<int, List<int>> byteAddress, byte[] abc123, List<int> nextRow, bool useDoubleQuote, int openCloseDoubleQuote, int column, int maxColumn, int nextRowChar, byte separator, byte csvReadSeparator)
        {           
            ConcurrentDictionary<int, Dictionary<int, List<int>>> tempByteAddress = new ConcurrentDictionary<int, Dictionary<int, List<int>>>();                       
            int segmentThread = 10;  
            int segment = Convert.ToInt32(Math.Round((double)(csvLength / segmentThread), 0));

            abc123Segment.Add(0);
            int nextChar = -1;

            for (int i = 1; i < segmentThread; i++)
            {
                do
                {
                    nextChar++;
                } while (abc123[segment * i + nextChar] != nextRow[nextRow.Count - 1]);

                abc123Segment.Add(segment * i + nextChar + 1);
            }
            if (segment * segmentThread < csvLength)
                abc123Segment.Add(csvLength);

            ConcurrentDictionary<int, Thread> concurrentSegmentAddress = new ConcurrentDictionary<int, Thread>(); // a thread manage queue job             

            var startTime = DateTime.Now;
            Parallel.For(0, abc123Segment.Count - 1, currentSegment =>
            {
                try // new a thread to manage queue job
                {
                    Import currentAddress = new Import();
                    concurrentSegmentAddress.TryAdd(currentSegment, new Thread(() => tempByteAddress[currentSegment] = currentAddress.findAddress(checkSegmentThreadCompleted, currentSegment, abc123Segment, useDoubleQuote, abc123, column, maxColumn, nextRow, nextRowChar, separator, csvReadSeparator, openCloseDoubleQuote)));
                    concurrentSegmentAddress[currentSegment].Start();
                }

                catch (Exception e)
                {
                    Console.WriteLine($"Thread fail '{e}'");
                }
            });

            do
            {
                Thread.Sleep(10);

            } while (checkSegmentThreadCompleted.Count < abc123Segment.Count - 1);

            for (int currentSegment = 0; currentSegment < abc123Segment.Count - 1; currentSegment++)
                for (int i = 0; i <= maxColumn; i++)
                    byteAddress[i].AddRange(tempByteAddress[currentSegment][i]);          
        }
        public Dictionary<int, List<int>> findAddress(ConcurrentQueue<int> checkSegmentThreadCompleted, int currentSegment, List<int> abc123Segment, bool useDoubleQuote, byte[] abc123, int column, int maxColumn, List<int> nextRow, int nextRowChar, byte separator, byte csvReadSeparator, int openCloseDoubleQuote)
        {
            Dictionary<int, List<int>> tempByteAddress = new Dictionary<int, List<int>>();

            for (int i = 0; i <= maxColumn; i++)
                tempByteAddress.Add(i, new List<int>());

            if(currentSegment == 0)            
                tempByteAddress[0].Add(0);          

            var fromAddress = abc123Segment[currentSegment];
            var toAddress = abc123Segment[currentSegment + 1];           

            if (useDoubleQuote == false)
            {
                for (int i = fromAddress; i < toAddress; i++)
                {
                    if (column >= maxColumn)
                    {
                        if (abc123[i] == nextRow[0])
                        {
                            tempByteAddress[maxColumn].Add(i + nextRowChar);
                            tempByteAddress[0].Add(i + nextRowChar);
                            column = 1;
                        }
                        else if (abc123[i] == separator)
                        {
                            useDoubleQuote = true; // unmatch column
                            for (int x = 0; x <= maxColumn; x++)
                                tempByteAddress[x].Clear();
                           
                            if (currentSegment == 0)
                                tempByteAddress[0].Add(0);
                            break;
                        }
                    }

                    if (abc123[i] == separator)
                    {
                        tempByteAddress[column].Add(i + 1);
                        column++;
                    }
                }
            }

            if (useDoubleQuote == true) // suspect existence of double quote to hide ","
            {
                separator = csvReadSeparator;
                column = 1;
                openCloseDoubleQuote = 1;
                for (int i = fromAddress; i < toAddress; i++)
                {
                    if (abc123[i] == 34) // space char
                    {
                        openCloseDoubleQuote = openCloseDoubleQuote * -1;
                        abc123[i] = 32; // replace double quote by space
                    }

                    if (openCloseDoubleQuote == 1)
                        separator = csvReadSeparator;
                    else
                        separator = 127; // del key

                    if (column >= maxColumn)
                    {
                        if (abc123[i] == nextRow[0])
                        {
                            tempByteAddress[maxColumn].Add(i + nextRowChar);
                            tempByteAddress[0].Add(i + nextRowChar);
                            column = 1;
                        }
                    }

                    if (abc123[i] == separator)
                    {
                        tempByteAddress[column].Add(i + 1);
                        column++;
                    }
                }
            }
            checkSegmentThreadCompleted.Enqueue(currentSegment);
            return tempByteAddress;
        }
        public void writeOneColumn(ConcurrentQueue<int> checkThreadCompleted, byte[] abc123, int x, int maxColumn, Dictionary<int, List<int>> byteAddress, ConcurrentDictionary<decimal, Thread> writeColumn, ConcurrentDictionary<int, List<double>> ramDetail, ConcurrentDictionary<int, Dictionary<double, string>> remK2V, ConcurrentDictionary<int, Dictionary<string, double>> ramV2K, ConcurrentDictionary<int, bool> isNumType)
        {
            var start = DateTime.Now;
            StringBuilder cellValue = new StringBuilder();            
            bool isNumber = true;
            bool tempIsNum = true;
            int byteAddressLength = 100;
            List<double> tryWriteRamDetail = new List<double>();
            List<bool> isWriteNumberSuccess = new List<bool>();
            isWriteNumberSuccess.Add(true);

            if (byteAddress[x].Count < byteAddressLength)
                byteAddressLength = byteAddress[x].Count - 1;            

            for (int y = 1; y < byteAddressLength; y++)
            {
                cellValue.Clear();

                for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)
                    cellValue.Append((char)abc123[j]);

                isNumber = double.TryParse(cellValue.ToString().Trim(), out double number);

                if (cellValue.ToString().Trim().Length == 0)
                    isNumber = true;

                tempIsNum = tempIsNum && isNumber;
            }

            if (!isNumType.ContainsKey(x))
                isNumType.TryAdd(x, tempIsNum);

            Import tryNumber = new Import();
            tryWriteRamDetail = tryNumber.writeNumberColumn(isWriteNumberSuccess, abc123, x, byteAddress, ramDetail, isNumType);

            if (isNumType[x] == true)
            {
                ramDetail.TryRemove(x, out List<double> value);
                ramDetail.TryAdd(x, tryWriteRamDetail);
            }

            if (isNumType[x] == false) // record string
            {
                if(!ramDetail.ContainsKey(x))
                   ramDetail.TryAdd(x, new List<double>());

                for (int y = 1; y < byteAddress[x].Count; y++)
                {
                    cellValue.Clear();
                    for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)
                    {
                        cellValue.Append((char)abc123[j]);
                    }

                    if (cellValue.ToString().Trim().Length == 0)
                        cellValue.Append("null");

                    string text = cellValue.ToString();

                    if (ramV2K[x].ContainsKey(text)) // same master record
                        ramDetail[x].Add(ramV2K[x][text]);

                    else // add new master record
                    {
                        remK2V[x].Add(ramV2K[x].Count, cellValue.ToString()); // for data 
                        ramV2K[x].Add(text, ramV2K[x].Count);
                        ramDetail[x].Add(ramV2K[x].Count - 1);
                    }
                }
            }
            checkThreadCompleted.Enqueue(x);           
        }
        public List<double> writeNumberColumn(List<bool> isWriteNumberSuccess, byte[] abc123, int x, Dictionary<int, List<int>> byteAddress, ConcurrentDictionary<int, List<double>> ramDetail, ConcurrentDictionary<int, bool> isNumType)
        {
            List<double> tryWriteRamDetail = new List<double>();

            StringBuilder cellValue = new StringBuilder();
            bool isNumber = true;

            tryWriteRamDetail.Add(0);

            if (isNumType[x] == true) // record number
            {
                //column value
                for (int y = 1; y < byteAddress[x].Count; y++)
                {
                    cellValue.Clear();
                    for (int j = byteAddress[x][y]; j < byteAddress[x + 1][y] - 1; j++)
                        cellValue.Append((char)abc123[j]);

                    isNumber = double.TryParse(cellValue.ToString().Trim(), out double number);

                    if (isNumber == true)
                    {
                        if (cellValue.ToString().Trim().Length != 0)
                            tryWriteRamDetail.Add(number);
                        else
                            tryWriteRamDetail.Add(0);
                    }
                    else if (cellValue.ToString().Trim().Length == 0)
                    {
                        tryWriteRamDetail.Add(0);
                        isNumber = true;
                    }
                    else if (isNumber == false)
                    {                       
                        isNumType.TryUpdate(x, false, true);
                        isWriteNumberSuccess[0] = false;
                        break;
                    }
                }
            }
            return tryWriteRamDetail;
        }
    }

    public class ExportCSV
    {
        public void ramTable2CSV( Dictionary<int, List<double>> distinctList, char csvWriteSeparator, string outputFolder, string outputFile)
        {

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                var e = distinctList[cell][0];
                if (cell > 0) csvString.Append(Separator);
                csvString.Append(distinctList[cell][0]);
            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (cell > 0) csvString.Append(Separator);
                    csvString.Append(distinctList[cell][line]);
                }
                csvString.Append(Environment.NewLine);
            }

            using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void ramDistinct2CSV( Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, char csvWriteSeparator, string outputFolder, string outputFile)
        {

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                var e = distinctList[cell][0];
                if (cell > 0) csvString.Append(Separator);
                csvString.Append(distinctList[cell][0]);

            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {
                        if (cell > 0) csvString.Append(Separator);
                        csvString.Append(distinctList[cell][line].ToString());
                    }
                    if (ramKey2Valuegz[cell].Count > 1)
                    {
                        if (cell > 0) csvString.Append(Separator);
                        csvString.Append(distinctList[cell][line]);
                    }
                }
                csvString.Append(Environment.NewLine);
            }
            using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void drillDown2CSVymTable(Dictionary<int, List<double>> ramDetailgz, List<decimal> distinctSet, Dictionary<decimal, List<int>> distinctList2DrillDown, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);
            csvString.Append("Set");
            csvString.Append(Separator);
            for (var cell = 0; cell < ramKey2Valuegz.Count; cell++)
            {
                if (cell > 0) csvString.Append(Separator);

                var currentCell = ramKey2Valuegz[cell][0];

                if (currentCell.Contains(Separator))
                    csvString.Append("\"" + currentCell + "\"");
                else
                    csvString.Append(currentCell);
            }
            csvString.Append(Environment.NewLine);

            for (var set = 0; set < distinctSet.Count; set++)
            {
                for (var line = 0; line < distinctList2DrillDown[distinctSet[set]].Count; line++)
                {
                    csvString.Append(set + 1);
                    csvString.Append(Separator);
                    for (var cell = 0; cell < ramKey2Valuegz.Count; cell++)
                    {
                        if (ramKey2Valuegz[cell].Count == 1)
                        {
                            if (cell > 0)
                                csvString.Append(Separator);
                                              
                            var currentCell = ramDetailgz[cell][distinctList2DrillDown[distinctSet[set]][line]].ToString();                                              

                            if (currentCell.Contains(Separator))
                                csvString.Append("\"" + currentCell + "\"");
                            else
                                csvString.Append(currentCell);
                        }
                        if (ramKey2Valuegz[cell].Count > 1)
                        {
                            if (cell > 0)
                                csvString.Append(Separator);

                            var currentCell = ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[set]][line]]];

                            if (ramKey2Valuegz[cell][0].ToUpper().Contains("DATE"))
                            {

                                isDateNumber = double.TryParse(currentCell, out dateNumber);
                                if (isDateNumber == true)
                                {
                                    if (dateNumber > 1000 && dateNumber < 401770)
                                    {
                                        dateValue = DateTime.FromOADate(dateNumber);
                                        dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                        currentCell = dateFormat;
                                    }
                                }
                            }

                            if (currentCell.Contains(Separator))
                                csvString.Append("\"" + currentCell + "\"");
                            else
                                csvString.Append(currentCell);
                        }
                    }
                    csvString.Append(Environment.NewLine);
                }
            }
            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();

        }
        public void ramDistinct2CSVymTable( Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);
            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                csvString.Append("Set");
                csvString.Append(Separator);
                if (cell > 0) csvString.Append(Separator);

                var currentCell = distinctRamKey2Value[cell][distinctList[cell][0]];

                if (currentCell.Contains(Separator))
                    csvString.Append("\"" + currentCell + "\"");
                else
                    csvString.Append(currentCell);
            }
            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                csvString.Append(line);
                csvString.Append(Separator);
                for (var cell = 0; cell < distinctList.Count; cell++)
                {
                    if (distinctRamKey2Value[cell].Count == 1)
                    {
                        if (cell > 0)
                            csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString();

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                    if (distinctRamKey2Value[cell].Count > 1)
                    {
                        if (cell > 0)
                            csvString.Append(Separator);

                        var currentCell = distinctRamKey2Value[cell][distinctList[cell][line]];

                        if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    currentCell = dateFormat;
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }

            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }
        public void ramDistinct2CSVcrosstabTable(Dictionary<int, List<double>> XdistinctList, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict,  Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, char csvWriteSeparator, string outputFolder, string outputFile)
        {
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder csvString = new StringBuilder();
            string Separator = Convert.ToString(csvWriteSeparator);

            int yHeaderCol = 0;
            for (var cell = 0; cell < distinctList.Count; cell++)
            {
                if (distinctList[cell][0] == 0)
                {
                    yHeaderCol++;
                }
                else
                {
                    break;
                }
            }

            int yxmHeaderCol = yHeaderCol + ((XdistinctList[0].Count - 1) * requestDict[requestID].measurement.Count);

            for (int j = 0; j < XdistinctList.Count; j++)
            {
                for (int cell = 0; cell < yHeaderCol; cell++)
                {
                    if (cell > 0) csvString.Append(Separator);
                    csvString.Append(" ");
                }

                for (int cell = 0; cell < XdistinctList[0].Count - 1; cell++)
                {
                    for (var i = 0; i < requestDict[requestID].measurement.Count; i++)
                    {
                        csvString.Append(Separator);

                        var currentCell = distinctXramKey2Value[j][XdistinctList[j][cell + 1]];
                        if (distinctXramKey2Value[j][XdistinctList[j][0]].ToString().ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    currentCell = dateValue.ToString("dd.MMM.yy");
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }

            for (int cell = 0; cell < yHeaderCol; cell++)
            {
                if (cell > 0) csvString.Append(Separator);

                var currentCell = distinctRamKey2Value[cell][distinctList[cell][0]];

                if (currentCell.Contains(Separator))
                    csvString.Append("\"" + currentCell + "\"");
                else
                    csvString.Append(currentCell);
            }

            for (var cell = 0; cell < XdistinctList[0].Count - 1; cell++)
            {
                for (var i = 0; i < requestDict[requestID].measurement.Count; i++)
                {
                    csvString.Append(Separator);

                    var currentCell = requestDict[requestID].measurement[i].Replace("#", " ");

                    if (currentCell.Contains(Separator))
                        csvString.Append("\"" + currentCell + "\"");
                    else
                        csvString.Append(currentCell);
                }
            }

            csvString.Append(Environment.NewLine);
            for (var line = 1; line < distinctList[0].Count; line++)
            {
                for (var cell = 0; cell < yxmHeaderCol; cell++)
                {
                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count == 1)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString();

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);

                    }
                    if (cell >= yHeaderCol)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctList[cell][line].ToString();

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }

                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count > 1)
                    {
                        if (cell > 0) csvString.Append(Separator);

                        var currentCell = distinctRamKey2Value[cell][distinctList[cell][line]];

                        if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(currentCell, out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    currentCell = dateFormat;
                                }
                            }
                        }

                        if (currentCell.Contains(Separator))
                            csvString.Append("\"" + currentCell + "\"");
                        else
                            csvString.Append(currentCell);
                    }
                }
                csvString.Append(Environment.NewLine);
            }
            using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + outputFile))
            {
                toDisk.Write(csvString);
                toDisk.Close();
            }
            csvString.Clear();
        }

    }
}
