using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using youFastConversion;

namespace Testing
{
    class Program
    {
        static void Main(string[] args)
        {
            // for Windows
            string[] importfile = Directory.GetFiles(@"D:\TestData\", "*.*", SearchOption.AllDirectories);
            int slash = 92; 

            // for Linux
            // string[] importfile = Directory.GetFiles("/home/kk/TestData/", "*.*", SearchOption.AllDirectories);
            // slash = 47; 

            Stopwatch stopwatch = new Stopwatch();            
            List<string> errorFileList = new List<string>();
            string headerSpace; string headerRow; string  leftMargin = "   ";

            binaram currentProcess = new binaram();
            csv2BinaramInput currentInput = new csv2BinaramInput();

            int n = 0;
            string message; int maxLength1; int maxLength2;
            long Filelength; int lastOneMatch; string fileName;

            foreach (string filePath in importfile)
            {
                currentInput.filePath = filePath;
                stopwatch.Reset(); stopwatch.Start();
                binaram csv2Binaram = currentProcess.csv2Binaram(currentInput);
                stopwatch.Stop();

                // to accept csv file having more than one row (include header row)
                // to accept file having more than one column
                // to accept validated rows has same number of csv separator
                if (csv2Binaram.validateRow > 1 && csv2Binaram.tableColumnCountExceptionList[0] > 1 && csv2Binaram.tableColumnCountExceptionList.Count == 1)
                {
                    maxLength1 = 1; maxLength2 = 1;
                    n++;

                    Filelength = new FileInfo(filePath).Length;

                    lastOneMatch = filePath.Substring(0, filePath.LastIndexOf((char)slash) + 1).LastIndexOf((char)slash);
                    fileName = filePath.Substring(lastOneMatch + 1, filePath.Length - lastOneMatch - 1);

                    message = leftMargin + ""; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + "File No: " + n; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + "File Name: " + fileName; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + "File Size: " + string.Format("{0:#,0}", Filelength) + " Bytes"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + ""; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    for (int i = 0; i < csv2Binaram.dataType.Count; i++)
                    {
                        if (csv2Binaram.columnName[i].Length > maxLength1)
                            maxLength1 = csv2Binaram.columnName[i].Length;
                    }

                    for (int i = 0; i < csv2Binaram.dataType.Count; i++)
                    {
                        if (csv2Binaram.dataType[i].Length > maxLength2)
                            maxLength2 = csv2Binaram.dataType[i].Length;
                    }                   

                    if (maxLength1 - 10 > 0)
                    {
                        headerSpace = new String((char)32, (maxLength1 - 10));
                        headerRow = "Column" + headerSpace + "        Type   Record";
                    }
                    else
                    {
                        headerSpace = new String((char)32, 5);
                        headerRow = "Column" + headerSpace + "  Type   Record";
                    }

                    message = leftMargin + headerRow; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    var line = new String((char)45, headerRow.Length + 3);
                    message = leftMargin + line; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    for (int i = 0; i < csv2Binaram.dataType.Count; i++)
                    {
                        var space1 = new String((char)32, (maxLength1 - csv2Binaram.columnName[i].Length));
                        var space2 = new String((char)32, (maxLength2 - csv2Binaram.dataType[i].Length));

                        if (i < 10)
                        {
                            message = leftMargin + i + " " + csv2Binaram.columnName[i] + space1 + "  " + csv2Binaram.dataType[i] + space2 + " " + string.Format("{0:#,0}", csv2Binaram.factTable[i].Count - 1);
                            Console.WriteLine(message);
                            File.AppendAllText("log.txt", message + Environment.NewLine);
                        }
                        else
                        {
                            message = leftMargin + i + " " + csv2Binaram.columnName[i] + space1 + " " + csv2Binaram.dataType[i] + space2 + " " + string.Format("{0:#,0}", csv2Binaram.factTable[i].Count - 1);
                            Console.WriteLine(message);
                            File.AppendAllText("log.txt", message + Environment.NewLine);
                        }
                    }

                    message = leftMargin + ""; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + "Time   Conversion"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + "---------------------------"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "CSV to Binaram"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    csv2DataTablesetting setCSV2DataTable = new csv2DataTablesetting();
                    DataTable csv2DataTable = currentProcess.csv2DataTable(currentInput, setCSV2DataTable);
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "CSV to DataTable"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    csv2HTMLsetting setCSV2HTML = new csv2HTMLsetting();

                    using (StreamWriter toDisk = new StreamWriter("csv2HTML-" + fileName.Substring(0, fileName.LastIndexOf((char)46)) + ".html"))
                    {
                        toDisk.Write(currentProcess.csv2HTML(currentInput, setCSV2HTML));
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "CSV to HTML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    csv2JSONsetting setCSV2JSON = new csv2JSONsetting();
                    setCSV2JSON.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    StringBuilder csv2JSON = currentProcess.csv2JSON(currentInput, setCSV2JSON);
                    using (StreamWriter toDisk = new StreamWriter("csv2JSON-" + setCSV2JSON.tableName + ".json"))
                    {
                        toDisk.Write(csv2JSON);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "CSV to JSON"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    csv2XMLsetting setCSV2XML = new csv2XMLsetting();
                    setCSV2XML.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    StringBuilder csv2XML = currentProcess.csv2XML(currentInput, setCSV2XML);
                    using (StreamWriter toDisk = new StreamWriter("csv2XML-" + setCSV2XML.tableName + ".xml"))
                    {
                        toDisk.Write(csv2XML);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "CSV to XML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    binaram2CSVsetting setBinaram2CSV = new binaram2CSVsetting();
                    StringBuilder binaram2CSV = currentProcess.binaram2CSVMultithread(csv2Binaram, setBinaram2CSV);
                    using (StreamWriter toDisk = new StreamWriter("binaram2CSV-" + fileName.Substring(0, fileName.LastIndexOf((char)46)) + ".csv"))
                    {
                        toDisk.Write(binaram2CSV);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "Binaram to CSV"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    binaram2DataTablesetting setBinaram2DataTable = new binaram2DataTablesetting();
                    DataTable binaram2DataTable = currentProcess.binaram2DataTableMultithread(csv2Binaram, setBinaram2DataTable);
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "Binaram to DataTable"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    binaram2HTMLsetting setBinaram2HTML = new binaram2HTMLsetting();
                    using (StreamWriter toDisk = new StreamWriter("binaram2HTML-" + fileName.Substring(0, fileName.LastIndexOf((char)46)) + ".html"))
                    {
                        toDisk.Write(currentProcess.binaram2HTMLMultithread(csv2Binaram, setBinaram2HTML));
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "Binaram to HTML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    binaram2JSONsetting setBinaram2JSON = new binaram2JSONsetting();
                    setBinaram2JSON.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    StringBuilder binaram2JSON = currentProcess.binaram2JSONMultithread(csv2Binaram, setBinaram2JSON);
                    using (StreamWriter toDisk = new StreamWriter("binaram2JSON-" + setBinaram2JSON.tableName + ".json"))
                    {
                        toDisk.Write(binaram2JSON);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "Binaram to JSON"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    binaram2XMLsetting setBinaram2XML = new binaram2XMLsetting();
                    setBinaram2XML.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    StringBuilder binaram2XML = currentProcess.binaram2XMLMultithread(csv2Binaram, setBinaram2XML);
                    using (StreamWriter toDisk = new StreamWriter("binaram2XML-" + setBinaram2XML.tableName + ".xml"))
                    {
                        toDisk.Write(binaram2XML);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "Binaram to XML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    csv2Binaram = null;

                    stopwatch.Reset(); stopwatch.Start();
                    dataTable2BinaramSetting setDataTable2Binaram = new dataTable2BinaramSetting();
                    binaram dataTable2Binaram = currentProcess.dataTable2BinaramMultithread(binaram2DataTable, setDataTable2Binaram);
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "DataTable to Binaram"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    dataTable2CSVsetting setDataTable2CSV = new dataTable2CSVsetting();
                    StringBuilder dataTable2CSV = currentProcess.dataTable2CSV(binaram2DataTable, setDataTable2CSV);
                    using (StreamWriter toDisk = new StreamWriter("dataTable2CSV-" + fileName.Substring(0, fileName.LastIndexOf((char)46)) + ".csv"))
                    {
                        toDisk.Write(dataTable2CSV);
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "DataTable to CSV"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    dataTable2HTMLsetting setDataTable2HTML = new dataTable2HTMLsetting();
                    using (StreamWriter toDisk = new StreamWriter("dataTable2HTML-" + fileName.Substring(0, fileName.LastIndexOf((char)46)) + ".html"))
                    {
                        toDisk.Write(currentProcess.dataTable2HTML(binaram2DataTable, setDataTable2HTML));
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "DataTable to HTML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    dataTable2JSONsetting setDataTable2JSON = new dataTable2JSONsetting();
                    setDataTable2JSON.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    using (StreamWriter toDisk = new StreamWriter("dataTable2JSON-" + setDataTable2JSON.tableName + ".json"))
                    {
                        toDisk.Write(currentProcess.dataTable2JSON(binaram2DataTable, setDataTable2JSON));
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "DataTable to JSON"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    stopwatch.Reset(); stopwatch.Start();
                    dataTable2XMLsetting setDataTable2XML = new dataTable2XMLsetting();
                    setDataTable2XML.tableName = fileName.Substring(0, fileName.LastIndexOf((char)46));
                    using (StreamWriter toDisk = new StreamWriter("dataTable2XML-" + setDataTable2XML.tableName + ".xml"))
                    {
                        toDisk.Write(currentProcess.dataTable2XML(binaram2DataTable, setDataTable2XML));
                        toDisk.Close();
                    }
                    message = leftMargin + string.Format("{0:0.000}", Math.Round(stopwatch.Elapsed.TotalSeconds, 3)) + "s " + "DataTable to XML"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

                    binaram2DataTable = null;
                }
                else
                {
                    message = leftMargin + filePath;

                    if (csv2Binaram.validateRow <= 1)
                        message = leftMargin + message + " validateRow:" + csv2Binaram.validateRow + " => validated number of row is not > 1";

                    if (csv2Binaram.tableColumnCountExceptionList[0] <= 1)
                        message = leftMargin + message + " tableColumnCountExceptionList[0]:" + csv2Binaram.tableColumnCountExceptionList[0] + " => validated number of column is not > 1";

                    if (csv2Binaram.tableColumnCountExceptionList.Count != 1)
                        message = leftMargin + message + " tableColumnCountExceptionList.Count:" + csv2Binaram.tableColumnCountExceptionList.Count + " => separator are not identical for each row";

                        errorFileList.Add(message);
                }                
            }          

            message = leftMargin + ""; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
            message = leftMargin + "-------- Rejected File List -------------"; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);

            foreach (string rejectMessage in errorFileList)
            {
                n++;
                message = leftMargin + "File" + n + ":" + rejectMessage; Console.WriteLine(message); File.AppendAllText("log.txt", message + Environment.NewLine);
            }          

            Console.ReadLine();
        }
    }
}

