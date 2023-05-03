using youFast;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Collections;
using System.Windows;

namespace clientMachine
{
    public class request
    {
        public string processID { get; set; }
        public string processButton { get; set; }
        public string dataset{ get; set; }
        public string randomFilter { get; set; }
        public string userID { get; set; }
        public string drillType { get; set; }
        public decimal drillSet { get; set; }
        public string drillSetHeader { get; set; }
        public decimal drillSetCrosstab { get; set; }
        public string debugOutput { get; set; }
        public string importFile { get; set; }
        public string importType { get; set; }
        public string timeStamp { get; set; }
        public string filterColumn { get; set; }
        public string direction { get; set; }
        public string sortingOrder { get; set; }
        public string openReport { get; set; }
        public decimal nextPageID { get; set; }
        public decimal cancelRequestID { get; set; }
        public int pageXlength { get; set; }
        public int pageYlength { get; set; }
        public int pageXlengthCrosstab { get; set; }
        public int pageYlengthCrosstab { get; set; }
        public string rotateDimension { get; set; }
        public string rotateDimensionFrom { get; set; }
        public string rotateDimensionTo { get; set; }
        public string sortXdimension { get; set; }
        public string sortYdimension { get; set; }
        public string precisionLevel { get; set; }
        public string drillDownEventType { get; set; }
        public string moveColumnDirection { get; set; }
        public string moveColumnName { get; set; }
        public string addColumnType { get; set; }
        public string resetDimensionOrder { get; set; }
        public string measureType { get; set; }
        public List<string> column { get; set; }
        public List<string> startOption { get; set; }
        public List<string> startColumnValue { get; set; }
        public List<string> endOption { get; set; }
        public List<string> endColumnValue { get; set; }
        public List<string> distinctDimension { get; set; }
        public List<string> distinctOrder { get; set; }
        public List<string> crosstabDimension { get; set; }
        public List<string> crosstabOrder { get; set; }
        public List<string> measurement { get; set; }
    }

    public class response
    {
        public decimal sourcedRecordCount;
        public decimal selectedRecordCount = 0;
        public decimal distinctCount;
        public decimal crosstabCount;
        public decimal requestID;       
        public bool updateCrosstab;
        public bool updateDistinct;
        public bool updateMeasure;
        public bool uploadDesktopFile;
        public Dictionary<string, int> memTable;
        public bool removeCrosstabMeasure;        
        public StringBuilder html;
        public StringBuilder htmlBackup;
        public DateTime startTime;
        public DateTime sortedTime;
        public DateTime endImportTime;
        public DateTime endExtractCSVTime;
        public DateTime endDistinctTime;
        public DateTime endTime;
    }

    public class userPreference
    {
        public string drillDownEventType;
        public int nextDrillDownEventType = 1;
    }


    class clientMachine
    {
        static void Main(string[] args)
        {
            Console.Clear();          

            int iteration = 1;
            string outputFolder = @"uSpace\exportedFile\";
            string sourceFolder = Environment.CurrentDirectory + "\\uSpace\\";
            string db1Folder = Environment.CurrentDirectory + "\\uSpace\\distinctDB\\Primary\\";
            string dbBackupFolder = Environment.CurrentDirectory + "\\uSpace\\distinctDB\\Backup\\";
            byte csvReadSeparator = 44;
            char csvWriteSeparator = ',';

            if (!Directory.Exists(outputFolder))
                Directory.CreateDirectory(outputFolder);

            if (!Directory.Exists(sourceFolder))
                Directory.CreateDirectory(sourceFolder);

            if (!Directory.Exists(db1Folder))
                Directory.CreateDirectory(db1Folder);

            if (!Directory.Exists(dbBackupFolder))
                Directory.CreateDirectory(dbBackupFolder);


            Dictionary<int, string> forwardMessage = new Dictionary<int, string>();           
            ConcurrentDictionary<decimal, request> requestDict = new ConcurrentDictionary<decimal, request>();
            ConcurrentDictionary<decimal, response> responseDict = new ConcurrentDictionary<decimal, response>();
            ConcurrentDictionary<string, userPreference> userPreference = new ConcurrentDictionary<string, userPreference>();
            userPreference.TryAdd("system", new userPreference());

            forwardMessage[0] = "waiting";
            WebSockAgentServer agentServer = new WebSockAgentServer();
            agentServer.webSock(userPreference, iteration, outputFolder, csvReadSeparator, csvWriteSeparator, forwardMessage, requestDict, responseDict, sourceFolder, db1Folder, dbBackupFolder);

            /*
              // convert html to c#

              byte[] array = File.ReadAllBytes("distinctWeb1.html");           
              StringBuilder htmlSharp = new StringBuilder();
              StringBuilder htmlCurrentLine = new StringBuilder();
              Console.WriteLine(array.Length);

              for (int i = 0; i < array.Length - 1; i++)
              {
                  if (array[i] != 13 && array[i + 1] != 10)
                  {   
                    if(array[i] == 34)
                       htmlCurrentLine.Append(((char)92).ToString() + (char)(array[i]));
                    else
                       htmlCurrentLine.Append((char)(array[i]));
                  }
                  else
                  {                     
                       htmlSharp.Append("html.AppendLine(" + ((char)34).ToString() + htmlCurrentLine.ToString().Trim() + ((char)34).ToString() + ");");
                       htmlCurrentLine.Clear();
                       htmlSharp.Append(Environment.NewLine);
                  }
              }            

              using (StreamWriter toDisk = new StreamWriter("test.html"))
              {
                  toDisk.Write(htmlSharp);
                  toDisk.Close();
              }
              */

            if (File.Exists("D:\\youFastSource\\data.cs"))
            {
                StringBuilder dataCS = new StringBuilder();

                string dataFolder = "D:\\youFastSource\\Data";
                string targetZipFile = Environment.CurrentDirectory + "\\uSpace\\factoryData.zip";
                string targetCSFile = "D:\\youFastSource\\data.cs";

                if (File.Exists(targetZipFile))
                    File.Delete(targetZipFile);

                ZipFile.CreateFromDirectory(dataFolder, targetZipFile);

                byte[] readFile = File.ReadAllBytes(targetZipFile);

                dataCS.AppendLine("using System;");
                dataCS.AppendLine("using System.IO;");
                dataCS.AppendLine("using System.IO.Compression;");
                dataCS.AppendLine("namespace youFast");
                dataCS.AppendLine("{");
                dataCS.AppendLine("public class Data");
                dataCS.AppendLine("{");
                dataCS.AppendLine("public void data(string outputZipFile, string outputCSVData)");
                dataCS.AppendLine("{");
                dataCS.AppendLine("if (File.Exists(outputCSVData + \"factoryData.csv\"))");
                dataCS.AppendLine("File.Delete(outputCSVData + \"factoryData.csv\");");

                dataCS.Append("byte[] factoryData = {");

                int i = 0;
                foreach (byte value in readFile)
                {
                    dataCS.Append(value.ToString());
                    i++;

                    if (i < readFile.Length)
                        dataCS.Append(",");
                }

                dataCS.AppendLine("};");

                dataCS.AppendLine("File.WriteAllBytes(outputZipFile, factoryData);");
                dataCS.AppendLine("ZipFile.ExtractToDirectory(outputZipFile, outputCSVData);");

                dataCS.AppendLine("File.Delete(outputZipFile);");
                dataCS.AppendLine("}");
                dataCS.AppendLine("}");
                dataCS.AppendLine("}");

                if (File.Exists(targetCSFile))
                    File.Delete(targetCSFile);

                using (StreamWriter toDisk = new StreamWriter(targetCSFile))
                {
                    toDisk.Write(dataCS);
                    toDisk.Close();
                }
            }

            string outputZipFile = Environment.CurrentDirectory + "\\uSpace\\" + "factoryData.zip";
            string outputCSVData = Environment.CurrentDirectory + "\\uSpace\\";

            Data currentData = new Data();
            currentData.data(outputZipFile, outputCSVData);

            /*
            using (FileStream stream = File.OpenWrite(@"rocket.ico"))
            {
                Bitmap bitmap = (Bitmap)Image.FromFile(@"rocket.png");
                Icon.FromHandle(bitmap.GetHicon()).Save(stream);
            }
            */
        }
    }
}


