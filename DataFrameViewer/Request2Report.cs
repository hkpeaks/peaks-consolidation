using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;

namespace youFast
{
    public class Request2Report
    {
        public void distinctDBreporting(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<int, decimal> currentRequestID, char csvWriteSeparator, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, StringBuilder> htmlTable, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string outputFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl)
        {           

            //Console.WriteLine("1 End milliseconds: {0:MM/dd/yyy HH:mm:ss.fff}", DateTime.Now);
            StringBuilder htmlEmpty = new StringBuilder();
            htmlEmpty.Append("");

            if (!responseDict.ContainsKey(requestID))
                responseDict.TryAdd(requestID, new clientMachine.response());

            responseDict[requestID].html = htmlEmpty;
            responseDict[requestID].requestID = requestID;

            Dictionary<int, List<double>> ramDetailgz = new Dictionary<int, List<double>>();
            Dictionary<int, Dictionary<double, string>> ramKey2Valuegz = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<string, double>> ramValue2Keygz = new Dictionary<int, Dictionary<string, double>>();
            Dictionary<int, Dictionary<double, double>> ramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> ramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
            
            bool useMemory = false;          
            string currentImportFile = requestDict[requestID].importFile.ToString();
            responseDict[requestID].uploadDesktopFile = false;
            string[] readFile = Directory.GetFiles(sourceFolder, requestDict[requestID].importFile.ToString()); // check if exist in csv import folder

            if (requestDict[requestID].importType == "overwrite"  && readFile.Length == 0) // use drag and drop file if not exist in csv import folder
            {
                responseDict[requestID].uploadDesktopFile = true;
                string folder = Environment.GetFolderPath(Environment.SpecialFolder.Desktop) + "\\";
                readFile = Directory.GetFiles(folder, requestDict[requestID].importFile.ToString());
                StringBuilder builder = new StringBuilder();
                foreach (string value in readFile)
                    builder.Append(value);
            }
            var end = DateTime.Now;
            Console.WriteLine("Startup Time : " + String.Format("{0:F3}", (end - responseDict[requestID].startTime).TotalSeconds) + " seconds");

            Request2Report currentReport = new Request2Report();
            
            if (useMemory == false && readFile.Length > 0) // use csv datastore if csv file list exist
            {                
                currentReport.importFile(outputFolder, responseDict, readFile, currentImportFile, ramDetail, remK2V, ramKey2Valuegz, sourceFolder, csvReadSeparator, db1Folder, requestID, requestDict, ramDetailgz);
                var startSimulationTime = DateTime.Now;
                /*
                StringBuilder master = new StringBuilder(); // export master record

                for (int i = 0; i < remK2V[requestDict[requestID].importFile].Count; i++)
                {
                    foreach (var member in remK2V[requestDict[requestID].importFile][i])
                    {
                        if (remK2V[requestDict[requestID].importFile][i][0] != member.Value)
                            master.AppendLine(requestDict[requestID].importFile + "," + remK2V[requestDict[requestID].importFile][i][0] + "," + member.Value);
                    }
                }
               

                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + requestDict[requestID].importFile))
                {
                    toDisk.Write(master);
                    toDisk.Close();
                }

                 */
                if (requestDict[requestID].importFile == "factoryData.csv")
                { 
                    Simulation currentSimulation = new Simulation();
                    currentSimulation.dimensionValueList(ramDetail, remK2V, requestDict, requestID, sourceFolder, outputFolder);
                    var endSimulationTime = DateTime.Now;
                 //   Console.WriteLine("Simulation Time : " + String.Format("{0:F3}", (endSimulationTime - startSimulationTime).TotalSeconds) + " seconds");
                }
            }
            var start5 = DateTime.Now;
            if (ramDetail.ContainsKey(currentImportFile))  // use ram datastore if filename exist in memory
            { 
                for (int i = 0; i < ramDetail[requestDict[requestID].importFile].Count; i++)
                    ramDetailgz[i] = ramDetail[requestDict[requestID].importFile][i];

                for (int i = 0; i < remK2V[requestDict[requestID].importFile].Count; i++)
                {
                    ramKey2Valuegz.Add(i, new Dictionary<double, string>());
                    ramKey2Valuegz[i] = remK2V[requestDict[requestID].importFile][i];
                }
                useMemory = true;                
            }
            
            if (useMemory == false && readFile.Length == 0) // use disk DB datastoe if csv file not exist
                currentReport.importDB(currentImportFile, ramDetail, remK2V, ramKey2Valuegz, sourceFolder, csvReadSeparator, db1Folder, requestID, requestDict, ramDetailgz);
          
            if (tableFact.ContainsKey(currentImportFile)) // build table fact statistics           
                tableFact[currentImportFile].Clear();           

            tableFact.TryAdd(currentImportFile, new ConcurrentDictionary<int, int>());            

            for (int i = 10; i < ramKey2Valuegz.Count + 10; i++)
                tableFact[currentImportFile].TryAdd(i, ramKey2Valuegz[i - 10].Count); // master record count for each column
           
            int masterRecordCount = 0;
            for (int i = 0; i < ramKey2Valuegz.Count; i++)
                masterRecordCount = masterRecordCount + tableFact[currentImportFile][i + 10];
            
            tableFact[currentImportFile].TryAdd(9, masterRecordCount); // total master record count for all columns
            tableFact[currentImportFile].TryAdd(8, ramDetailgz[0].Count - 1);  // total row                      
           
           
            Dictionary<string, string> columnName2ID = new Dictionary<string, string>();           
            for (int i = 0; i < remK2V[requestDict[requestID].importFile].Count; i++)          
                columnName2ID.Add(remK2V[requestDict[requestID].importFile][i][0], i.ToString());
           
            if (requestDict[requestID].randomFilter == "Y") // random generate filter
            {
                Dictionary<string, string> variable = new Dictionary<string, string>();
                Dictionary<string, List<string>> array = new Dictionary<string, List<string>>();

                List<string> displayRandomSelectedValue = new List<string>();
                List<string> selectDistinctCol = new List<string>();

                currentReport.randomGenerateSelectionCriteria(variable, array, requestID, requestDict, remK2V, columnName2ID, selectDistinctCol, ramDetail, displayRandomSelectedValue);

                ExportHTML currentExport = new ExportHTML();
                int serialID = 0;

                screenControl.Add(requestID, new Dictionary<string, StringBuilder>());

                screenControl[requestID].Add("displayFilterDropDownList", new StringBuilder());
                screenControl[requestID]["displayFilterDropDownList"].Append(currentExport.ramdistint2displayFilterDropDownList(outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile]).ToString());

                screenControl[requestID].Add("measurement", new StringBuilder());
                screenControl[requestID]["measurement"].Append(currentExport.ramdistint2Measurement(outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile]).ToString());

                screenControl[requestID].Add("displayCrosstalDimension", new StringBuilder());
                screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2Crosstab(tableFact, outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile]).ToString());

                screenControl[requestID].Add("displaySelectedFilterValue", new StringBuilder());
                screenControl[requestID]["displaySelectedFilterValue"].Append(currentExport.ramdistinct2DisplaySelectedFilterValue(outputFolder, requestID, requestDict, serialID, remK2V[requestDict[requestID].importFile], displayRandomSelectedValue).ToString());

                screenControl[requestID].Add("displaySelectedColumn", new StringBuilder());
                screenControl[requestID]["displaySelectedColumn"].Append(currentExport.ramdistint2DisplaySelectedColumn(tableFact, remK2V, outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile], selectDistinctCol).ToString());
               
                requestDict[requestID].processID = "refreshSelectFileList";
                screenControl[requestID].Add("ramdistint2SelectFile", new StringBuilder());
                screenControl[requestID]["ramdistint2SelectFile"].Append(currentExport.ramdistint2SelectFile(remK2V, outputFolder, requestID, requestDict, sourceFolder, db1Folder).ToString()); //get html filter by StringBuider                           
            }
            else
            {              
                List<string> tempStartColumnValue = new List<string>(); // validation of column value
                List<string> tempEndColumnValue = new List<string>();             

                for (int i = 0; i < requestDict[requestID].startColumnValue.Count; i++)
                {
                    tempStartColumnValue.Add(requestDict[requestID].startColumnValue[i]);
                    tempEndColumnValue.Add(requestDict[requestID].endColumnValue[i]);

                    if (requestDict[requestID].startColumnValue[i] == "blankValue" && requestDict[requestID].endColumnValue[i] != "blankValue")
                       requestDict[requestID].startColumnValue[i] = requestDict[requestID].endColumnValue[i];                   

                    if (requestDict[requestID].endColumnValue[i] == "blankValue" && requestDict[requestID].startColumnValue[i] != "blankValue")
                       requestDict[requestID].endColumnValue[i] = requestDict[requestID].startColumnValue[i];

                    if (string.Compare(requestDict[requestID].endColumnValue[i], requestDict[requestID].startColumnValue[i]) < 0)                   
                        requestDict[requestID].column[i] = "Fact";
                    
                    if (requestDict[requestID].startColumnValue[i] == "blankValue" && requestDict[requestID].endColumnValue[i] == "blankValue")                    
                        requestDict[requestID].column[i] = "Fact";
                }              

                if (requestDict[requestID].measurement == null || requestDict[requestID].measurement.Count == 0)
                { 
                    List<string> addMeasure = new List<string>();
                    addMeasure.Add("Fact");
                    requestDict[requestID].measurement = addMeasure;
                    responseDict[requestID].updateMeasure = true;
                    ExportHTML currentExport = new ExportHTML();
                    screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                    screenControl[requestID].Add("measurement", new StringBuilder());
                    screenControl[requestID]["measurement"].Append(currentExport.ramdistint2Measurement(outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile]).ToString());
                }
                if (requestDict[requestID].distinctDimension == null)
                {
                    List<string> addDistinct = new List<string>();
                    addDistinct.Add(ramKey2Valuegz[0][0].ToString());
                    requestDict[requestID].distinctDimension = addDistinct;
                    responseDict[requestID].updateDistinct = true;
                    ExportHTML currentExport = new ExportHTML();
                    screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                    screenControl[requestID].Add("displaySelectedColumn", new StringBuilder());
                    screenControl[requestID]["displaySelectedColumn"].Append(currentExport.ramdistint2DisplaySelectedColumn(tableFact, remK2V, outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile], addDistinct).ToString());
                }
                else // is crosstabDimension = distinctDimension
                {
                    bool isDimensionEqual = true;
                    if (requestDict[requestID].crosstabDimension != null)
                    { 
                        if(requestDict[requestID].distinctDimension.Count <= requestDict[requestID].crosstabDimension.Count)
                        { 
                            for (int i = 0; i < requestDict[requestID].crosstabDimension.Count; i++)
                            { 
                                if (requestDict[requestID].distinctDimension.Contains(requestDict[requestID].crosstabDimension[i]))                                
                                    isDimensionEqual = isDimensionEqual && true;                               
                                else
                                    isDimensionEqual = isDimensionEqual && false;
                            }

                            if (isDimensionEqual == true)
                            {
                                requestDict[requestID].crosstabDimension.RemoveAt(0);
                                responseDict[requestID].updateCrosstab = true;
                                ExportHTML currentExport = new ExportHTML();                                

                                if (!screenControl.ContainsKey(requestID))
                                    screenControl.Add(requestID, new Dictionary<string, StringBuilder>());

                                screenControl[requestID].Add("displayCrosstalDimension", new StringBuilder());
                                screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2Crosstab(tableFact, outputFolder, requestID, requestDict, remK2V[requestDict[requestID].importFile]).ToString());
                            }
                        }
                    }
                }
            }
            var end5 = DateTime.Now;
            Console.WriteLine("Startup2 Time : " + String.Format("{0:F3}", (end5 - start5).TotalSeconds) + " seconds");

            currentReport.runReportByCurrentDataset(userPreference, tableFact, remK2V, sourceFolder, db1Folder, screenControl, currentRequestID, ramValue2Keygz, outputFolder, csvWriteSeparator, requestID, requestDict, responseDict, columnName2ID, htmlTable, ramDetailgz, ramKey2Valuegz);            
        }
        public void importFile(string outputFolder, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string[] readFile, string currentImportFile, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, string sourceFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, List<double>> ramDetailgz)
        {
            var csvStart = DateTime.Now;
            Import currentImport = new Import();

            if (ramDetail.ContainsKey(currentImportFile) || remK2V.ContainsKey(currentImportFile))
            { 
                ramDetail.Remove(currentImportFile);
                remK2V.Remove(currentImportFile);
            }

            ramDetail.Add(currentImportFile, new Dictionary<int, List<double>>());
            remK2V.Add(currentImportFile, new Dictionary<int, Dictionary<double, string>>());           

            
            (Dictionary<int, List<double>> ramDetailnew, Dictionary<int, Dictionary<double, string>> remK2Vnew, Dictionary<string, int> csvInfo) = currentImport.CSV2ramDetail(readFile, csvReadSeparator);
            responseDict[requestID].endExtractCSVTime = DateTime.Now;
            Console.WriteLine("Extract CSV" + currentImportFile + " Byte:" + string.Format("{0:#,0}", csvInfo["Byte"]).ToString() +  " Column:" + string.Format("{0:#,0}", csvInfo["Column"]).ToString() + " Row:" + string.Format("{0:#,0}", csvInfo["Row"]).ToString() + " Time:" + String.Format("{0:F3}", (responseDict[requestID].endExtractCSVTime - csvStart).TotalSeconds) + " seconds");

            var copyDataStartTime = DateTime.Now;

            for (int i = 0; i < ramDetailnew.Count; i++)                
                    ramDetail[currentImportFile].Add(i, ramDetailnew[i]);

            for (int i = 0; i < remK2Vnew.Count; i++)                 
                    remK2V[currentImportFile].Add(i, remK2Vnew[i]);           

            int totalColumn = remK2V[requestDict[requestID].importFile].Count;
           
            remK2V[currentImportFile].Add(totalColumn, new Dictionary<double, string>());                
                    remK2V[currentImportFile][totalColumn].Add(0, "Fact");
           
            ramDetail[currentImportFile].Add(totalColumn, new List<double>());               
                    ramDetail[currentImportFile][totalColumn].Add(0);
             
            for (int i = 1; i < ramDetail[currentImportFile][0].Count; i++)  
                    ramDetail[currentImportFile][totalColumn].Add(1);          

            if (!Directory.Exists(sourceFolder + "importedFile\\" + requestDict[requestID].importFile.ToString() + "\\"))
                Directory.CreateDirectory(sourceFolder + "importedFile\\" + requestDict[requestID].importFile.ToString() + "\\");

            int fileNo = 1;
            bool fileExist = false;

            do
            {
                fileExist = false;
                string[] targetFile = Directory.GetFiles(sourceFolder + "importedFile\\" + requestDict[requestID].importFile.ToString() + "\\", "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString());

                foreach (string filePath in targetFile)
                {
                    if (filePath == sourceFolder + "importedFile\\" + requestDict[requestID].importFile.ToString() + "\\" + "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString())
                    {
                        fileNo++;
                        fileExist = true;
                    }
                    else
                        fileExist = false;
                }

            } while (fileExist == true);

           if(responseDict[requestID].uploadDesktopFile == false)
                File.Move(sourceFolder + requestDict[requestID].importFile.ToString(), sourceFolder + "importedFile\\" + requestDict[requestID].importFile.ToString() + "\\" + "imported" + fileNo.ToString() + "~" + requestDict[requestID].importFile.ToString());

          
            ConcurrentDictionary<decimal, Thread> ioThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage queue job             

            try // new a thread to manage queue job
            {
                ioThread.TryAdd(requestID, new Thread(() => ramDB2Disk()));
                ioThread[requestID].Start();
            }
            catch (Exception e)
            {
                Console.WriteLine($"queueThread fail '{e}'");
            }
          
            responseDict[requestID].endImportTime = DateTime.Now;

            Console.WriteLine("copy data Time : " + String.Format("{0:F3}", (responseDict[requestID].endImportTime - responseDict[requestID].endExtractCSVTime).TotalSeconds) + " seconds");

            void ramDB2Disk() //Backup DB to Disk                        
            {
                if (!Directory.Exists(db1Folder + "\\" + currentImportFile + "\\"))
                    Directory.CreateDirectory(db1Folder + "\\" + currentImportFile + "\\");

                BinaryFormatter formatter = new BinaryFormatter();

                using (MemoryStream MemoryStream = new MemoryStream())               
                
                using (GZipStream gZipStream = new GZipStream(File.OpenWrite(db1Folder + "\\" + currentImportFile + "\\" + "ramDetail" + ".db"), CompressionMode.Compress))
                    formatter.Serialize(gZipStream, ramDetail[currentImportFile]);
               
                using (GZipStream gZipStream = new GZipStream(File.OpenWrite(db1Folder + "\\" + currentImportFile + "\\" + "ramKey2Value.db"), CompressionMode.Compress))
                    formatter.Serialize(gZipStream, remK2V[currentImportFile]);
            }                        
        }
        public void importDB(string currentImportFile, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, string sourceFolder, byte csvReadSeparator, string db1Folder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, List<double>> ramDetailgz)
        {            
            if (ramDetail.ContainsKey(currentImportFile) || remK2V.ContainsKey(currentImportFile))
            {
                ramDetail.Remove(currentImportFile);
                remK2V.Remove(currentImportFile);
            }

            BinaryFormatter formatter1 = new BinaryFormatter();

            using (GZipStream gZipStream = new GZipStream(File.OpenRead(db1Folder + requestDict[requestID].importFile.ToString() + "\\" + "ramDetail.db"), CompressionMode.Decompress))
                ramDetail.Add(currentImportFile, (Dictionary<int, List<double>>)formatter1.Deserialize(gZipStream));

            using (GZipStream gZipStream = new GZipStream(File.OpenRead(db1Folder + requestDict[requestID].importFile.ToString() + "\\" + "ramKey2Value.db"), CompressionMode.Decompress))
                remK2V.Add(currentImportFile, (Dictionary<int, Dictionary<double, string>>)formatter1.Deserialize(gZipStream));

            if (ramDetail.ContainsKey(requestDict[requestID].importFile))
            {  
                for (int i = 0; i < remK2V[requestDict[requestID].importFile].Count; i++)
                {
                    ramDetailgz.Add(i, ramDetail[requestDict[requestID].importFile][i]);
                    ramKey2Valuegz.Add(i, remK2V[requestDict[requestID].importFile][i]);
                }
            }           
        }
        public void randomGenerateSelectionCriteria(Dictionary<string, string> variable, Dictionary<string, List<string>> array, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, Dictionary<string, string> columnName2ID, List<string> selectDistinctCol, Dictionary<string, Dictionary<int, List<double>>> ramDetail, List<string> displayRandomSelectedValue) // Generate Selection Criteria and Run Report
        {   
            variable.Clear();
            array.Clear();

            int countTextColumn = 0;
            List<int> selectDistinctColKey = new List<int>();
            List<string> selectFilterColValue = new List<string>();
            List<string> selectMeasureCol = new List<string>();
            selectMeasureCol.Add("Fact");
            string currentRandomDistinctCol;
        
            do
            {
                if (remK2V[requestDict[requestID].importFile][countTextColumn].Count > 1)
                {
                    selectFilterColValue.Add(remK2V[requestDict[requestID].importFile][countTextColumn][0].ToString());
                    selectDistinctColKey.Add(countTextColumn);
                }
                countTextColumn++;
            } while (countTextColumn < remK2V[requestDict[requestID].importFile].Count);

            Random randomValue = new Random();
            int selectedFilter = randomValue.Next(0, selectDistinctColKey.Count - 1);
            int selectedFromToValue = randomValue.Next(1, remK2V[requestDict[requestID].importFile][selectDistinctColKey[selectedFilter]].Count - 1);
            var selectedFilterCol = selectFilterColValue[selectedFilter].ToString().Trim().Replace(" ", "#");
            var selectedFilterCol2 = selectFilterColValue[selectedFilter].ToString().Trim();

            List<string> sortedValue = new List<string>();
            string addColumnID = columnName2ID[selectedFilterCol2];
            bool success = Int32.TryParse(addColumnID, out int number);
            for (int j = 1; j < remK2V[requestDict[requestID].importFile][number].Count; j++)
                sortedValue.Add(remK2V[requestDict[requestID].importFile][number][j]);

            sortedValue.Sort();

            var selectedFilterColValue = remK2V[requestDict[requestID].importFile][selectDistinctColKey[selectedFilter]][selectedFromToValue].ToString();      

            var selectedStartFilterColValue = sortedValue[0].Replace(":", ";");
            selectedStartFilterColValue = selectedStartFilterColValue.Replace("/", "|");            

            var selectedEndFilterColValue = sortedValue[sortedValue.Count - 1].Replace(":", ";");
            selectedEndFilterColValue = selectedEndFilterColValue.Replace("/", "|");
          
            int loop = 0;
            do
            {
                var currentRandomCol = randomValue.Next(0, selectDistinctColKey.Count);
                currentRandomDistinctCol = remK2V[requestDict[requestID].importFile][selectDistinctColKey[currentRandomCol]][0];

                if (!selectDistinctCol.Contains(currentRandomDistinctCol.Replace(" ", "#")) && remK2V[requestDict[requestID].importFile][selectDistinctColKey[currentRandomCol]].Count > 1)
                {
                    selectDistinctCol.Add(currentRandomDistinctCol.Replace(" ", "#"));
                }
                loop++;
                if (loop > 10) break;

            } while (selectDistinctCol.Count <= selectDistinctColKey.Count && selectDistinctCol.Count < 3);

            StringBuilder findAutoRequest = new StringBuilder();

            if (ramDetail[requestDict[requestID].importFile][0].Count > 200000)
            {
                findAutoRequest.Append("{\"processID\":\"runReport\",\"processButton\": \"drillRow\",\"userID\":\"system\",\"debugOutput\":\"N\",\"pageXlength\":\"20\",\"pageYlength\":\"16\",\"sortingOrder\":\"sortAscending\",\"sortXdimension\":\"A\",\"sortYdimension\":\"A\",\"precisionLevel\":\"Dollar\",\"measureType\":\"sum\",\"column\":[" + selectedFilterCol + "],\"startOption\":[\">=\"], \"startColumnValue\":[" + selectedFilterColValue.ToString() + "],\"endOption\":[\"<=\"],\"endColumnValue\":[" + selectedFilterColValue.ToString() + "],\"distinctDimension\":[");
                selectedStartFilterColValue = selectedFilterColValue.ToString();
                selectedEndFilterColValue = selectedFilterColValue.ToString();
            }
            else
                findAutoRequest.Append("{\"processID\":\"runReport\",\"processButton\": \"drillRow\",\"userID\":\"system\",\"debugOutput\":\"N\",\"pageXlength\":\"20\",\"pageYlength\":\"16\",\"sortingOrder\":\"sortAscending\",\"sortXdimension\":\"A\",\"sortYdimension\":\"A\",\"precisionLevel\":\"Dollar\",\"measureType\":\"sum\",\"column\":[" + selectedFilterCol + "],\"startOption\":[\">=\"], \"startColumnValue\":[" + selectedStartFilterColValue.ToString() + "],\"endOption\":[\"<=\"],\"endColumnValue\":[" + selectedEndFilterColValue.ToString() + "],\"distinctDimension\":[");

            displayRandomSelectedValue.Add(selectedFilterCol);
            displayRandomSelectedValue.Add(selectedStartFilterColValue.ToString());
            displayRandomSelectedValue.Add(selectedEndFilterColValue.ToString());

            for (int i = 0; i < selectDistinctCol.Count; i++)            
            {
                findAutoRequest.Append(selectDistinctCol[i].ToString());

                if (selectDistinctCol.Count > 1 && i < selectDistinctCol.Count - 1)
                    findAutoRequest.Append(",");
            }

            findAutoRequest.Append("],\"crosstabDimension\":[],\"measurement\":[],\"cancelRequestID\":" + requestID.ToString() + "}");

            string autoRequest = findAutoRequest.ToString();          

            Json convert = new Json();
            convert.Json2VariableArray(autoRequest, variable, array);
            convert.Json2VariableList(autoRequest, requestID, variable, array, requestDict);
        }
        public void runReportByCurrentDataset(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, ConcurrentDictionary<int, decimal> currentRequestID, Dictionary<int, Dictionary<string, double>> ramValue2Keygz, string outputFolder, char csvWriteSeparator, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<string, string> _columnName2ID, Dictionary<int, StringBuilder> htmlTable, Dictionary<int, List<double>> _ramDetailgz, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz)
        {
            var start3 = DateTime.Now;
            List<string> addCrosstab2Distinct = new List<string>();            

            for (int i = 0; i < _ramKey2Valuegz.Count; i++)
            {   
                for (int j = 0; j < requestDict[requestID].distinctDimension.Count; j++)
                    if (_ramKey2Valuegz[i][0] == requestDict[requestID].distinctDimension[j].Trim().Replace("#", " "))
                    {
                        addCrosstab2Distinct.Add(_ramKey2Valuegz[i][0]);
                        i++;
                    }

                if (requestDict[requestID].crosstabDimension != null)
                {
                    for (int j = 0; j < requestDict[requestID].crosstabDimension.Count; j++)
                        if (_ramKey2Valuegz[i][0] == requestDict[requestID].crosstabDimension[j].Trim().Replace("#", " "))
                            addCrosstab2Distinct.Add(_ramKey2Valuegz[i][0]);
                }
            }

            requestDict[requestID].distinctDimension.Clear();

            for (int i = 0; i < addCrosstab2Distinct.Count; i++)           
                requestDict[requestID].distinctDimension.Add(addCrosstab2Distinct[i]);

            Dictionary<string, int> selectedColumn = new Dictionary<string, int>();

            for (int i = 0; i < requestDict[requestID].distinctDimension.Count; i++)
                selectedColumn.Add(requestDict[requestID].distinctDimension[i].Trim().Replace("#", " "), i);

            int matchColumn = 0;
            if (requestDict[requestID].distinctOrder != null)
                if(requestDict[requestID].distinctOrder.Count == requestDict[requestID].distinctDimension.Count)
                {
                    for (int i = 0; i < requestDict[requestID].distinctOrder.Count; i++)                        
                        if (selectedColumn.ContainsKey(requestDict[requestID].distinctOrder[i].Trim().Replace("#", " ").ToString()))
                            matchColumn++;

                    if(matchColumn == requestDict[requestID].distinctOrder.Count)
                    {
                        selectedColumn.Clear();
                        for (int i = 0; i < requestDict[requestID].distinctOrder.Count; i++)                      
                            selectedColumn.Add(requestDict[requestID].distinctOrder[i].Trim().Replace("#", " ").ToString(), i);
                    }
                } 
            
            List<string> reorganisedColumn = new List<string>(); // move selected distinct column to 0,1,2 order
            List<string> sourceColumn1 = new List<string>();
            List<string> sourceColumn2 = new List<string>();  

            for (int i = 0; i < _ramKey2Valuegz.Count; i++)
                if (selectedColumn.ContainsKey(_ramKey2Valuegz[i][0]))
                    sourceColumn1.Add(_ramKey2Valuegz[i][0]);               
                else
                    sourceColumn2.Add(_ramKey2Valuegz[i][0]);

            foreach (var pair in selectedColumn)
                reorganisedColumn.Add(pair.Key);

            for (int i = 0; i < sourceColumn2.Count; i++)
                reorganisedColumn.Add(sourceColumn2[i]);

            StringBuilder debug = new StringBuilder();            
           // Dictionary<decimal, List<int>> distinctList2DrillDown = new Dictionary<decimal, List<int>>();
            Dictionary<int, Dictionary<double, string>> ramKey2Valuegz = new Dictionary<int, Dictionary<double, string>>();          
            Dictionary<string, string> columnName2ID = new Dictionary<string, string>();
            Dictionary<int, List<double>> ramDetailgz = new Dictionary<int, List<double>>();                      

            for (int i = 0; i < reorganisedColumn.Count; i++)
                for (int j = 0; j < _ramKey2Valuegz.Count; j++)
                    if (reorganisedColumn[i] == _ramKey2Valuegz[j][0])
                    {                        
                        ramDetailgz[i] = _ramDetailgz[j];
                        ramKey2Valuegz.Add(i, new Dictionary<double, string>());
                        foreach (var pair in _ramKey2Valuegz[j])                        
                            ramKey2Valuegz[i].Add(pair.Key, pair.Value); 
                    }           

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
                columnName2ID.Add(ramKey2Valuegz[i][0], i.ToString());  

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "debug"))
                    Directory.CreateDirectory(outputFolder + "debug");
            }
           
            Dictionary<int, List<string>> selectionCriteria = new Dictionary<int, List<string>>();

            List<int> distinctDimension = new List<int>(); // for muti-dimensional distinct dimension                            
            List<int> crosstabDimension = new List<int>(); // for crosstab dimension (included in distinct dimension)
            validateSelectionCrieteria(); // to validate user confirmed dimension and its criteria; if error for certain cases, will assign factory setting
           
            List<int> yDimension = new List<int>(); // distinct dimension excluding crosstab dimension

            Dictionary<int, string> xyDimension = new Dictionary<int, string>();
            List<int> revisedX = new List<int>(); // excluding other non-distinct dimensions 
            List<int> revisedY = new List<int>(); // excluding other non-distinct dimensions            

            sortSelectedDimensionOrder(); // to sort and group column ID based on selected X Y dimension (by X, by Y and by XY)
           
            // measure
            Dictionary<int, List<int>> measurementColumn = new Dictionary<int, List<int>>();
            Dictionary<int, List<string>> measurementOperator = new Dictionary<int, List<string>>();
            Dictionary<int, List<double>> measurementRange = new Dictionary<int, List<double>>();
            List<int> measure = new List<int>(); // numerical dimensions excluding Date, Code
            Dictionary<int, int> multipleMeasurementColumn = new Dictionary<int, int>();
            bool noMultipleMeasurementColumn = true;
        
            recordMeasurementSelectionCriteria(); // this is used to filter number in Distinct tab
            
            Dictionary<int, Dictionary<double, string>> dimensionCriteria = new Dictionary<int, Dictionary<double, string>>(); // selected code range of particular dimension            

            conditional2ExactMatch(); // preparation work for filterNumber is included in recordMeasurementSelectionCriteria()        
           
            // Distinct List           
            //Dictionary<int, List<double>> distinctList = new Dictionary<int, List<double>>(); // selected X + Y + M dimensions

            // Key to Value
            Dictionary<int, Dictionary<double, string>> distinctRamKey2Value = new Dictionary<int, Dictionary<double, string>>();

            //Checksum
            List<decimal> distinctListChecksum = new List<decimal>(); // record checksum for distinct list         
            Dictionary<decimal, decimal> unsorted2SortedCheksum = new Dictionary<decimal, decimal>();
            List<decimal> distinctSet = new List<decimal>();

            Dictionary<int, string> dataSortingOrder = new Dictionary<int, string>();
            dataSortingOrder[0] = "sortAscending";

            Dictionary<int, string> columnMoveOrder = new Dictionary<int, string>();
            //

            var end3 = DateTime.Now;
            Console.WriteLine("reorgan Time : " + String.Format("{0:F3}", (end3 - start3).TotalSeconds) + " seconds");

            var start2 = DateTime.Now;
            Dictionary<int, Dictionary<double, double>> ramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> ramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
            int order = 0; // sorting based on reorganised and distinct dimension
            for (int i = 0; i < distinctDimension.Count; i++)
            {
                order = 0;
                ramKey2Order.Add(distinctDimension[i], new Dictionary<double, double>());
                ramOrder2Key.Add(distinctDimension[i], new Dictionary<double, double>());
                foreach (var item in ramKey2Valuegz[distinctDimension[i]].OrderBy(j => j.Value))
                {
                    order++;
                    ramKey2Order[distinctDimension[i]].Add(item.Key, order);
                    ramOrder2Key[distinctDimension[i]].Add(order, item.Key);
                }
            }
            var end2 = DateTime.Now;
            Console.WriteLine("Sort Master Time : " + String.Format("{0:F3}", (end2 - start2).TotalSeconds) + " seconds");           

            var startTime = DateTime.Now;
            // filter2distinct();  // X + Y Dimensions

            List<int> ramDetailSegment = new List<int>();

            int segmentThread = 10;
            if(ramDetailgz[0].Count < 1000)
                segmentThread = 1;

            int segment = Convert.ToInt32(Math.Round((double)(ramDetailgz[0].Count / segmentThread), 0)); 

            int line = 1;
            int maxLine = ramDetailgz[0].Count;            
            do
            {
                ramDetailSegment.Add(line);
                line = line + segment;                
                
            } while (line < maxLine);
            ramDetailSegment.Add(maxLine);

            var startDistinct = DateTime.Now;
            
            Dictionary<int, List<double>> distinctList = new Dictionary<int, List<double>>();
            Dictionary<decimal, List<int>> distinctList2DrillDown = new Dictionary<decimal, List<int>>();
            Dictionary<decimal, int> distinctDimensionChecksumList = new Dictionary<decimal, int>();
            ConcurrentDictionary<int, List<decimal>> tempDistinctListChecksum = new ConcurrentDictionary<int, List<decimal>>();
            ConcurrentDictionary<int, Dictionary<int, List<double>>> tempDistinctList = new ConcurrentDictionary<int, Dictionary<int, List<double>>>();
            ConcurrentDictionary<int, Dictionary<decimal, List<int>>> tempDistinctList2DrillDown = new ConcurrentDictionary<int, Dictionary<decimal, List<int>>>();
            ConcurrentDictionary<int, Dictionary<decimal, int>> tempDistinctDimensionChecksumList = new ConcurrentDictionary<int, Dictionary<decimal, int>>();
            ConcurrentQueue<int> checkSegmentThreadCompleted = new ConcurrentQueue<int>();
            int unique = 0;            

            ConcurrentDictionary<int, Thread> concurrentSegmentAddress = new ConcurrentDictionary<int, Thread>(); // a thread manage queue job             

            for (int currentSegment = 0; currentSegment < ramDetailSegment.Count; currentSegment++)
            {
                tempDistinctList.TryAdd(currentSegment, new Dictionary<int, List<double>>());
                tempDistinctList2DrillDown.TryAdd(currentSegment, new Dictionary<decimal, List<int>>());
                tempDistinctDimensionChecksumList.TryAdd(currentSegment, new Dictionary<decimal, int>());
            }

            Parallel.For(0, ramDetailSegment.Count - 1, currentSegment =>
            {
                try // new a thread to manage queue job
                {                   
                    Distinct currentDistinct = new Distinct();
                    concurrentSegmentAddress.TryAdd(currentSegment, new Thread(() => (tempDistinctList[currentSegment], tempDistinctList2DrillDown[currentSegment], tempDistinctListChecksum[currentSegment], tempDistinctDimensionChecksumList[currentSegment]) = currentDistinct.filter2DistinctDB(checkSegmentThreadCompleted, currentSegment, ramDetailSegment, requestID, requestDict, responseDict, measurementColumn, noMultipleMeasurementColumn, measurementOperator, measurementRange, ramDetailgz, ramKey2Valuegz, distinctDimension, measure, dimensionCriteria, crosstabDimension, debug)));
                    concurrentSegmentAddress[currentSegment].Start();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Thread fail '{e}'");
                }
            });

            do
            {
                Thread.Sleep(2);
            } while (checkSegmentThreadCompleted.Count < ramDetailSegment.Count - 1);           

            ExportCSV currentExport = new ExportCSV();

            for (int i = 0; i < (distinctDimension.Count + measure.Count); i++)
            { 
                distinctList.Add(i, new List<double>());
                distinctList[i].Add(0);
            }
            distinctListChecksum.Add(0);

            for (int currentSegment = 0; currentSegment < ramDetailSegment.Count - 1; currentSegment++)
            {
                for (int i = 0; i < tempDistinctListChecksum[currentSegment].Count; i++)  // crash
                {                   
                    if (!distinctDimensionChecksumList.ContainsKey(tempDistinctListChecksum[currentSegment][i]))
                    {
                        unique++;
                        distinctDimensionChecksumList.Add(tempDistinctListChecksum[currentSegment][i], unique);
                        distinctListChecksum.Add(tempDistinctListChecksum[currentSegment][i]);

                        for (int d = 0; d < distinctDimension.Count; d++)
                            distinctList[d].Add(tempDistinctList[currentSegment][d][i]); // add dimension value for first unique item                       

                        for (int d = distinctDimension.Count; d < (distinctDimension.Count + measure.Count); d++)
                            distinctList[d].Add(tempDistinctList[currentSegment][d][i]);                      

                        if (requestDict[requestID].processButton == "drillRow")
                        {
                            distinctList2DrillDown.Add(tempDistinctListChecksum[currentSegment][i], new List<int>());
                            distinctList2DrillDown[tempDistinctListChecksum[currentSegment][i]].AddRange(tempDistinctList2DrillDown[currentSegment][tempDistinctListChecksum[currentSegment][i]]);
                        }
                    }
                    else // addition = current value + last value of the same key
                    {
                        var add = distinctDimensionChecksumList[tempDistinctListChecksum[currentSegment][i]]; // return unique line number  

                        for (int d = distinctDimension.Count; d < (distinctDimension.Count + measure.Count); d++)                        
                            distinctList[d][add] = Math.Round((Double)(distinctList[d][add] + tempDistinctList[currentSegment][d][i]), 2); // sum measure column by dimension column                         

                        if (requestDict[requestID].processButton == "drillRow")
                            distinctList2DrillDown[tempDistinctListChecksum[currentSegment][i]].AddRange(tempDistinctList2DrillDown[currentSegment][tempDistinctListChecksum[currentSegment][i]]);
                    }
                }
            }  

            if (requestDict[requestID].debugOutput == "Y")
                currentExport.ramDistinct2CSV(distinctList, ramKey2Valuegz, csvWriteSeparator, outputFolder + "\\" + "debug", "distinctList-numberOnly" + ".csv");

            // reorganize master (key to value) for distinctList
            for (int i = 0; i < distinctDimension.Count; i++)
                distinctRamKey2Value[i] = ramKey2Valuegz[distinctDimension[i]];

            for (int i = distinctDimension.Count; i < (distinctDimension.Count + measure.Count); i++)
                distinctRamKey2Value[i] = ramKey2Valuegz[measure[i - distinctDimension.Count]];

            // export to csv
            if (requestDict[requestID].debugOutput == "Y")
                currentExport.ramDistinct2CSVymTable(distinctList, distinctRamKey2Value, csvWriteSeparator, outputFolder, "\\" + "debug" + "\\" + "distinctList" + ".csv");

            responseDict[requestID].endDistinctTime = DateTime.Now;            
            Console.WriteLine("Total Distinct Time : " + String.Format("{0:F3}", (responseDict[requestID].endDistinctTime - startDistinct).TotalSeconds) + " seconds");

            int xAddressCol = distinctList.Count; // to store x coordinate of crosstab
            int yAddressCol = distinctList.Count + 1; // to store y coordinate of crosstab            

            ConcurrentDictionary<decimal, Thread> presentationThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage presentation
            Dictionary<int, int> stopPresentationThread = new Dictionary<int, int>();
            int presentationJob = 0;
            int moveColumnID = 0;
            string startRotateDimension = "N";
            string startMoveDimension = "N";

            presentationJob++;
            
            try
            {
                Presentation newPresentation = new Presentation();
                presentationThread.TryAdd(presentationJob, new Thread(() => newPresentation.presentationOfData(ramDetailgz, userPreference, tableFact, remK2V, sourceFolder, db1Folder, screenControl, distinctDimensionChecksumList, distinctListChecksum, distinctSet, unsorted2SortedCheksum, distinctDimension, xyDimension, crosstabDimension, yDimension, _ramKey2Valuegz, ramKey2Valuegz, revisedX, revisedY, requestID, requestDict, debug, outputFolder, responseDict, distinctRamKey2Value, ramKey2Order, ramOrder2Key, distinctList, htmlTable, measure, csvWriteSeparator, dataSortingOrder, xAddressCol, yAddressCol, presentationJob, stopPresentationThread, presentationThread, columnMoveOrder, moveColumnID, startRotateDimension, columnName2ID, startMoveDimension, distinctList2DrillDown)));
                presentationThread[presentationJob].Start();
                stopPresentationThread[presentationJob] = presentationThread.Count;
            }
            catch (Exception e)
            {
                Console.WriteLine($"presentationThread fail '{e}'");
            }          

            InterimEvent currentEvent = new InterimEvent();           
            currentEvent.postDistinctEvent(ramDetailgz, userPreference, tableFact, remK2V, sourceFolder, db1Folder, screenControl, distinctDimensionChecksumList, currentRequestID, distinctListChecksum, distinctSet, unsorted2SortedCheksum, distinctDimension, xyDimension, requestID, requestDict, responseDict, presentationJob, dataSortingOrder, presentationThread, crosstabDimension, yDimension, _ramKey2Valuegz, ramKey2Valuegz, revisedX, revisedY, debug, outputFolder, distinctRamKey2Value, stopPresentationThread, ramKey2Order, ramOrder2Key, distinctList, htmlTable, measure, csvWriteSeparator, xAddressCol, yAddressCol, columnName2ID, columnMoveOrder, startRotateDimension, startMoveDimension, distinctList2DrillDown);      
            // Common Functions for both routes

            void validateSelectionCrieteria()
            {                
                selectionCriteria.Add(0, new List<string>()); // column
                selectionCriteria.Add(1, new List<string>()); // startOption
                selectionCriteria.Add(2, new List<string>()); // startColumnValue
                selectionCriteria.Add(3, new List<string>()); // endOption
                selectionCriteria.Add(4, new List<string>()); // endColumnValue
                selectionCriteria.Add(5, new List<string>()); // distinctDimension
                selectionCriteria.Add(6, new List<string>()); // crosstabDimension
                selectionCriteria.Add(7, new List<string>()); // measurement              

                for (int i = 0; i < requestDict[requestID].column.Count; i++)
                {
                    selectionCriteria[0].Add(columnName2ID[requestDict[requestID].column[i].Replace("#", " ")]); //convert column name to ID
                    selectionCriteria[1].Add(requestDict[requestID].startOption[i]);

                    var startColumn = requestDict[requestID].startColumnValue[i].Replace(";", ":");
                    startColumn = startColumn.Replace("|", "/");
                    selectionCriteria[2].Add(startColumn);

                    selectionCriteria[3].Add(requestDict[requestID].endOption[i]);

                    var endColumn = requestDict[requestID].endColumnValue[i].Replace(";", ":");
                    endColumn = endColumn.Replace("|", "/");
                    selectionCriteria[4].Add(endColumn);
                }               

                // selection criteria = filter (0 ~ 4) + distinct dimension (5) + crosstab dimension (6)
                for (int i = 0; i < requestDict[requestID].distinctDimension.Count; i++)
                    selectionCriteria[5].Add(columnName2ID[requestDict[requestID].distinctDimension[i].Replace("#", " ")]); // add filter column ID

                // selectionCriteria[6] => crosstab dimension (string)
                if (requestDict[requestID].crosstabDimension != null)
                {
                    for (int i = 0; i < requestDict[requestID].crosstabDimension.Count; i++)
                        selectionCriteria[6].Add(columnName2ID[requestDict[requestID].crosstabDimension[i].Replace("#", " ")]);
                }

                List<Double> validateLogElementCount = new List<double>();
                List<Double> validateFactor = new List<double>();

                // distinct dimension (number)
                for (int i = 0; i < selectionCriteria[5].Count; i++)
                {
                    bool success = Int32.TryParse(selectionCriteria[5][i], out int column);
                    distinctDimension.Add(column);
                }

                // crosstab dimension (number)
                for (int i = 0; i < selectionCriteria[6].Count; i++)
                {
                    bool success = Int32.TryParse(selectionCriteria[6][i], out int column);
                    crosstabDimension.Add(column);
                    if (!distinctDimension.Contains(column))
                    {
                        distinctDimension.Add(column);
                        selectionCriteria[5].Add(column.ToString());
                    }
                }

                decimal validateDistinctDimensionChecksum = 0;

                for (int i = distinctDimension.Count - 1; i > 0; i--)
                {
                    if (i == distinctDimension.Count - 1) validateLogElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[distinctDimension[i]].Count, 10) + 0.5000001), 0));
                    if (i < distinctDimension.Count - 1) validateLogElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[distinctDimension[i]].Count, 10) + 0.5000001), 0) + validateLogElementCount[distinctDimension.Count - 2 - i]);
                }

                for (int i = 0; i < (distinctDimension.Count - 1); i++)
                    validateFactor.Add(Math.Pow(10, validateLogElementCount[i]));

                if (validateFactor.Count > 0) // > 1 column
                {  
                    int dd = 0;
                    do
                    {
                        if (dd < distinctDimension.Count - 1)
                        {
                            if (Math.Ceiling(Math.Log(Convert.ToDouble(validateDistinctDimensionChecksum) + Convert.ToDouble(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1] * validateFactor[distinctDimension.Count - 2 - dd]), 10)) < 29)
                                validateDistinctDimensionChecksum = validateDistinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1] * validateFactor[distinctDimension.Count - 2 - dd]);
                            else
                            {
                                removeOneDistinctDimension();
                                removeOneDistinctDimension();
                                removeOneDistinctDimension();
                                validateDistinctDimensionChecksum = 0;
                                dd = -1;
                            }
                        }

                        if (dd == distinctDimension.Count - 1)
                        {
                            if (Math.Ceiling(Math.Log(Convert.ToDouble(validateDistinctDimensionChecksum) + Convert.ToDouble(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1]), 10)) < 29)
                            {
                                validateDistinctDimensionChecksum = validateDistinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[distinctDimension[dd]][ramDetailgz[0].Count - 1]);
                            }
                            else
                            {
                                removeOneDistinctDimension();
                                removeOneDistinctDimension();
                                removeOneDistinctDimension();
                                validateDistinctDimensionChecksum = 0;
                                dd = -1;
                            }
                        }
                        dd++;
                    } while (dd <= distinctDimension.Count);

                    void removeOneDistinctDimension()
                    {
                        var last = distinctDimension.Count - 1;

                        if (crosstabDimension.Contains(distinctDimension[last]))
                        {
                            var lastCrossTab = crosstabDimension.Count - 1;
                            crosstabDimension.RemoveAt(lastCrossTab);
                            var sc6last = selectionCriteria[6].Count - 1;
                            selectionCriteria[6].RemoveAt(sc6last);
                        }
                        if (!crosstabDimension.Contains(distinctDimension[last]))
                        {
                            var sc5last = selectionCriteria[5].Count - 1;
                            selectionCriteria[5].RemoveAt(sc5last);
                        }
                        distinctDimension.RemoveAt(last);
                    }                                 
                }
            }

            void sortSelectedDimensionOrder()
            {
                distinctDimension.Sort();

                if (requestDict[requestID].debugOutput == "Y")
                {
                    debug.Append(Environment.NewLine);
                    debug.Append("Sorted distinctDimension: ");
                    foreach (int d in distinctDimension) debug.Append(d + " ");
                    debug.Append(Environment.NewLine); debug.Append(Environment.NewLine);
                }

                // y dimension (number)
                for (int i = 0; i < selectionCriteria[5].Count; i++)
                {
                    bool success = Int32.TryParse(selectionCriteria[5][i], out int column);
                    if (!crosstabDimension.Contains(column))
                        yDimension.Add(column);
                }

                // xy dimension (number) 
                for (int i = 0; i < crosstabDimension.Count; i++)
                    xyDimension.Add(crosstabDimension[i], "x");

                for (int i = 0; i < yDimension.Count; i++)
                    xyDimension.Add(yDimension[i], "y");

                int empty = 0;
                for (int i = 0; i < xyDimension.Count + empty; i++)
                {
                    if (!xyDimension.ContainsKey(i))
                        empty++;

                    if (xyDimension.ContainsKey(i))
                    {
                        if (xyDimension[i] == "x")
                            revisedX.Add((i - empty));

                        if (xyDimension[i] == "y")
                            revisedY.Add((i - empty));
                    }
                }

                if (requestDict[requestID].debugOutput == "Y")
                {
                    debug.Append("crosstabDimension: ");
                    for (int i = 0; i < crosstabDimension.Count; i++)
                        debug.Append(crosstabDimension[i] + " ");

                    debug.Append(" yDimension: ");
                    for (int i = 0; i < yDimension.Count; i++)
                        debug.Append(yDimension[i] + " ");

                    debug.Append(" revisedX ");
                    for (int i = 0; i < revisedX.Count; i++)
                        debug.Append(revisedX[i] + " ");

                    debug.Append(" revisedY: ");
                    for (int i = 0; i < revisedY.Count; i++)
                        debug.Append(revisedY[i] + " ");

                    debug.Append(Environment.NewLine);
                }

            }

            void recordMeasurementSelectionCriteria()
            {                
                measurementColumn.Add(0, new List<int>());
                
                measurementOperator.Add(0, new List<string>());
                measurementOperator.Add(1, new List<string>());
                
                measurementRange.Add(0, new List<double>());
                measurementRange.Add(1, new List<double>());
                measurementRange.Add(2, new List<double>());

                for (int i = 0; i < requestDict[requestID].column.Count; i++)
                {
                    bool successCol = Int32.TryParse(selectionCriteria[0][i], out int measurementCol); // convert web input string to number                                                                 

                    if (ramKey2Valuegz[measurementCol].Count == 1) // if a column has column name only (no master record), assume it is a numerical column
                    {
                        measurementColumn[0].Add(measurementCol); //convert column name to ID                       

                        if (multipleMeasurementColumn.ContainsKey(measurementCol))  // if multiple, will affect filter number route
                            noMultipleMeasurementColumn = noMultipleMeasurementColumn && false;

                        if (!multipleMeasurementColumn.ContainsKey(measurementCol))
                            multipleMeasurementColumn.Add(measurementCol, measurementCol);


                        measurementOperator[0].Add(requestDict[requestID].startOption[i]); // add data from requestDict object directly
                        measurementOperator[1].Add(requestDict[requestID].endOption[i]); // add data from requestDict object directly

                        bool successStart = Int32.TryParse(selectionCriteria[2][i], out int startNum); // convert web input string to number                                  
                        bool successEnd = Int32.TryParse(selectionCriteria[4][i], out int endNum); // convert web input string to number                                  

                        measurementRange[0].Add(startNum);
                        measurementRange[1].Add(endNum);
                    }
                }

                List<string> addMeasurement = new List<string>();
                addMeasurement.Add("Fact");

                if (crosstabDimension.Count == 0)               
                    if (requestDict[requestID].measurement == null)                    
                        requestDict[requestID].measurement = addMeasurement;

                if (crosstabDimension.Count > 0)                
                    if (requestDict[requestID].measurement == null)                   
                        requestDict[requestID].measurement = addMeasurement;         

                for (int i = 0; i < requestDict[requestID].measurement.Count; i++)
                {                    
                    if(columnName2ID.ContainsKey(requestDict[requestID].measurement[i].Replace("#", " ")))
                        selectionCriteria[7].Add(columnName2ID[requestDict[requestID].measurement[i].Replace("#", " ")]);
                }

                for (int i = 0; i < selectionCriteria[7].Count; i++)
                {
                    bool success = Int32.TryParse(selectionCriteria[7][i], out int column);
                    measure.Add(column);
                }
            }

            void conditional2ExactMatch()
            {
                // Convert from conditional match label to exact match 
                for (int i = 0; i < ramDetailgz.Count; i++)
                    dimensionCriteria.Add(i, new Dictionary<double, string>());

                for (int i = 0; i < selectionCriteria[0].Count; i++)
                {
                    bool success = Int32.TryParse(selectionCriteria[0][i], out int filterColumn); // convert web input string to number                                  

                    if (ramKey2Valuegz[filterColumn].Count > 1) // dimension
                    {
                        for (double j = 1; j < ramKey2Valuegz[filterColumn].Count; j++)
                        {
                            if (selectionCriteria[1][i].ToString().Replace(" ", "") == "=" && selectionCriteria[3][i].ToString() == "=")
                                if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) == 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) == 0) // =0 means =="L60"
                                    conditional2exact();

                            if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">" && selectionCriteria[3][i].ToString() == "<")
                                if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) > 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) < 0)
                                    conditional2exact();

                            if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">=" && selectionCriteria[3][i].ToString() == "<=")
                                if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[2][i]) >= 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) <= 0)
                                    conditional2exact();

                            if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">" && selectionCriteria[3][i].ToString() == "<=")
                                if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) > 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) <= 0)
                                    conditional2exact();

                            if (selectionCriteria[1][i].ToString().Replace(" ", "") == ">=" && selectionCriteria[3][i].ToString() == "<")
                                if (string.Compare(ramKey2Valuegz[filterColumn][j].ToString(), selectionCriteria[2][i]) >= 0 && string.Compare(ramKey2Valuegz[filterColumn][j].ToString().Replace(" ", ""), selectionCriteria[4][i]) < 0)
                                    dimensionCriteria[filterColumn].Add(j, ramKey2Valuegz[filterColumn][j]); // output criteria dictionary for filtering data                   

                            void conditional2exact()
                            {
                                dimensionCriteria[filterColumn].Add(j, ramKey2Valuegz[filterColumn][j]); // output criteria dictionary for filtering data
                            }
                        }
                    }
                }
            }            
        }
    }       
}
