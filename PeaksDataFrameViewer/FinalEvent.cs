using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace youFast
{
    public class FinalEvent
    {
        public void reportEvent(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, List<decimal> distinctSet, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, int YpageStart, int YpageEnd, int Yrow, int Ypage, double YtotalPage,  Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, string outputFolder, char csvWriteSeparator, Dictionary<int, string> dataSortingOrder, int presentationJob, ConcurrentDictionary<decimal, Thread> presentationThread, Dictionary<decimal, List<int>> distinctList2DrillDown)        
        {
            var startEventTime = DateTime.Now;
            var endEventTime = DateTime.Now;
            double timeout = 0;
            List<string> nextPrecision = new List<string>();
            nextPrecision.Add("Thousand");
            nextPrecision.Add("Million");
            nextPrecision.Add("Cent");
            nextPrecision.Add("Dollar");          

            int nextLevel = 0;
            int lastYPage = 0;
            int lastYPageDrill = 0;
            int pageEndDownToggle = 0;
            int pageEndDownToggleDrill = 0;
            int pageHomeToggle = 0;
            int pageHomeToggleDrill = 0;

            double _YtotalPageDrill = 1;
            int YtotalPageDrill = 1;
            int YpageDrill = 1;
            int XYmaxRowDrill = 1;
            int YrowDrill = 1;
            int YpageStartDrill = 1;
            int YpageEndDrill = 1;
            List<int> _pageYlength = new List<int>();            
            _pageYlength.Add(1);
            List<int> _drillSet = new List<int>();
            _drillSet.Add(0);

            do
            {
                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].drillType == "set")
                {
                    requestDict[2].drillType = "null";

                    if (requestDict.ContainsKey(2) && requestDict[2].drillSet > 0)
                    {
                        XYmaxRowDrill = requestDict[2].pageYlength;
                        _pageYlength[0] = requestDict[2].pageYlength;

                        _drillSet[0] = Convert.ToInt32(requestDict[2].drillSet);

                    }
                    else
                        XYmaxRowDrill = _pageYlength[0];

                    YrowDrill = XYmaxRowDrill - 1;
                    _YtotalPageDrill = (distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count - 1) / YrowDrill;
                    YtotalPageDrill = (int)Math.Floor(_YtotalPageDrill) + 1;

                    YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                    if ((YpageDrill - 1) < YtotalPageDrill)
                        YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                    else
                        YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 2)) + ((distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count - 1) % YrowDrill);

                    if (YpageDrill >= YtotalPageDrill)
                        YpageDrill = YtotalPageDrill;                                       

                    if (requestDict[2].direction == "pageHome")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        if (YpageDrill > 1) lastYPageDrill = YpageDrill;
                        YpageDrill = 1;

                        pageHomeToggleDrill++;
                        if (pageHomeToggleDrill >= 2 && lastYPageDrill > 0)
                        {
                            YpageDrill = lastYPageDrill;
                            pageHomeToggleDrill = 0;

                            if (YpageDrill == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                        YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                        }

                        ExportHTML currentExport1 = new ExportHTML();

                        YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                        if ((YpageDrill - 1) < YtotalPageDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count >= YrowDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if (YpageDrill == YtotalPageDrill + 1)
                            YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;

                        currentExport1.drillDown2HtmlYMtable(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctSet, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "null")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        YrowDrill = requestDict[2].pageYlength - 1;
                        if (YpageDrill > 1) lastYPageDrill = YpageDrill;
                        YpageDrill = 1;

                        pageHomeToggleDrill++;
                        if (pageHomeToggleDrill >= 2 && lastYPageDrill > 0)
                        {
                            YpageDrill = lastYPageDrill;
                            pageHomeToggleDrill = 0;

                            if (YpageDrill == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                        YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                        }

                        ExportHTML currentExport1 = new ExportHTML();

                        YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                        if ((YpageDrill - 1) < YtotalPageDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count >= YrowDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if (YpageDrill == YtotalPageDrill + 1)
                            YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;

                        currentExport1.drillDown2HtmlYMtable(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctSet, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "pageEndDown")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        if (YpageDrill < Convert.ToInt32(YtotalPageDrill)) 
                            lastYPageDrill = YpageDrill;

                        YpageDrill = Convert.ToInt32(YtotalPageDrill);
                        pageEndDownToggleDrill++;
                        if (pageEndDownToggleDrill >= 2)
                        {
                            Ypage = lastYPageDrill;
                            pageEndDownToggleDrill = 0;

                            if (Ypage == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                        YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                        }
                        if (Ypage >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count >= YrowDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (Ypage - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtable(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctSet, distinctList2DrillDown, ramKey2Valuegz);
                    }                   

                    if (requestDict[2].direction == "Up" && YpageDrill > 1)
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        YpageDrill = YpageDrill - 1;
                        

                        if (requestDict[requestID].debugOutput == "Y")
                            Console.WriteLine("YpageDrill " + YpageDrill + " YtotalPageDrill " + YtotalPageDrill + " YpageEndDrill " + YpageEndDrill);

                        if (YpageDrill == 1)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            else
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if (YpageDrill >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count >= YrowDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtable(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctSet, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "Down" && YpageDrill < YtotalPageDrill)
                    {

                        startEventTime = DateTime.Now;
                        requestDict[2].direction = "";
                        YpageDrill++;
                        if (requestDict[requestID].debugOutput == "Y")
                            Console.WriteLine("YpageDrill " + YpageDrill + " YtotalPageDrill " + YtotalPageDrill + " YpageEndDrill " + YpageEndDrill);

                        if (YpageDrill == 1)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            else
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }
                        if (YpageDrill >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count >= Yrow)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                                    YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtable(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctSet, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].openReport == "Y")
                    {
                        startEventTime = DateTime.Now;
                        requestDict[2].openReport = "";
                        ExportCSV currentExport1 = new ExportCSV();

                        int fileNo = 1;
                        bool fileExist = false;

                        do
                        {
                            fileExist = false;
                            string[] sourceFile2 = Directory.GetFiles(outputFolder.ToString(), "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                            foreach (string filePath in sourceFile2)
                            {

                                if (filePath == outputFolder.ToString() + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile)
                                {
                                    fileNo++;
                                    fileExist = true;
                                }
                                else
                                    fileExist = false;
                            }

                        } while (fileExist == true);

                        currentExport1.drillDown2CSVymTable(ramDetailgz, distinctSet, distinctList2DrillDown, ramKey2Valuegz, csvWriteSeparator, outputFolder, "\\" + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                        if (!Directory.Exists(outputFolder))
                            Directory.CreateDirectory(outputFolder);

                        FileInfo fi2 = new FileInfo(@outputFolder + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);
                        if (fi2.Exists)
                        {
                            try
                            {
                                using (Process exeProcess = Process.Start(@outputFolder + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile))
                                {
                                    exeProcess.WaitForExit();
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"please close file '{e}'");
                            }
                        }
                        else
                        {
                            //file doesn't exist
                        }
                    }
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "pageHome" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    if (Ypage > 1) lastYPage = Ypage;
                    Ypage = 1;

                    pageHomeToggle++;
                    if (pageHomeToggle >= 2 && lastYPage > 0)
                    {
                        Ypage = lastYPage;
                        pageHomeToggle = 0;

                        if (Ypage == 1)
                        {
                            YpageStart = 1 + (Yrow * (Ypage - 1));
                            if ((Ypage - 1) < YtotalPage)
                            {
                                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                                if (YpageEnd >= distinctList[0].Count)
                                    YpageEnd = distinctList[0].Count;
                            }
                            else
                                YpageEnd = distinctList[0].Count;
                        }
                    }

                    ExportHTML currentExport1 = new ExportHTML();

                    YpageStart = 1 + (Yrow * (Ypage - 1));
                    if ((Ypage - 1) < YtotalPage)
                    {
                        YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                        if (YpageEnd >= distinctList[0].Count)
                            YpageEnd = distinctList[0].Count;
                    }
                    if ((Ypage - 1) <= YtotalPage && distinctList[0].Count >= Yrow)
                    {
                        YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                        if (YpageEnd >= distinctList[0].Count)
                            YpageEnd = distinctList[0].Count;
                    }
                    if (Ypage == YtotalPage + 1)
                        YpageEnd = distinctList[0].Count;

                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "pageEndDown" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;

                    requestDict[2].direction = "";
                    if (Ypage < Convert.ToInt32(YtotalPage)) lastYPage = Ypage;
                    Ypage = Convert.ToInt32(YtotalPage);
                    pageEndDownToggle++;
                    if (pageEndDownToggle >= 2)
                    {
                        Ypage = lastYPage;
                        pageEndDownToggle = 0;

                        if (Ypage == 1)
                        {
                            YpageStart = 1 + (Yrow * (Ypage - 1));
                            if ((Ypage - 1) < YtotalPage)
                            {
                                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                                if (YpageEnd >= distinctList[0].Count)
                                    YpageEnd = distinctList[0].Count;
                            }
                            else
                                YpageEnd = distinctList[0].Count;
                        }
                    }
                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && distinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = distinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Up" && Ypage > 1 && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Ypage = Ypage - 1;

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Ypage " + Ypage + " YtotalPage " + YtotalPage + " YpageEnd " + YpageEnd);

                    if (Ypage == 1)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        else
                            YpageEnd = distinctList[0].Count;
                    }
                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && distinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = distinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Down" && Ypage < YtotalPage && requestDict[2].drillType == "null")
                {

                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Ypage++;
                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Ypage " + Ypage + " YtotalPage " + YtotalPage + " YpageEnd " + YpageEnd);

                    if (Ypage == 1)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        else
                            YpageEnd = distinctList[0].Count;
                    }
                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && distinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= distinctList[0].Count)
                                YpageEnd = distinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = distinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }


                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].precisionLevel == "Next" && requestDict[2].drillType == "null")
                {
                    requestDict[2].precisionLevel = "";
                    requestDict[requestID].precisionLevel = nextPrecision[nextLevel];
                    nextLevel++;
                    if (nextLevel == 4) nextLevel = 0;
                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].drillDownEventType == "Next" && requestDict[2].drillType == "null")                
                {
                    requestDict[2].drillDownEventType = "";
                    userPreference["system"].nextDrillDownEventType++;                   
                    if (userPreference["system"].nextDrillDownEventType == 2)
                        userPreference["system"].nextDrillDownEventType = 0;

                    if (userPreference["system"].nextDrillDownEventType == 0)
                        userPreference["system"].drillDownEventType = "onmouseenter";
                        

                    if (userPreference["system"].nextDrillDownEventType == 1)
                        userPreference["system"].drillDownEventType = "onclick";
                    
                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, distinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
                }


                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].openReport == "Y" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].openReport = "";
                    ExportCSV currentExport1 = new ExportCSV();

                    int fileNo = 1;
                    bool fileExist = false;

                    do
                    {
                        fileExist = false;
                        string[] sourceFile = Directory.GetFiles(outputFolder.ToString(), "distinct" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                        foreach (string filePath in sourceFile)
                        {

                            if (filePath == outputFolder.ToString() + "distinct" + fileNo.ToString() + "_" + requestDict[requestID].importFile)
                            {
                                fileNo++;
                                fileExist = true;
                            }
                            else
                                fileExist = false;
                        }

                    } while (fileExist == true);


                    ConcurrentDictionary<decimal, Thread> exportThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage queue job   


                    try // new a thread to manage queue job
                    {
                        exportThread.TryAdd(requestID, new Thread(() => currentExport1.drillDown2CSVymTable(ramDetailgz, distinctSet, distinctList2DrillDown, ramKey2Valuegz, csvWriteSeparator, outputFolder, "\\" + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile)));
                        exportThread[requestID].Start();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"queueThread fail '{e}'");
                    }

                    currentExport1.ramDistinct2CSVymTable(distinctList, distinctRamKey2Value, csvWriteSeparator, outputFolder, "\\" + "distinct" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                    if (!Directory.Exists(outputFolder))
                        Directory.CreateDirectory(outputFolder);

                    FileInfo fi = new FileInfo(@outputFolder + "distinct" + fileNo.ToString() + "_" + requestDict[requestID].importFile);
                    if (fi.Exists)
                    {
                        try
                        {
                            using (Process exeProcess = Process.Start(@outputFolder + "distinct" + fileNo.ToString() + "_" + requestDict[requestID].importFile))
                            {
                                exeProcess.WaitForExit();
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"please close file '{e}'");
                        }
                    }
                    else
                    {
                        //file doesn't exist
                    }
                }

                endEventTime = DateTime.Now;

                timeout = (Convert.ToDouble(endEventTime.ToOADate()) - Convert.ToDouble(startEventTime.ToOADate())) * 100000;
                // Console.WriteLine(timeout);

                Thread.Sleep(10);
            } while (timeout < 10000 && presentationThread.Count == presentationJob);
        }

        public void crosstabReportEvent(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, Dictionary<int, List<double>> YXMdistinctDrillKey, Dictionary<double, decimal> crosstabAddress2DrillSetDict, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<decimal, List<int>> distinctList2DrillDown, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, int Xpage, int XpageStart, int XpageEnd, int Xcolumn, int yHeaderCol, int YpageStart, int YpageEnd, int Yrow, int Ypage, double YtotalPage, double XtotalPage, Dictionary<int, StringBuilder> htmlTable, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, Dictionary<int, List<double>> YXMdistinctList, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, string outputFolder, List<int> revisedY, List<int> measure, char csvWriteSeparator, Dictionary<int, string> dataSortingOrder, int presentationJob, Dictionary<int, int> stopPresentationThread, ConcurrentDictionary<decimal, Thread> presentationThread)
        {
            var startEventTime = DateTime.Now;
            var endEventTime = DateTime.Now;
            double timeout = 0;
            List<string> nextPrecision = new List<string>();
            nextPrecision.Add("Thousand");
            nextPrecision.Add("Million");
            nextPrecision.Add("Cent");
            nextPrecision.Add("Dollar");
            int nextLevel = 0;
            int lastYPage = 0;
            int lastYPageDrill = 0;
            int lastXPage = 0;
            int pageEndDownToggle = 0;
            int pageEndDownToggleDrill = 0;
            int pageEndRightToggle = 0;
            int pageHomeToggle = 0;
            int pageHomeToggleDrill = 0;

            double _YtotalPageDrill = 1;
            int YtotalPageDrill = 1;
            int YpageDrill = 1;
            int XYmaxRowDrill = 1;
            int YrowDrill = 1;
            int YpageStartDrill = 1;
            int YpageEndDrill = 1;
            List<int> _pageYlength = new List<int>();
            _pageYlength.Add(1);
            List<decimal> _drillSet = new List<decimal>();
            _drillSet.Add(0);           
           
            do
            {
                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].drillType == "set")
                { 
                    requestDict[2].drillType = "null";                    

                    if (requestDict.ContainsKey(2) && requestDict[2].drillSet > 0)
                    {                        
                        XYmaxRowDrill = requestDict[2].pageYlength;
                        _pageYlength[0] = requestDict[2].pageYlength;

                        _drillSet[0] = requestDict[2].drillSet;

                    }
                    else
                        XYmaxRowDrill = _pageYlength[0];

                    YrowDrill = XYmaxRowDrill - 1;                   
                    _YtotalPageDrill = (distinctList2DrillDown[_drillSet[0]].Count - 1) / YrowDrill;

                    YtotalPageDrill = (int)Math.Floor(_YtotalPageDrill) + 1;

                    YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                    if ((YpageDrill - 1) < YtotalPageDrill)
                        YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                    else
                        YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 2)) + ((distinctList2DrillDown[_drillSet[0]].Count - 1) % YrowDrill);

                    if (YpageDrill >= YtotalPageDrill)
                        YpageDrill = 0;

                    if (requestDict[2].direction == "pageHome")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        if (YpageDrill > 1) lastYPageDrill = YpageDrill;
                        YpageDrill = 1;

                        pageHomeToggleDrill++;
                        if (pageHomeToggleDrill >= 2 && lastYPageDrill > 0)
                        {
                            YpageDrill = lastYPageDrill;
                            pageHomeToggleDrill = 0;

                            if (YpageDrill == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                        YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                        }

                        ExportHTML currentExport1 = new ExportHTML();

                        YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                        if ((YpageDrill - 1) < YtotalPageDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= YrowDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if (YpageDrill == YtotalPageDrill + 1)
                            YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;                              

                       currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);                      
                    }

                    if (requestDict[2].direction == "pageHome")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        if (YpageDrill > 1) lastYPageDrill = YpageDrill;
                        YpageDrill = 1;

                        pageHomeToggleDrill++;
                        if (pageHomeToggleDrill >= 2 && lastYPageDrill > 0)
                        {
                            YpageDrill = lastYPageDrill;
                            pageHomeToggleDrill = 0;

                            if (YpageDrill == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                        YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                        }

                        ExportHTML currentExport1 = new ExportHTML();

                        YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                        if ((YpageDrill - 1) < YtotalPageDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= YrowDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if (YpageDrill == YtotalPageDrill + 1)
                            YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;

                        currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "null")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        YrowDrill = requestDict[2].pageYlength - 1;
                        if (YpageDrill > 1) lastYPageDrill = YpageDrill;
                        YpageDrill = 1;

                        pageHomeToggleDrill++;
                        if (pageHomeToggleDrill >= 2 && lastYPageDrill > 0)
                        {
                            YpageDrill = lastYPageDrill;
                            pageHomeToggleDrill = 0;

                            if (YpageDrill == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                        YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                        }

                        ExportHTML currentExport1 = new ExportHTML();

                        YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                        if ((YpageDrill - 1) < YtotalPageDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= YrowDrill)
                        {
                            YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                            if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if (YpageDrill == YtotalPageDrill + 1)
                            YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;

                        currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "pageEndDown")
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        if (YpageDrill < Convert.ToInt32(YtotalPageDrill)) lastYPageDrill = YpageDrill;
                        YpageDrill = Convert.ToInt32(YtotalPageDrill);
                        pageEndDownToggleDrill++;
                        if (pageEndDownToggleDrill >= 2)
                        {
                            Ypage = lastYPageDrill;
                            pageEndDownToggleDrill = 0;

                            if (Ypage == 1)
                            {
                                YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                                if ((YpageDrill - 1) < YtotalPageDrill)
                                {
                                    YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                    if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                        YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                                }
                                else
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                        }
                        if (Ypage >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= YrowDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (Ypage - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "Up" && YpageDrill > 1)
                    {
                        startEventTime = DateTime.Now;

                        requestDict[2].direction = "";
                        YpageDrill = YpageDrill - 1;

                        if (requestDict[requestID].debugOutput == "Y")
                            Console.WriteLine("YpageDrill " + YpageDrill + " YtotalPageDrill " + YtotalPageDrill + " YpageEndDrill " + YpageEndDrill);

                        if (YpageDrill == 1)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            else
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if (YpageDrill >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= YrowDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].direction == "Down" && YpageDrill < YtotalPageDrill)
                    {

                        startEventTime = DateTime.Now;
                        requestDict[2].direction = "";
                        YpageDrill++;
                        if (requestDict[requestID].debugOutput == "Y")
                            Console.WriteLine("YpageDrill " + YpageDrill + " YtotalPageDrill " + YtotalPageDrill + " YpageEndDrill " + YpageEndDrill);

                        if (YpageDrill == 1)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            else
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }
                        if (YpageDrill >= 2)
                        {
                            YpageStartDrill = 1 + (YrowDrill * (YpageDrill - 1));
                            if ((YpageDrill - 1) < YtotalPageDrill)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if ((YpageDrill - 1) <= YtotalPageDrill && distinctList2DrillDown[_drillSet[0]].Count >= Yrow)
                            {
                                YpageEndDrill = 1 + YrowDrill + (YrowDrill * (YpageDrill - 1));
                                if (YpageEndDrill >= distinctList2DrillDown[_drillSet[0]].Count)
                                    YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                            }
                            if (YpageDrill == YtotalPageDrill + 1)
                                YpageEndDrill = distinctList2DrillDown[_drillSet[0]].Count;
                        }

                        ExportHTML currentExport1 = new ExportHTML();
                        currentExport1.drillDown2HtmlYMtableCrosstab(ramDetailgz, _drillSet, YpageDrill, YtotalPageDrill, YpageStartDrill, YpageEndDrill, requestID, requestDict, responseDict, outputFolder, distinctList2DrillDown, ramKey2Valuegz);
                    }

                    if (requestDict[2].openReport == "Y")
                    {
                        startEventTime = DateTime.Now;
                        requestDict[2].openReport = "";
                        ExportCSV currentExport1 = new ExportCSV();

                        int fileNo = 1;
                        bool fileExist = false;

                        do
                        {
                            fileExist = false;
                            string[] sourceFile2 = Directory.GetFiles(outputFolder.ToString(), "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                            foreach (string filePath in sourceFile2)
                            {

                                if (filePath == outputFolder.ToString() + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile)
                                {
                                    fileNo++;
                                    fileExist = true;
                                }
                                else
                                    fileExist = false;
                            }

                        } while (fileExist == true);

                        currentExport1.drillDown2CSVymTable(ramDetailgz, distinctSet, distinctList2DrillDown, ramKey2Valuegz, csvWriteSeparator, outputFolder, "\\" + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                        if (!Directory.Exists(outputFolder))
                            Directory.CreateDirectory(outputFolder);

                        FileInfo fi2 = new FileInfo(@outputFolder + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile);
                        if (fi2.Exists)
                        {
                            try
                            {
                                using (Process exeProcess = Process.Start(@outputFolder + "distinct2DrillDown" + fileNo.ToString() + "_" + requestDict[requestID].importFile))
                                {
                                    exeProcess.WaitForExit();
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"please close file '{e}'");
                            }
                        }
                        else
                        {
                            //file doesn't exist
                        }
                    }
                }


                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "pageHome" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    if (Xpage > 1) lastXPage = Xpage;
                    Xpage = 1;

                    if (Ypage > 1) lastYPage = Ypage;
                    Ypage = 1;

                    pageHomeToggle++;

                    if (pageHomeToggle >= 2 && (lastXPage > 0 || lastYPage > 0))
                    {
                        if (lastXPage > 0)
                            Xpage = lastXPage;
                        else
                            Xpage = 1;

                        if (lastYPage > 0)
                            Ypage = lastYPage;
                        else
                            Ypage = 1;

                        pageHomeToggle = 0;

                        if (Ypage == 1)
                        {
                            YpageStart = 1 + (Yrow * (Ypage - 1));
                            if ((Ypage - 1) < YtotalPage)
                            {
                                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                                if (YpageEnd >= YdistinctList[0].Count)
                                    YpageEnd = YdistinctList[0].Count;
                            }
                            else
                                YpageEnd = YdistinctList[0].Count;
                        }
                    }

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageEnd " + XpageEnd);

                    XpageStart = 0 + (Xcolumn * (Xpage - 1));
                    if ((Xpage - 1) < XtotalPage)
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 1));
                    else
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 2)) + ((XdistinctList[0].Count - 1) % Xcolumn);

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                    
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "pageEndRight" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    if (Xpage < Convert.ToInt32(XtotalPage) + 1) lastXPage = Xpage;
                    Xpage = Convert.ToInt32(XtotalPage) + 1;
                    pageEndRightToggle++;
                    if (pageEndRightToggle >= 2)
                    {
                        Xpage = lastXPage;
                        pageEndRightToggle = 0;
                    }

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageEnd " + XpageEnd);

                    XpageStart = 0 + (Xcolumn * (Xpage - 1));
                    if ((Xpage - 1) < XtotalPage)
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 1));
                    else
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 2)) + ((XdistinctList[0].Count - 1) % Xcolumn);

                    if (XpageStart < 0) XpageStart = 1;
                    if (XpageEnd < 0) XpageEnd = 1;

                   // Console.WriteLine("pageEndRight Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageStart " + XpageStart + " XpageEnd " + XpageEnd);

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "pageEndDown" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    if (Ypage < Convert.ToInt32(YtotalPage)) lastYPage = Ypage;
                    Ypage = Convert.ToInt32(YtotalPage);
                    pageEndDownToggle++;
                    if (pageEndDownToggle >= 2)
                    {
                        Ypage = lastYPage;
                        pageEndDownToggle = 0;

                        if (Ypage == 1)
                        {
                            YpageStart = 1 + (Yrow * (Ypage - 1));
                            if ((Ypage - 1) < YtotalPage)
                            {
                                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                                if (YpageEnd >= YdistinctList[0].Count)
                                    YpageEnd = YdistinctList[0].Count;
                            }
                            else
                                YpageEnd = YdistinctList[0].Count;
                        }
                    }

                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && YdistinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = YdistinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Left" && Xpage > 1 && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Xpage = Xpage - 1;

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageEnd " + XpageEnd);

                    XpageStart = 0 + (Xcolumn * (Xpage - 1));
                    if ((Xpage - 1) < XtotalPage)
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 1));
                    else
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 2)) + ((XdistinctList[0].Count - 1) % Xcolumn);

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Right" && (Xpage - 1) < XtotalPage && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Xpage++;

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageEnd " + XpageEnd);

                    XpageStart = 0 + (Xcolumn * (Xpage - 1));
                    if ((Xpage - 1) < XtotalPage)
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 1));
                    else
                        XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 2)) + ((XdistinctList[0].Count - 1) % Xcolumn);

                   // Console.WriteLine("pageRight Xpage " + Xpage + " XtotalPage " + XtotalPage + " XpageStart " + XpageStart + " XpageEnd " + XpageEnd);

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);                 
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Up" && Ypage > 1 && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Ypage = Ypage - 1;

                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Ypage " + Ypage + " YtotalPage " + YtotalPage + " YpageEnd " + YpageEnd);

                    if (Ypage == 1)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        else
                            YpageEnd = YdistinctList[0].Count;
                    }
                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && YdistinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = YdistinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);                 
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].direction == "Down" && Ypage < YtotalPage && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].direction = "";
                    Ypage++;
                    if (requestDict[requestID].debugOutput == "Y")
                        Console.WriteLine("Ypage " + Ypage + " YtotalPage " + YtotalPage + " YpageEnd " + YpageEnd);

                    if (Ypage == 1)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        else
                            YpageEnd = YdistinctList[0].Count;
                    }
                    if (Ypage >= 2)
                    {
                        YpageStart = 1 + (Yrow * (Ypage - 1));
                        if ((Ypage - 1) < YtotalPage)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if ((Ypage - 1) <= YtotalPage && YdistinctList[0].Count >= Yrow)
                        {
                            YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
                            if (YpageEnd >= YdistinctList[0].Count)
                                YpageEnd = YdistinctList[0].Count;
                        }
                        if (Ypage == YtotalPage + 1)
                            YpageEnd = YdistinctList[0].Count;
                    }

                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].precisionLevel == "Next" && requestDict[2].drillType == "null")
                {
                    requestDict[2].precisionLevel = "";
                    requestDict[requestID].precisionLevel = nextPrecision[nextLevel];
                    nextLevel++;
                    if (nextLevel == 4) nextLevel = 0;
                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].drillDownEventType == "Next" && requestDict[2].drillType == "null")
                {
                    requestDict[2].drillDownEventType = "";
                    userPreference["system"].nextDrillDownEventType++;
                    if (userPreference["system"].nextDrillDownEventType == 2)
                        userPreference["system"].nextDrillDownEventType = 0;

                    if (userPreference["system"].nextDrillDownEventType == 0)
                        userPreference["system"].drillDownEventType = "onmouseenter";


                    if (userPreference["system"].nextDrillDownEventType == 1)
                        userPreference["system"].drillDownEventType = "onclick";
                    
                    ExportHTML currentExport1 = new ExportHTML();
                    currentExport1.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, outputFolder, dataSortingOrder);
                }



                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].openReport == "Y" && requestDict[2].drillType == "null")
                {
                    startEventTime = DateTime.Now;
                    requestDict[2].openReport = "";
                    ExportCSV currentExport1 = new ExportCSV();

                    if (!Directory.Exists(outputFolder))
                        Directory.CreateDirectory(outputFolder);                  

                   
                    int fileNo = 1;
                    bool fileExist = false;

                    do
                    {
                        fileExist = false;
                        string[] sourceFile = Directory.GetFiles(outputFolder.ToString(), "crosstab" + fileNo.ToString() + "_" + requestDict[requestID].importFile);

                        foreach (string filePath in sourceFile)
                        {

                            if (filePath == outputFolder.ToString() + "crosstab" + fileNo.ToString() + "_" + requestDict[requestID].importFile)
                            {
                                fileNo++;
                                fileExist = true;
                            }
                            else
                                fileExist = false;
                        }

                    } while (fileExist == true);

                    currentExport1.ramDistinct2CSVcrosstabTable(XdistinctList, requestID, requestDict, YXMdistinctList, distinctXramKey2Value, distinctYramKey2Value, csvWriteSeparator, outputFolder, "crosstab" + fileNo.ToString() + "_" + requestDict[requestID].importFile);
                    FileInfo fi = new FileInfo(@outputFolder + "crosstab" + fileNo.ToString() + "_" + requestDict[requestID].importFile);
                    if (fi.Exists)
                    {
                        try
                        {
                            using (Process exeProcess = Process.Start(@outputFolder + "crosstab" + fileNo.ToString() + "_" + requestDict[requestID].importFile))
                            {
                                exeProcess.WaitForExit();
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine($"please close file '{e}'");
                        }
                    }
                    else
                    {
                        //file doesn't exist
                    }
                    
                }                  

                endEventTime = DateTime.Now;

                timeout = (Convert.ToDouble(endEventTime.ToOADate()) - Convert.ToDouble(startEventTime.ToOADate())) * 100000;
                // Console.WriteLine(timeout);

                Thread.Sleep(10);
            }   while (timeout < 10000 && presentationThread.Count == presentationJob);
    
        }
    }
}
