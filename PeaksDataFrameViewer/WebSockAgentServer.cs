using Fleck;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace youFast
{
    public class WebSockAgentServer
    {
        void requestQueue2Thread(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<int, decimal> currentRequestID, string sourceFolder, byte csvReadSeparator, string db1Folder, int iteration, string outputFolder, char csvWriteSeparator, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, string> forwardMessage, Dictionary<int, StringBuilder> htmlTable, Dictionary<string, Dictionary<int, List<double>>> ramDetail, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, ConcurrentQueue<decimal> incomingRequestQueue, ConcurrentDictionary<decimal, Thread> request2Response, ConcurrentDictionary<decimal, int> cancelRequestID, bool isRemove, DateTime currentDateTime, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl)
        {
            Request2Report processRequest = new Request2Report();

            while (true)
            {
                if (incomingRequestQueue.Count > 0)
                {
                    decimal requestID;                   
                    if (!incomingRequestQueue.TryPeek(out requestID)) 
                        Console.WriteLine("TryPeek failed when it should have succeeded");

                    else if (requestID != 0)
                    {
                        if (!incomingRequestQueue.TryDequeue(out requestID))
                            Console.WriteLine("TryDeqeue failed when it should have succeeded");

                        else if (requestID != 0)
                        {                         
                            try
                            {  
                                if (request2Response.TryAdd(requestID, new Thread(() => processRequest.distinctDBreporting(userPreference, tableFact, currentRequestID, csvWriteSeparator, responseDict, htmlTable, ramDetail, remK2V, sourceFolder, outputFolder, csvReadSeparator, db1Folder, requestID, requestDict, screenControl))) == true)
                                    request2Response[requestID].Start();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"request2Response fail '{e}'");
                            }
                        }                                 
                    }
                  //  Console.WriteLine(" isRequestID " + requestID + " Queue.Count " + incomingRequestQueue.Count +  " cancelRequestID.count " + cancelRequestID.Count + " completeJob.count " + request2Response.Count);
                }
                Thread.Sleep(2);
            }
        }    
       
        public void webSock(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, int iteration, string outputFolder, byte csvReadSeparator, char csvWriteSeparator, Dictionary<int, string> forwardMessage, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string sourceFolder, string db1Folder, string dbBackupFolder)
        {          
            Dictionary<string, Dictionary<int, List<double>>> ramDetail = new Dictionary<string, Dictionary<int, List<double>>>();
            Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V = new Dictionary<string, Dictionary<int, Dictionary<double, string>>>();
            ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact = new ConcurrentDictionary<string, ConcurrentDictionary<int, int>>();            

            StringBuilder htmlFilter = new StringBuilder();            
            Dictionary<int, StringBuilder> htmlTable = new Dictionary<int, StringBuilder>();
            ConcurrentDictionary<decimal, int> cancelRequestID = new ConcurrentDictionary<decimal, int>(); // list of completed request  
            ConcurrentDictionary<decimal, Thread> queueThread = new ConcurrentDictionary<decimal, Thread>(); // a thread manage queue job
            ConcurrentQueue<decimal> incomingRequestQueue = new ConcurrentQueue<decimal>(); // current o/s queue job
            ConcurrentDictionary<decimal, string> incomingRequest = new ConcurrentDictionary<decimal, string>(); // keep system generate requestID and message
            ConcurrentDictionary<decimal, Thread> request2Response = new ConcurrentDictionary<decimal, Thread>(); // each thread manage each report to pagination
            Dictionary<int, Dictionary<string, string>> processAcceptList = new Dictionary<int, Dictionary<string, string>>();
            Dictionary<int, bool> resetDimensionOrder = new Dictionary<int, bool>();
            Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl = new Dictionary<decimal, Dictionary<string, StringBuilder>>();
            ConcurrentDictionary<int, decimal> currentRequestID = new ConcurrentDictionary<int, decimal>();

            resetDimensionOrder[0] = false;
            processAcceptList.Add(1, new Dictionary<string, string>());
            processAcceptList.Add(2, new Dictionary<string, string>());        
            processAcceptList[2].Add("dataSorting", "interimEvent");
            processAcceptList[2].Add("moveToCrosstab", "interimEvent");
            processAcceptList[2].Add("removeFromCrosstab", "interimEvent");
            processAcceptList[2].Add("rotateDimension", "interimEvent");
            processAcceptList[2].Add("rotateDimensionCrosstab", "interimEvent");
            processAcceptList[2].Add("rotateDimensionDrillDown", "interimEvent");
            processAcceptList[2].Add("displayPrecision", "finalEvent");
            processAcceptList[2].Add("nextPrecision", "finalEvent");
            processAcceptList[2].Add("changeDrillDownEvent", "finalEvent");            
            processAcceptList[2].Add("drillDown", "finalEvent");
            processAcceptList[2].Add("pageMove", "finalEvent");
            processAcceptList[2].Add("downloadReport", "finalEvent");
            processAcceptList[2].Add("openReportByWindows", "finalEvent");

            int serialID = 0; // to force uniqueness of html ID for adding filter control          
            decimal followUpRequestID = 0;                        
            bool isRemove = false;  
            DateTime currentDateTime = DateTime.Now;
            Random random = new Random();

            var clients = new List<IWebSocketConnection>();
           // var server = new WebSocketServer("ws://192.168.1.195:5000");
            var server = new WebSocketServer("ws://127.0.0.1:5000");

            try // new a thread to manage queue job
            {
                queueThread.TryAdd(1, new Thread(() => requestQueue2Thread(userPreference, tableFact, currentRequestID, sourceFolder, csvReadSeparator, db1Folder, iteration, outputFolder, csvWriteSeparator, requestDict, responseDict, forwardMessage, htmlTable, ramDetail, remK2V, incomingRequestQueue, request2Response, cancelRequestID, isRemove, currentDateTime, screenControl)));
                queueThread[1].Start();
            }         

            catch (Exception e)
            {
                Console.WriteLine($"queueThread fail '{e}'");
            }

            decimal userRequestCount = 0;

            server.Start(socket =>
            {
            socket.OnOpen = () =>
            {
                clients.Add(socket);
            };

            socket.OnClose = () =>
            {
                clients.Remove(socket);
            };

            socket.OnMessage = message =>
            {

                forwardMessage[0] = message;
               /* 
                if(message.Length > 20)
                    Console.WriteLine(message + " ");
                    */
                    

                if (message.Contains("{") == false) // message must not be JSON
                {
                    if (decimal.TryParse(message.Trim(), out followUpRequestID) == true)
                    {
                        if (responseDict.ContainsKey(followUpRequestID))
                        {
                            try
                            {
                                if(responseDict[followUpRequestID].html != null)
                                { 
                                    if (responseDict[followUpRequestID].html.Length > 0)
                                    {
                                        socket.Send(responseDict[followUpRequestID].html.ToString());
                                        responseDict[followUpRequestID].html.Clear();                                                       

                                        if (screenControl[followUpRequestID]["displayFilterDropDownList"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["displayFilterDropDownList"].ToString());

                                        if (screenControl[followUpRequestID]["displaySelectedFilterValue"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["displaySelectedFilterValue"].ToString());

                                        if (screenControl[followUpRequestID]["displayCrosstalDimension"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["displayCrosstalDimension"].ToString());

                                        if (screenControl[followUpRequestID]["measurement"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["measurement"].ToString());                               

                                        if (screenControl[followUpRequestID]["displaySelectedColumn"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["displaySelectedColumn"].ToString());

                                        if (screenControl[followUpRequestID]["ramdistint2SelectFile"].ToString().Length > 0)
                                            socket.Send(screenControl[followUpRequestID]["ramdistint2SelectFile"].ToString());                                
                              
                                        screenControl[followUpRequestID].Clear();
                                    }
                                    else
                                    {
                                        if (responseDict[followUpRequestID].updateMeasure == true)
                                            if (screenControl[followUpRequestID]["measurement"].ToString().Length > 0)
                                                socket.Send(screenControl[followUpRequestID]["measurement"].ToString());     

                                        if (responseDict[followUpRequestID].updateCrosstab == true)
                                            if (screenControl[followUpRequestID]["displayCrosstalDimension"].ToString().Length > 0)                               
                                                socket.Send(screenControl[followUpRequestID]["displayCrosstalDimension"].ToString());

                                        if (responseDict[followUpRequestID].updateDistinct == true)
                                            if (screenControl[followUpRequestID]["displaySelectedColumn"].ToString().Length > 0)
                                                socket.Send(screenControl[followUpRequestID]["displaySelectedColumn"].ToString());
                                    }
                                }
                            }
                            catch
                            {
                                 //   Console.WriteLine("followUpRequestID fail");
                            }                        
                        }
                    }                           
                }
                else // message may be JSON
                {       
                    decimal requestID2Queue = 0;
                    DateTime dt = DateTime.Now;
                    userRequestCount++;
                    requestID2Queue = (userRequestCount * 1000 + dt.Millisecond) / 1000;

                    if (!responseDict.ContainsKey(requestID2Queue))
                        responseDict.TryAdd(requestID2Queue, new clientMachine.response());

                    responseDict[requestID2Queue].startTime = DateTime.Now;                    
                    
                    //Console.WriteLine("Start milliseconds: {0:MM/dd/yyy HH:mm:ss.fff}", DateTime.Now);
                    Dictionary<string, string> variable = new Dictionary<string, string>();
                    Dictionary<string, List<string>> array = new Dictionary<string, List<string>>();
                    Json convert = new Json();
                    convert.Json2VariableArray(message, variable, array);
                    convert.Json2VariableList(message, requestID2Queue, variable, array, requestDict);

                    if (processAcceptList[2].ContainsKey(requestDict[requestID2Queue].processID) == true && requestDict[requestID2Queue].processButton != "drillRow")
                    {
                        socket.Send("nextPageID " + requestDict[requestID2Queue].nextPageID.ToString());
                        convert.Json2VariableList(message, 2, variable, array, requestDict);
                    }   

                    else if (processAcceptList[2].ContainsKey(requestDict[requestID2Queue].processID) == true && requestDict[requestID2Queue].processButton == "drillRow")
                    { 
                        if(requestDict[requestID2Queue].processID != "rotateDimension" || requestDict[requestID2Queue].processID != "rotateDimensionCrosstab")
                        { 
                            socket.Send("nextPageID " + requestDict[requestID2Queue].nextPageID.ToString());                  
                            convert.Json2VariableList(message, 2, variable, array, requestDict);
                        }

                        if (requestDict[requestID2Queue].processID == "rotateDimension")
                        {   
                            decimal Q = requestDict[requestID2Queue].nextPageID + 100;                        
                            requestDict[Q] = requestDict[requestDict[requestID2Queue].nextPageID];
                            requestDict[Q].processID = "runReport";

                            List<string> rotateDimension = new List<string>();
                            for (int i = 0; i < requestDict[Q].distinctDimension.Count; i++)
                            {
                                if (requestDict[Q].distinctDimension[i].Trim().ToString() == requestDict[requestID2Queue].rotateDimensionFrom.Trim().ToString())
                                    rotateDimension.Add(requestDict[requestID2Queue].rotateDimensionTo.ToString());

                                else if (requestDict[Q].distinctDimension[i].Trim().ToString() == requestDict[requestID2Queue].rotateDimensionTo.Trim().ToString())
                                    rotateDimension.Add(requestDict[requestID2Queue].rotateDimensionFrom.ToString());

                                else
                                    rotateDimension.Add(requestDict[Q].distinctDimension[i].ToString());
                            }
                        
                            requestDict[Q].distinctDimension.Clear();

                            if (!responseDict.ContainsKey(Q))
                            { 
                                responseDict.TryAdd(Q, new clientMachine.response());
                                responseDict[Q].startTime = DateTime.Now;
                            } 

                            for (int i = 0; i < rotateDimension.Count; i++)
                                requestDict[Q].distinctDimension.Add(rotateDimension[i].Trim().ToString());                            

                            requestDict[Q].randomFilter = "N";                      
                            socket.Send("<div class=\"w3-container\"><button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> RequestID = " + Q.ToString() + " is Processing </button></div>");

                            if (incomingRequest.TryAdd(Q, message) == true)                       
                                    incomingRequestQueue.Enqueue(Q);
                       
                            requestDict[Q].processID = "";   
                        }

                        if (requestDict[requestID2Queue].processID == "rotateDimensionCrosstab")
                        { 
                            decimal Q = requestDict[requestID2Queue].nextPageID + 100;
                            requestDict[Q] = requestDict[requestDict[requestID2Queue].nextPageID];
                            requestDict[Q].processID = "runReport";

                            List<string> rotateDimension = new List<string>();
                            for (int i = 0; i < requestDict[Q].distinctDimension.Count; i++)
                            {
                                if (requestDict[Q].distinctDimension[i].Trim().ToString() == requestDict[requestID2Queue].rotateDimensionFrom.Trim().ToString())
                                    rotateDimension.Add(requestDict[requestID2Queue].rotateDimensionTo.ToString());

                                else if (requestDict[Q].distinctDimension[i].Trim().ToString() == requestDict[requestID2Queue].rotateDimensionTo.Trim().ToString())
                                    rotateDimension.Add(requestDict[requestID2Queue].rotateDimensionFrom.ToString());

                                else
                                    rotateDimension.Add(requestDict[Q].distinctDimension[i].ToString());
                            }

                            requestDict[Q].distinctDimension.Clear();

                            if (!responseDict.ContainsKey(Q))
                            {
                                responseDict.TryAdd(Q, new clientMachine.response());
                                responseDict[Q].startTime = DateTime.Now;
                            }

                            for (int i = 0; i < rotateDimension.Count; i++)
                                requestDict[Q].distinctDimension.Add(rotateDimension[i].Trim().ToString());

                            requestDict[Q].randomFilter = "N";
                            socket.Send("<div class=\"w3-container\"><button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> RequestID = " + Q.ToString() + " is Processing </button></div>");

                            if (incomingRequest.TryAdd(Q, message) == true)
                                incomingRequestQueue.Enqueue(Q);

                            requestDict[Q].processID = "";
                        }

                        requestDict[requestID2Queue].processID = "";
                    }

                    else if (requestDict[requestID2Queue].processID == "css") // next process is "function displayFilterDropDownList()", where processID = "selectFile" 
                    {
                        CSS currentExport = new CSS();
                        var htmlmessage = currentExport.css().ToString();
                        if (htmlmessage.Contains("{") == true)
                            socket.Send(htmlmessage);
                    }

                    else if (requestDict[requestID2Queue].processID == "template") 
                    {
                        CSS currentExport = new CSS();
                        var htmlmessage = currentExport.template().ToString();
                        if (htmlmessage.Contains("<") == true)
                            socket.Send(htmlmessage);
                    }

                    else if (requestDict[requestID2Queue].processID == "selectFile") // display list of file for next process "importSelectedFile" to collect select file event
                    {
                        ExportHTML currentExport = new ExportHTML();
                        var htmlmessage = currentExport.ramdistint2SelectFile(remK2V, outputFolder, requestID2Queue, requestDict, sourceFolder, db1Folder).ToString(); //get html filter by StringBuider                           

                        if (htmlmessage.Contains("<") == true)
                            socket.Send(htmlmessage);
                    }

                    else if (requestDict[requestID2Queue].processID == "refreshSelectFileList") // display list of file for next process "importSelectedFile" to collect select file event
                    {
                        ExportHTML currentExport = new ExportHTML();
                        var htmlmessage = currentExport.ramdistint2SelectFile(remK2V, outputFolder, requestID2Queue, requestDict, sourceFolder, db1Folder).ToString(); //get html filter by StringBuider                           

                        if (htmlmessage.Contains("<") == true)
                            socket.Send(htmlmessage);
                    }

                    else if (requestDict[requestID2Queue].processID == "importSelectedFile")
                    {      
                        currentRequestID[0] = requestID2Queue;
                        requestDict[requestID2Queue].processID = "";
                        socket.Send("<div class=\"w3-container\"><button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> RequestID = " + requestID2Queue.ToString() + " is Processing </button></div>");
                        if (incomingRequest.TryAdd(requestID2Queue, message) == true)
                            incomingRequestQueue.Enqueue(requestID2Queue);
                    }
/*
                    else if (requestDict[requestID2Queue].processID == "dropFile")
                    {      
                        currentRequestID[0] = requestID2Queue;
                        requestDict[requestID2Queue].processID = "";
                        socket.Send("<div class=\"w3-container\"><button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> RequestID = " + requestID2Queue.ToString() + " is Processing </button></div>");
                        if (incomingRequest.TryAdd(requestID2Queue, message) == true)
                            incomingRequestQueue.Enqueue(requestID2Queue);
                    }
*/
                    else if (requestDict[requestID2Queue].processID == "addFilter")
                    {
                        serialID++;
                        ExportHTML currentExport = new ExportHTML();                    
                        var htmlmessage = currentExport.ramdistinct2AddFilter(outputFolder, requestID2Queue, requestDict, serialID, remK2V[requestDict[requestID2Queue].importFile]).ToString(); //get html filter by StringBuider                           
                        if (htmlmessage.Contains("<") == true)
                            socket.Send(htmlmessage);
                    }

                    else if (requestDict[requestID2Queue].processID == "addDisplayColumn")
                    {
                        if (requestDict[requestID2Queue].addColumnType == "AddAllDistinctDimension")
                        {
                            serialID++;
                            ExportHTML currentExport = new ExportHTML();
                            var htmlmessage = currentExport.ramdistint2DisplayAllColumn(outputFolder, requestID2Queue, requestDict, remK2V[requestDict[requestID2Queue].importFile]).ToString(); // get Crosstab Dimension for selection
                            if (htmlmessage.Contains("<") == true)
                                socket.Send(htmlmessage);
                        }
                        if (requestDict[requestID2Queue].addColumnType == "AddCrosstabDimension")
                        {
                            serialID++;
                            ExportHTML currentExport = new ExportHTML();
                            var htmlmessage = currentExport.ramdistint2Crosstab(tableFact, outputFolder, requestID2Queue, requestDict, remK2V[requestDict[requestID2Queue].importFile]).ToString(); // get Crosstab Dimension for selection
                            if (htmlmessage.Contains("<") == true)
                                socket.Send(htmlmessage);
                        }

                        if (requestDict[requestID2Queue].addColumnType == "AddMeasurement")
                        {
                            ExportHTML currentExport = new ExportHTML();
                            var htmlmessage = currentExport.ramdistint2Measurement(outputFolder, requestID2Queue, requestDict, remK2V[requestDict[requestID2Queue].importFile]).ToString(); // get Measurement Column for selection
                            if (htmlmessage.Contains("<") == true)
                                socket.Send(htmlmessage);
                        }
                        if (requestDict[requestID2Queue].addColumnType == "AddAllMeasurement")
                        {
                            ExportHTML currentExport = new ExportHTML();
                            var htmlmessage = currentExport.ramdistint2AllMeasurement(outputFolder, requestID2Queue, requestDict, remK2V[requestDict[requestID2Queue].importFile]).ToString(); // get Measurement Column for selection
                            if (htmlmessage.Contains("<") == true)
                                socket.Send(htmlmessage);
                        }
                    }

                    else if (requestDict[requestID2Queue].processID == "runReport")
                    {                      
                        requestDict[requestID2Queue].processID = "";
                        currentRequestID[0] = requestID2Queue;
                   
                        socket.Send("<div class=\"w3-container\"><button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> RequestID = " + requestID2Queue.ToString() + " is Processing </button></div>");
                            if (incomingRequest.TryAdd(requestID2Queue, message) == true)
                                incomingRequestQueue.Enqueue(requestID2Queue);
                    }
                }
            };

            socket.OnBinary = message =>
            {

            };
        });

            Javascript homepage = new Javascript();

            try // new a thread to manage queue job
            {
                queueThread.TryAdd(2, new Thread(() => homepage.distinctDesktopHtml()));
                queueThread[2].Start();
            }
            catch (Exception e)
            {
                Console.WriteLine($"queueThread fail '{e}'");
            }

          //  WinSockClient client = new WinSockClient();           

          //  client.winSock(iteration, outputFolder, forwardMessage, csvWriteSeparator, columnName2ID, htmlTable, requestDict, responseDict, ramDetailgz, ramKey2Valuegz, ramValue2Keygz, ramKey2Order, ramOrder2Key);
        }        
    }
}
