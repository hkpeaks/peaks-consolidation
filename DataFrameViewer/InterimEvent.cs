using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace youFast
{
    public class InterimEvent
    {      
        public void postDistinctEvent(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, Dictionary<decimal, int> distinctDimensionChecksumList, ConcurrentDictionary<int, decimal> currentRequestID, List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, List<int> distinctDimension, Dictionary<int, string> xyDimension, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, int presentationJob, Dictionary<int, string> dataSortingOrder, ConcurrentDictionary<decimal, Thread> presentationThread, List<int> crosstabDimension, List<int> yDimension, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<int> revisedX, List<int> revisedY, StringBuilder debug, string outputFolder, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, int> stopPresentationThread, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key,  Dictionary<int, List<double>> distinctList, Dictionary<int, StringBuilder> htmlTable, List<int> measure, char csvWriteSeparator, int xAddressCol, int yAddressCol, Dictionary<string, string> columnName2ID, Dictionary<int, string> columnMoveOrder, string startRotateDimension, string startMoveDimension, Dictionary<decimal, List<int>> distinctList2DrillDown)
        {
            int moveColumnID = 0;
            List<decimal> endInterimEvent = new List<decimal>();
            var startEventTime = DateTime.Now;
            var endEventTime = DateTime.Now;
            double timeout = 0;         
              
            do
            {   
                startRotateDimension = "N";
                startMoveDimension = "N";

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].rotateDimension == "Y")
                {
                    responseDict[requestID].startTime = DateTime.Now;
                    requestDict[2].rotateDimension = "N";
                    startRotateDimension = "Y";
                    endInterimEvent.Add(requestDict[2].nextPageID);
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
                }
                

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].moveColumnDirection == "Y2X")
                {

                    responseDict[requestID].startTime = DateTime.Now;
                    endInterimEvent.Add(requestDict[2].nextPageID);
                    presentationJob++;
                    bool isSuccess = Int32.TryParse(columnName2ID[requestDict[2].moveColumnName.Replace("#", " ")], out moveColumnID);
                    columnMoveOrder.Clear();
                    columnMoveOrder[moveColumnID] = "Y2X";
                    requestDict[2].moveColumnDirection = " ";
                    startMoveDimension = "Y";

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
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].moveColumnDirection == "X2Y")
                {
                    responseDict[requestID].startTime = DateTime.Now;
                    endInterimEvent.Add(requestDict[2].nextPageID);
                    presentationJob++;
                    bool isSuccess = Int32.TryParse(columnName2ID[requestDict[2].moveColumnName.Replace("#", " ")], out moveColumnID);
                    columnMoveOrder.Clear();
                    columnMoveOrder[moveColumnID] = "X2Y";
                    requestDict[2].moveColumnDirection = " ";
                    startMoveDimension = "Y";

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
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].sortingOrder == "sortAscending")
                {
                    responseDict[requestID].startTime = DateTime.Now;
                    endInterimEvent.Add(requestDict[2].nextPageID);
                    presentationJob++;
                    columnMoveOrder.Clear();
                    dataSortingOrder[0] = "sortAscending";
                    requestDict[requestID].sortXdimension = "A";
                    requestDict[requestID].sortYdimension = "A";
                    requestDict[2].sortXdimension = "A";
                    requestDict[2].sortYdimension = "A";
                    requestDict[2].sortingOrder = "";

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
                }

                if (requestDict.ContainsKey(2) && (requestDict[2].nextPageID == responseDict[requestID].requestID) && requestDict[2].sortingOrder == "sortDescending")
                {
                    responseDict[requestID].startTime = DateTime.Now;
                    endInterimEvent.Add(requestDict[2].nextPageID);
                    presentationJob++;
                    columnMoveOrder.Clear();
                    dataSortingOrder[0] = "sortDescending";
                    requestDict[requestID].sortXdimension = "D";
                    requestDict[requestID].sortYdimension = "D";
                    requestDict[2].sortXdimension = "D";
                    requestDict[2].sortYdimension = "D";
                    requestDict[2].sortingOrder = "";

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
                }

                Thread.Sleep(5);
                endEventTime = DateTime.Now;
                timeout = (Convert.ToDouble(endEventTime.ToOADate()) - Convert.ToDouble(startEventTime.ToOADate())) * 100000;
           
            } while(currentRequestID[0] == responseDict[requestID].requestID && (timeout < 10000 || endInterimEvent.Count <= 1 || (endInterimEvent.Count >= 2 && endInterimEvent[endInterimEvent.Count - 1] == endInterimEvent[endInterimEvent.Count - 2])));
        }
    }    

    public class Presentation
    {
        public void presentationOfData(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, Dictionary<decimal, int> distinctDimensionChecksumList, List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, List<int> distinctDimension, Dictionary<int, string> xyDimension, List<int> crosstabDimension, List<int> yDimension, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<int> revisedX, List<int> revisedY, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, StringBuilder debug, string outputFolder, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key,  Dictionary<int, List<double>> distinctList, Dictionary<int, StringBuilder> htmlTable, List<int> measure, char csvWriteSeparator, Dictionary<int, string> dataSortingOrder, int xAddressCol, int yAddressCol, int presentationJob, Dictionary<int, int> stopPresentationThread, ConcurrentDictionary<decimal, Thread> presentationThread, Dictionary<int, string> columnMoveOrder, int moveColumnID, string startRotateDimension, Dictionary<string, string> columnName2ID, string startMoveDimension, Dictionary<decimal, List<int>> distinctList2DrillDown)
        {
            Dictionary<int, List<double>> XdistinctList = new Dictionary<int, List<double>>(); // selected X dimensions of distinctList       
            Dictionary<int, List<double>> YdistinctList = new Dictionary<int, List<double>>(); // selected Y dimensions of distinctList                
            Dictionary<int, List<double>> YXMdistinctList = new Dictionary<int, List<double>>(); // crosstab distinctList     
            Dictionary<int, Dictionary<double, string>> distinctXramKey2Value = new Dictionary<int, Dictionary<double, string>>();
            Dictionary<int, Dictionary<double, string>> distinctYramKey2Value = new Dictionary<int, Dictionary<double, string>>();

            if ((crosstabDimension.Count == 0 && distinctDimension.Count == 1) || crosstabDimension.Count == 0 && !columnMoveOrder.ContainsKey(moveColumnID) || (crosstabDimension.Count == 0 && columnMoveOrder.ContainsKey(moveColumnID) && columnMoveOrder[moveColumnID] != "Y2X") || (columnMoveOrder.ContainsKey(moveColumnID) && crosstabDimension.Count == 1 && columnMoveOrder[moveColumnID] == "X2Y"))
            {
                List<int> sortedYdimension = new List<int>();
                ChangeInReportPresentation chanagePresentation = new ChangeInReportPresentation();                

                if (crosstabDimension.Count >= 1)
                {
                    yDimension.Clear();
                    xyDimension.Clear();
                    sortedYdimension.Clear();
                    revisedX.Clear(); revisedY.Clear();
                    crosstabDimension.Clear();
                    distinctRamKey2Value.Clear();
                    yDimension = distinctDimension;
                    distinctList.Remove(yAddressCol);
                    distinctList.Remove(xAddressCol);
                    for (int i = 0; i < distinctDimension.Count; i++)
                    {
                        distinctRamKey2Value[i] = ramKey2Valuegz[distinctDimension[i]];
                        revisedY.Add(i);
                    }
                    for (int i = distinctDimension.Count; i < (distinctDimension.Count + measure.Count); i++)
                        distinctRamKey2Value[i] = ramKey2Valuegz[measure[i - distinctDimension.Count]];
                }
               
                if (crosstabDimension.Count == 0)
                {
                    yDimension = distinctDimension;                  
                    chanagePresentation.sortSelectedYdimension(sortedYdimension, yDimension, distinctYramKey2Value, ramKey2Valuegz, revisedX, revisedY, requestID, requestDict, debug);
                }

                if (startRotateDimension == "Y")
                {   
                    var rotateDimensionFrom = requestDict[2].rotateDimensionFrom.Replace("#", " ");
                    var rotateDimensionTo = requestDict[2].rotateDimensionTo.Replace("#", " ");
                    List<int> rotateFrom = new List<int>();
                    List<int> rotateTo = new List<int>();                    
                    rotateFrom.Add(0); rotateTo.Add(0);                     

                    for (int i = 0; i < ramKey2Valuegz.Count; i++)
                    {
                        if (ramKey2Valuegz[i][0] == rotateDimensionFrom)
                            rotateFrom[0] = i;

                        if (ramKey2Valuegz[i][0] == rotateDimensionTo)
                            rotateTo[0] = i;
                    }
                    // ramKey2Valuegz
                    Dictionary<int, Dictionary<double, string>> tempKey2Value = new Dictionary<int, Dictionary<double, string>>();
                    tempKey2Value.Add(0, new Dictionary<double, string>());

                    foreach (var pair in ramKey2Valuegz[rotateTo[0]])
                        tempKey2Value[0].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                    ramKey2Valuegz[rotateTo[0]].Clear();

                    foreach (var pair in ramKey2Valuegz[rotateFrom[0]])
                        ramKey2Valuegz[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                    ramKey2Valuegz[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Value[0])
                        ramKey2Valuegz[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom                    

                    // ramKey2Order
                    Dictionary<int, Dictionary<double, double>> tempKey2Order = new Dictionary<int, Dictionary<double, double>>();
                    tempKey2Order.Add(0, new Dictionary<double, double>());

                    foreach (var pair in ramKey2Order[rotateTo[0]])
                        tempKey2Order[0].Add(pair.Key, pair.Value); // copy rotateTo to temp

                    ramKey2Order[rotateTo[0]].Clear();

                    foreach (var pair in ramKey2Order[rotateFrom[0]])
                        ramKey2Order[rotateTo[0]].Add(pair.Key, pair.Value); // copy rotateFrom to rotateTo

                    ramKey2Order[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Order[0])
                        ramKey2Order[rotateFrom[0]].Add(pair.Key, pair.Value); //copy temp to rotateFrom    

                    // ramOrder2Key
                    Dictionary<int, Dictionary<double, double>> tempOrder2Key = new Dictionary<int, Dictionary<double, double>>();
                    tempOrder2Key.Add(0, new Dictionary<double, double>());

                    foreach (var pair in ramOrder2Key[rotateTo[0]])
                        tempOrder2Key[0].Add(pair.Key, pair.Value); // copy rotateTo to temp

                    ramOrder2Key[rotateTo[0]].Clear();

                    foreach (var pair in ramOrder2Key[rotateFrom[0]])
                        ramOrder2Key[rotateTo[0]].Add(pair.Key, pair.Value); // copy rotateFrom to rotateTo

                    ramOrder2Key[rotateFrom[0]].Clear();

                    foreach (var pair in tempOrder2Key[0])
                        ramOrder2Key[rotateFrom[0]].Add(pair.Key, pair.Value); //copy temp to rotateFrom    

                    for (int i = 0; i < distinctRamKey2Value.Count; i++) // for distinctRamKey2Value & distinctList
                    {
                        if (distinctRamKey2Value[i][0] == rotateDimensionFrom)
                            rotateFrom[0] = i;

                        if (distinctRamKey2Value[i][0] == rotateDimensionTo)
                            rotateTo[0] = i;
                    }

                    // distinctRamKey2Value
                    tempKey2Value.Add(1, new Dictionary<double, string>());

                    foreach (var pair in distinctRamKey2Value[rotateTo[0]])
                        tempKey2Value[1].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                    distinctRamKey2Value[rotateTo[0]].Clear();

                    foreach (var pair in distinctRamKey2Value[rotateFrom[0]])
                        distinctRamKey2Value[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                    distinctRamKey2Value[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Value[1])
                        distinctRamKey2Value[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom   

                    // distinctList
                    Dictionary<int, List<double>> tempDistinctList = new Dictionary<int, List<double>>();                    
                    tempDistinctList.Add(0, new List<double>());

                    foreach (var value in distinctList[rotateTo[0]])
                        tempDistinctList[0].Add(value); // copy rotateTo to temp

                    distinctList[rotateTo[0]].Clear();

                    foreach (var value in distinctList[rotateFrom[0]])
                        distinctList[rotateTo[0]].Add(value); // copy rotateFrom to rotateTo

                    distinctList[rotateFrom[0]].Clear();

                    foreach (var value in tempDistinctList[0])
                        distinctList[rotateFrom[0]].Add(value); //copy temp to rotateFrom 

                    for (int i = 0; i < distinctYramKey2Value.Count; i++)  // for distinctYramKey2Value
                    {
                        if (distinctYramKey2Value[i][0] == rotateDimensionFrom)
                            rotateFrom[0] = i;

                        if (distinctYramKey2Value[i][0] == rotateDimensionTo)
                            rotateTo[0] = i;
                    }

                    // distinctYRamKey2Value
                    tempKey2Value.Add(2, new Dictionary<double, string>());

                    foreach (var pair in distinctYramKey2Value[rotateTo[0]])
                        tempKey2Value[2].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                    distinctYramKey2Value[rotateTo[0]].Clear();

                    foreach (var pair in distinctYramKey2Value[rotateFrom[0]])
                        distinctYramKey2Value[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                    distinctYramKey2Value[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Value[2])
                        distinctYramKey2Value[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom     
                }

                chanagePresentation.sortYdata(distinctListChecksum, distinctSet, unsorted2SortedCheksum, sortedYdimension, requestID, requestDict, outputFolder, responseDict, distinctRamKey2Value, distinctYramKey2Value, YdistinctList, ramKey2Order, ramOrder2Key, yDimension, distinctList, revisedY, dataSortingOrder);

                if (requestDict[requestID].debugOutput == "Y")
                {
                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "debugInfo.txt"))
                    {
                        toDisk.Write(debug);
                        toDisk.Close();
                        debug.Clear();
                    }
                } 

                chanagePresentation.startPaginationInYM(ramDetailgz, userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, distinctSet, unsorted2SortedCheksum, requestID, responseDict, distinctList, YdistinctList, requestDict, htmlTable, ramKey2Valuegz, distinctRamKey2Value, outputFolder, revisedY, measure, csvWriteSeparator, dataSortingOrder, presentationJob, presentationThread, dataSortingOrder, distinctList2DrillDown);
            }
            else if ((yDimension.Count == 0 && crosstabDimension.Count == 0 && columnMoveOrder.ContainsKey(moveColumnID) && columnMoveOrder[moveColumnID] == "Y2X") || (yDimension.Count >= 1 && !columnMoveOrder.ContainsKey(moveColumnID)) || (yDimension.Count == 1 && columnMoveOrder.ContainsKey(moveColumnID) && columnMoveOrder[moveColumnID] == "X2Y") || yDimension.Count >= 2 && crosstabDimension.Count >= 1 || yDimension.Count >= 2 && columnMoveOrder.ContainsKey(moveColumnID) && columnMoveOrder[moveColumnID] == "Y2X")
            {                
                List<int> sortedXdimension = new List<int>();
                List<int> sortedYdimension = new List<int>();
                List<int> sortedXYdimension = new List<int>(); // dimension for calc checkSum of combine X,Y dimension   
                Dictionary<int, string> sortedRevisedXY = new Dictionary<int, string>(); //combine X,Y dimension after sorting    
                ChangeInReportPresentation chanagePresentation = new ChangeInReportPresentation();

                if (startMoveDimension == "Y")
                    chanagePresentation.sortSelectedDimensionOrder(distinctDimension, crosstabDimension, requestID, requestDict, debug, yDimension, xyDimension, revisedX, revisedY, columnMoveOrder);

                chanagePresentation.sortSelectedXYdimension(sortedXdimension, sortedYdimension, sortedXYdimension, sortedRevisedXY, crosstabDimension, distinctXramKey2Value, ramKey2Valuegz, yDimension, distinctYramKey2Value, revisedX, revisedY, requestID, requestDict, debug);

                if (startRotateDimension == "Y")
                {
                    var rotateDimensionFrom = requestDict[2].rotateDimensionFrom.Replace("#", " ");
                    var rotateDimensionTo = requestDict[2].rotateDimensionTo.Replace("#", " ");
                    List<int> rotateFrom = new List<int>();
                    List<int> rotateTo = new List<int>();
                    List<int> rotateScenario = new List<int>();
                    rotateFrom.Add(0); rotateTo.Add(0); rotateFrom.Add(1); rotateTo.Add(1);
                    rotateScenario.Add(0); rotateScenario.Add(0); rotateScenario.Add(0); rotateScenario.Add(0); rotateScenario.Add(0);

                    for (int i = 0; i < ramKey2Valuegz.Count; i++) // for ramKey2Valuegz, ramKey2Order & ramOrder2Key
                    {
                        if (ramKey2Valuegz[i][0] == rotateDimensionFrom)
                            rotateFrom[0] = i;

                        if (ramKey2Valuegz[i][0] == rotateDimensionTo)
                            rotateTo[0] = i;
                    }

                    // ramKey2Valuegz
                    Dictionary<int, Dictionary<double, string>> tempKey2Value = new Dictionary<int, Dictionary<double, string>>();
                    tempKey2Value.Add(0, new Dictionary<double, string>());

                    foreach (var pair in ramKey2Valuegz[rotateTo[0]])
                        tempKey2Value[0].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                    ramKey2Valuegz[rotateTo[0]].Clear();

                    foreach (var pair in ramKey2Valuegz[rotateFrom[0]])
                        ramKey2Valuegz[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                    ramKey2Valuegz[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Value[0])
                        ramKey2Valuegz[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom                    

                    // ramKey2Order
                    Dictionary<int, Dictionary<double, double>> tempKey2Order = new Dictionary<int, Dictionary<double, double>>();
                    tempKey2Order.Add(0, new Dictionary<double, double>());

                    foreach (var pair in ramKey2Order[rotateTo[0]])
                        tempKey2Order[0].Add(pair.Key, pair.Value); // copy rotateTo to temp

                    ramKey2Order[rotateTo[0]].Clear();

                    foreach (var pair in ramKey2Order[rotateFrom[0]])
                        ramKey2Order[rotateTo[0]].Add(pair.Key, pair.Value); // copy rotateFrom to rotateTo

                    ramKey2Order[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Order[0])
                        ramKey2Order[rotateFrom[0]].Add(pair.Key, pair.Value); //copy temp to rotateFrom    

                    // ramOrder2Key
                    Dictionary<int, Dictionary<double, double>> tempOrder2Key = new Dictionary<int, Dictionary<double, double>>();
                    tempOrder2Key.Add(0, new Dictionary<double, double>());

                    foreach (var pair in ramOrder2Key[rotateTo[0]])
                        tempOrder2Key[0].Add(pair.Key, pair.Value); // copy rotateTo to temp

                    ramOrder2Key[rotateTo[0]].Clear();

                    foreach (var pair in ramOrder2Key[rotateFrom[0]])
                        ramOrder2Key[rotateTo[0]].Add(pair.Key, pair.Value); // copy rotateFrom to rotateTo

                    ramOrder2Key[rotateFrom[0]].Clear();

                    foreach (var pair in tempOrder2Key[0])
                        ramOrder2Key[rotateFrom[0]].Add(pair.Key, pair.Value); //copy temp to rotateFrom    

                    for (int i = 0; i < distinctRamKey2Value.Count; i++) // for distinctRamKey2Value & distinctList
                    {
                        if (distinctRamKey2Value[i][0] == rotateDimensionFrom)
                            rotateFrom[0] = i;

                        if (distinctRamKey2Value[i][0] == rotateDimensionTo)
                            rotateTo[0] = i;
                    }

                    // distinctRamKey2Value
                    tempKey2Value.Add(1, new Dictionary<double, string>());

                    foreach (var pair in distinctRamKey2Value[rotateTo[0]])
                        tempKey2Value[1].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                    distinctRamKey2Value[rotateTo[0]].Clear();

                    foreach (var pair in distinctRamKey2Value[rotateFrom[0]])
                        distinctRamKey2Value[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                    distinctRamKey2Value[rotateFrom[0]].Clear();

                    foreach (var pair in tempKey2Value[1])
                        distinctRamKey2Value[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom     

                    Dictionary<int, List<double>> tempDistinctList = new Dictionary<int, List<double>>();                    
                    tempDistinctList.Add(0, new List<double>());

                    foreach (var value in distinctList[rotateTo[0]])
                        tempDistinctList[0].Add(value); // copy rotateTo to temp

                    distinctList[rotateTo[0]].Clear();

                    foreach (var value in distinctList[rotateFrom[0]])
                        distinctList[rotateTo[0]].Add(value); // copy rotateFrom to rotateTo

                    distinctList[rotateFrom[0]].Clear();

                    foreach (var value in tempDistinctList[0])
                        distinctList[rotateFrom[0]].Add(value); //copy temp to rotateFrom 

                    for (int i = 0; i < distinctXramKey2Value.Count; i++) // distinctXramKey2Value
                    {
                        if (distinctXramKey2Value[i][0] == rotateDimensionFrom)
                        {
                            rotateFrom[0] = i;
                            rotateScenario[1] = 1;
                        }

                        if (distinctXramKey2Value[i][0] == rotateDimensionTo)
                        {
                            rotateTo[0] = i;
                            rotateScenario[2] = 2;
                        }
                    }

                    for (int i = 0; i < distinctYramKey2Value.Count; i++) // distinctYramKey2Value
                    {
                        if (distinctYramKey2Value[i][0] == rotateDimensionFrom)
                        {
                            rotateFrom[1] = i;
                            rotateScenario[3] = 10;
                        }

                        if (distinctYramKey2Value[i][0] == rotateDimensionTo)
                        {
                            rotateTo[1] = i;
                            rotateScenario[4] = 20;
                        }
                    }

                    rotateScenario[0] = rotateScenario[1] + rotateScenario[2] + rotateScenario[3] + rotateScenario[4];

                    if (rotateScenario[0] == 3)
                    {
                        // distinctXramKey2Value
                        tempKey2Value.Add(2, new Dictionary<double, string>());

                        foreach (var pair in distinctXramKey2Value[rotateTo[0]])
                            tempKey2Value[2].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                        distinctXramKey2Value[rotateTo[0]].Clear();

                        foreach (var pair in distinctXramKey2Value[rotateFrom[0]])
                            distinctXramKey2Value[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                        distinctXramKey2Value[rotateFrom[0]].Clear();

                        foreach (var pair in tempKey2Value[2])
                            distinctXramKey2Value[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom    
                    }

                    if (rotateScenario[0] == 30)
                    {
                        // distinctYramKey2Value
                        tempKey2Value.Add(3, new Dictionary<double, string>());

                        foreach (var pair in distinctYramKey2Value[rotateTo[1]])
                            tempKey2Value[3].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                        distinctYramKey2Value[rotateTo[1]].Clear();

                        foreach (var pair in distinctYramKey2Value[rotateFrom[1]])
                            distinctYramKey2Value[rotateTo[1]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                        distinctYramKey2Value[rotateFrom[1]].Clear();

                        foreach (var pair in tempKey2Value[3])
                            distinctYramKey2Value[rotateFrom[1]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom    
                    }

                    if (rotateScenario[0] == 21)
                    {
                        // X to Y
                        tempKey2Value.Add(4, new Dictionary<double, string>());

                        foreach (var pair in distinctYramKey2Value[rotateTo[1]])
                            tempKey2Value[4].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                        distinctYramKey2Value[rotateTo[1]].Clear();

                        foreach (var pair in distinctXramKey2Value[rotateFrom[0]])
                            distinctYramKey2Value[rotateTo[1]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                        distinctXramKey2Value[rotateFrom[0]].Clear();

                        foreach (var pair in tempKey2Value[4])
                            distinctXramKey2Value[rotateFrom[0]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom    
                    }

                    if (rotateScenario[0] == 12)
                    {
                        // Y to X
                        tempKey2Value.Add(5, new Dictionary<double, string>());

                        foreach (var pair in distinctXramKey2Value[rotateTo[0]])
                            tempKey2Value[5].Add(pair.Key, pair.Value.ToString()); // copy rotateTo to temp

                        distinctXramKey2Value[rotateTo[0]].Clear();

                        foreach (var pair in distinctYramKey2Value[rotateFrom[1]])
                            distinctXramKey2Value[rotateTo[0]].Add(pair.Key, pair.Value.ToString()); // copy rotateFrom to rotateTo

                        distinctYramKey2Value[rotateFrom[1]].Clear();

                        foreach (var pair in tempKey2Value[5])
                            distinctYramKey2Value[rotateFrom[1]].Add(pair.Key, pair.Value.ToString()); //copy temp to rotateFrom    
                    }

                    columnName2ID.Clear();

                    for (int i = 0; i < ramKey2Valuegz.Count; i++)
                        columnName2ID.Add(ramKey2Valuegz[i][0], i.ToString());
                }
               
                chanagePresentation.sortXYdata(distinctListChecksum, distinctSet, unsorted2SortedCheksum, sortedXdimension, sortedYdimension, requestID, requestDict, outputFolder, responseDict, distinctXramKey2Value, distinctYramKey2Value, distinctRamKey2Value, XdistinctList, YdistinctList, ramKey2Order, ramOrder2Key, crosstabDimension, yDimension, distinctList, revisedX, revisedY, measure, dataSortingOrder);

                Dictionary<double, decimal> crosstabAddress2DrillSetDict = new Dictionary<double, decimal>();
               // List<double> crosstabAddress2DrillSetList = new List<double>();
                Dictionary<int, List<double>> YXMdistinctDrillKey = new Dictionary<int, List<double>>();

                chanagePresentation.crosstab(YXMdistinctDrillKey, distinctDimensionChecksumList, crosstabAddress2DrillSetDict, sortedRevisedXY, sortedXdimension, sortedYdimension, csvWriteSeparator, outputFolder, requestID, responseDict, requestDict, measure, distinctList, XdistinctList, YdistinctList, distinctXramKey2Value, distinctYramKey2Value, YXMdistinctList, debug, xAddressCol, yAddressCol);                

                if (requestDict[requestID].debugOutput == "Y")
                {
                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "debugInfo.txt"))
                    {
                        toDisk.Write(debug);
                        toDisk.Close();
                        debug.Clear();
                    }
                }               

                chanagePresentation.startPaginationInYXM(ramDetailgz, tableFact, userPreference, screenControl, YXMdistinctDrillKey, distinctDimensionChecksumList, crosstabAddress2DrillSetDict,distinctSet, unsorted2SortedCheksum, distinctList2DrillDown, sortedXdimension, YXMdistinctList, requestDict, requestID, XdistinctList, YdistinctList, responseDict, htmlTable, _ramKey2Valuegz, ramKey2Valuegz, distinctXramKey2Value, distinctYramKey2Value, outputFolder, revisedY, measure, csvWriteSeparator, dataSortingOrder, presentationJob, stopPresentationThread, presentationThread);              
            }
        }
    }

    public class ChangeInReportPresentation
    {
        // YM route (no crosstab)
        public void sortSelectedYdimension(List<int> sortedYdimension, List<int> yDimension, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<int> revisedX, List<int> revisedY, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, StringBuilder debug)
        {
           
            for (int i = 0; i < yDimension.Count; i++)           
                distinctYramKey2Value[i] = ramKey2Valuegz[yDimension[i]];

            for (int i = 0; i < (revisedX.Count + revisedY.Count); i++) // revisedX and revisedY are selected dimensions for reporting
            {
                if (i < revisedY.Count)
                    sortedYdimension.Add(i);
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append(Environment.NewLine); debug.Append("sortedRevisedY");
                for (int i = 0; i < revisedY.Count; i++)
                {
                    debug.Append(i + " " + sortedYdimension[i]);
                    debug.Append(Environment.NewLine);
                }

                debug.Append(Environment.NewLine);
            }
        }
        public void sortYdata(List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, List<int> sortedYdimension, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, string outputFolder, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, Dictionary<int, List<double>> YdistinctList, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key, List<int> yDimension,  Dictionary<int, List<double>> distinctList, List<int> revisedY, Dictionary<int, string> dataSortingOrder)
        {
            var startSort = DateTime.Now;
            Console.WriteLine("interm Time : " + String.Format("{0:F3}", (startSort - responseDict[requestID].endDistinctTime).TotalSeconds) + " seconds");
            
            Dictionary<int, Dictionary<double, double>> distinctYramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> distinctYramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, List<double>> copyYdistinctList = new Dictionary<int, List<double>>();


            // select X, Y dimension from distinctList to output XdistinctList and YdistinctList
            if (requestDict[requestID].sortYdimension == "A" || requestDict[requestID].sortYdimension == "D")
            {
                Sorting currentSorting = new Sorting();
                currentSorting.sortingY(distinctListChecksum, distinctSet, unsorted2SortedCheksum, requestID, outputFolder, requestDict, responseDict, distinctYramKey2Value, distinctRamKey2Value, YdistinctList, sortedYdimension, ramKey2Order, ramOrder2Key, distinctYramKey2Order, distinctYramOrder2Key, copyYdistinctList, yDimension, distinctList, revisedY);
            }
            responseDict[requestID].sortedTime = DateTime.Now;
            Console.WriteLine("Sort Transaction Time : " + String.Format("{0:F3}", (responseDict[requestID].sortedTime - startSort).TotalSeconds) + " seconds");
        }
        public void startPaginationInYM(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, decimal requestID, ConcurrentDictionary<decimal, clientMachine.response> responseDict,  Dictionary<int, List<double>> distinctList, Dictionary<int, List<double>> YdistinctList, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, StringBuilder> htmlTable, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, string outputFolder, List<int> revisedY, List<int> measure, char csvWriteSeparator, Dictionary<int, string> sortingOrder, int presentationJob, ConcurrentDictionary<decimal, Thread> presentationThread, Dictionary<int, string> dataSortingOrder, Dictionary<decimal, List<int>> distinctList2DrillDown)
        {
            var startOutputdistinctTime2 = DateTime.Now;
            responseDict[requestID].distinctCount = Convert.ToDecimal(distinctList[0].Count) - 1;
            var startFirstPageTime = DateTime.Now; // Pagination for first page            
            int Ypage = 1;
            int XYmaxRow = requestDict[requestID].pageYlength;
            int Yrow = XYmaxRow - 1;
            double _YtotalPage = (distinctList[0].Count - 1) / Yrow;
            int YtotalPage = (int)Math.Floor(_YtotalPage) + 1;
            int YpageStart;
            int YpageEnd;

            if (userPreference["system"].nextDrillDownEventType == 0)
                userPreference["system"].drillDownEventType = "onmouseenter";

            if (userPreference["system"].nextDrillDownEventType == 1)
                userPreference["system"].drillDownEventType = "onclick";

            YpageStart = 1 + (Yrow * (Ypage - 1));
            if ((Ypage - 1) < YtotalPage)
                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
            else
                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 2)) + ((distinctList[0].Count - 1) % Yrow);

            if (requestDict[requestID].debugOutput == "Y")
                Console.WriteLine("nPage " + Ypage + " Yrow " + Yrow + " YtotalPage" + YtotalPage + " YpageStart " + YpageStart + " YpageEnd " + YpageEnd);

            responseDict[requestID].endTime = DateTime.Now;
            Console.WriteLine("final Time : " + String.Format("{0:F3}", (responseDict[requestID].endTime - responseDict[requestID].sortedTime).TotalSeconds) + " seconds");
            ExportHTML currentExport = new ExportHTML();
            currentExport.ramDistinct2HtmlYMtable(userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, Ypage, YtotalPage, YpageStart, YpageEnd, requestID, requestDict, responseDict, YdistinctList, distinctRamKey2Value, outputFolder, dataSortingOrder);
        
            FinalEvent currentLoop = new FinalEvent();
            currentLoop.reportEvent(ramDetailgz, userPreference, tableFact, remK2V, sourceFolder, db1Folder, _ramKey2Valuegz, screenControl, distinctSet, requestID, requestDict, responseDict, YpageStart, YpageEnd, Yrow, Ypage, YtotalPage, YdistinctList, ramKey2Valuegz, distinctRamKey2Value, outputFolder, csvWriteSeparator, sortingOrder, presentationJob, presentationThread, distinctList2DrillDown);           
        }

        // YXM route (crosstab)
        public void sortSelectedDimensionOrder(List<int> distinctDimension, List<int> crosstabDimension, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, StringBuilder debug, List<int> yDimension, Dictionary<int, string> xyDimension, List<int> revisedX, List<int> revisedY, Dictionary<int, string> columnMoveOrder)
        {
            int moveCol;
            string moveDirection;

            foreach (var pair in columnMoveOrder)
            {
                moveCol = pair.Key;
                moveDirection = pair.Value;

                if (moveDirection == "Y2X")
                    crosstabDimension.Add(moveCol);

                if (moveDirection == "X2Y")
                    for (int i = 0; i < crosstabDimension.Count; i++)
                        if (crosstabDimension[i] == moveCol)
                            crosstabDimension.RemoveAt(i);
            }

            xyDimension.Clear(); yDimension.Clear();
            revisedX.Clear(); revisedY.Clear();

            crosstabDimension.Sort();
            distinctDimension.Sort();

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append(Environment.NewLine);
                debug.Append("Sorted distinctDimension: ");
                foreach (int d in distinctDimension) debug.Append(d + " ");
                debug.Append(Environment.NewLine); debug.Append(Environment.NewLine);
            }

            // y dimension (number)
            for (int i = 0; i < distinctDimension.Count; i++)
            {
                if (!crosstabDimension.Contains(distinctDimension[i]))
                    yDimension.Add(distinctDimension[i]);
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
        public void sortSelectedXYdimension(List<int> sortedXdimension, List<int> sortedYdimension, List<int> sortedXYdimension, Dictionary<int, string> sortedRevisedXY, List<int> crosstabDimension, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<int> yDimension, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, List<int> revisedX, List<int> revisedY, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, StringBuilder debug)
        {
            Dictionary<int, Dictionary<double, string>> distinctXYramKey2Value = new Dictionary<int, Dictionary<double, string>>();

            for (int i = 0; i < crosstabDimension.Count; i++)
                distinctXramKey2Value[i] = ramKey2Valuegz[crosstabDimension[i]];  // crosstabDimension[i] retured X dimension in the sorted XY order 

            for (int i = 0; i < yDimension.Count; i++)
                distinctYramKey2Value[i] = ramKey2Valuegz[yDimension[i]];

            for (int i = 0; i < (revisedX.Count + revisedY.Count); i++) // revisedX and revisedY are selected dimensions for reporting
            {
                if (i < revisedX.Count) // reset X,Y dimension number from 0, to match with XdistinctList and YdistinctList
                {
                    sortedRevisedXY.Add(revisedX[i], "X");
                    sortedXdimension.Add(i);
                }

                if (i < revisedY.Count)
                {
                    sortedRevisedXY.Add(revisedY[i], "Y");
                    sortedYdimension.Add(i);
                }
            }

            for (int i = 0; i < sortedRevisedXY.Count; i++)
                sortedXYdimension.Add(i);

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append(Environment.NewLine); debug.Append("sortedRevisedX"); debug.Append(Environment.NewLine);
                for (int i = 0; i < revisedX.Count; i++)
                {
                    debug.Append(i + " " + sortedXdimension[i]);
                    debug.Append(Environment.NewLine);
                }

                debug.Append(Environment.NewLine); debug.Append("sortedRevisedY"); debug.Append(Environment.NewLine);
                for (int i = 0; i < revisedY.Count; i++)
                {
                    debug.Append(i + " " + sortedYdimension[i]);
                    debug.Append(Environment.NewLine);
                }

                debug.Append(Environment.NewLine); debug.Append("sortedRevisedXY"); debug.Append(Environment.NewLine);
                for (int i = 0; i < sortedRevisedXY.Count; i++)
                {
                    debug.Append(i + " " + sortedRevisedXY[i] + " " + sortedXYdimension[i]);
                    debug.Append(Environment.NewLine);
                }

                debug.Append(Environment.NewLine);
            }

            int xCol = 0;
            int yCol = 0;

            for (int i = 0; i < sortedRevisedXY.Count; i++)
            {
                if (sortedRevisedXY[i] == "X")
                {
                    distinctXYramKey2Value[i] = distinctXramKey2Value[xCol]; // key to value for X dimensions
                    xCol++;
                }

                if (sortedRevisedXY[i] == "Y")
                {
                    distinctXYramKey2Value[i] = distinctYramKey2Value[yCol]; // key to value for Y dimensions
                    yCol++;
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
                debug.Append("distinctXYramKey2Value.Count" + " " + distinctXYramKey2Value.Count); debug.Append(Environment.NewLine);
        }
        public void sortXYdata(List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, List<int> sortedXdimension, List<int> sortedYdimension, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, string outputFolder, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key, List<int> crosstabDimension, List<int> yDimension,  Dictionary<int, List<double>> distinctList, List<int> revisedX, List<int> revisedY, List<int> measure, Dictionary<int, string> dataSortingOrder)
        {
            Dictionary<int, Dictionary<double, double>> distinctXramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> distinctXramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> distinctYramKey2Order = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, Dictionary<double, double>> distinctYramOrder2Key = new Dictionary<int, Dictionary<double, double>>();
            Dictionary<int, List<double>> copyXdistinctList = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> copyYdistinctList = new Dictionary<int, List<double>>();

            // select X, Y dimension from distinctList to output XdistinctList and YdistinctList
            if (requestDict[requestID].sortXdimension == "A" || requestDict[requestID].sortXdimension == "D" || requestDict[requestID].sortYdimension == "A" || requestDict[requestID].sortYdimension == "D")
            {
                var startSort = DateTime.Now;
                Sorting currentSorting = new Sorting();
                currentSorting.sortingXY(distinctListChecksum, distinctSet, unsorted2SortedCheksum,requestID, outputFolder, requestDict, responseDict, distinctXramKey2Value, distinctYramKey2Value, distinctRamKey2Value, XdistinctList, YdistinctList, sortedXdimension, sortedYdimension, ramKey2Order, ramOrder2Key, distinctXramKey2Order, distinctXramOrder2Key, distinctYramKey2Order, distinctYramOrder2Key, copyXdistinctList, copyYdistinctList, crosstabDimension, yDimension, distinctList, revisedX, revisedY);
                var endSort = DateTime.Now;
                Console.WriteLine("Total Sort Time : " + String.Format("{0:F3}", (endSort - startSort).TotalSeconds) + " seconds");
            }

            // select X, Y dimension from distinctList to output XdistinctList and YdistinctList
            if (requestDict[requestID].sortXdimension != "A" && requestDict[requestID].sortXdimension != "D" && requestDict[requestID].sortYdimension != "A" && requestDict[requestID].sortYdimension != "D")
            {
                Distinct currentdistinct = new Distinct();
               
                XdistinctList = currentdistinct.distinctDB(distinctList, distinctRamKey2Value, revisedX);
                YdistinctList = currentdistinct.distinctDB(distinctList, distinctRamKey2Value, revisedY);
               
            }
            responseDict[requestID].distinctCount = Convert.ToDecimal(distinctList[0].Count) - 1;
            responseDict[requestID].crosstabCount = Convert.ToDecimal(XdistinctList[0].Count) * Convert.ToDecimal(YdistinctList[0].Count) * Convert.ToDecimal(measure.Count);
        }        
        public void crosstab(Dictionary<int, List<double>> YXMdistinctDrillKey, Dictionary<decimal, int> distinctDimensionChecksumList, Dictionary<double, decimal> crosstabAddress2DrillSetDict, Dictionary<int, string> sortedRevisedXY, List<int> sortedXdimension, List<int> sortedYdimension, char csvWriteSeparator, string outputFolder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.response> responseDict, ConcurrentDictionary<decimal, clientMachine.request> requestDict, List<int> measure,  Dictionary<int, List<double>> distinctList, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, Dictionary<int, List<double>> YXMdistinctList, StringBuilder debug, int xAddressCol, int yAddressCol)
        {
            Dictionary<int, List<double>> sortedXYdistinctList = new Dictionary<int, List<double>>(); // sorted & combined very long of X, Y distinctList     
            Crosstab currentCrosstab = new Crosstab();  
            var start = DateTime.Now;            
            currentCrosstab.crosstabByCopyZero(YXMdistinctDrillKey, distinctDimensionChecksumList, crosstabAddress2DrillSetDict, csvWriteSeparator, outputFolder, requestID, requestDict, measure, distinctList, XdistinctList, YdistinctList, sortedRevisedXY, distinctXramKey2Value, distinctYramKey2Value, sortedXdimension, sortedYdimension, YXMdistinctList, debug, xAddressCol, yAddressCol);

            var end = DateTime.Now;
            Console.WriteLine("Total Crosstab Time : " + String.Format("{0:F3}", (end - start).TotalSeconds) + " seconds");
        }       
        public void startPaginationInYXM(Dictionary<int, List<double>> ramDetailgz, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, Dictionary<int, List<double>> YXMdistinctDrillKey, Dictionary<decimal, int> distinctDimensionChecksumList, Dictionary<double, decimal> crosstabAddress2DrillSetDict, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, Dictionary<decimal, List<int>> distinctList2DrillDown, List<int> sortedXdimension, Dictionary<int, List<double>> YXMdistinctList, ConcurrentDictionary<decimal, clientMachine.request> requestDict, decimal requestID, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, StringBuilder> htmlTable, Dictionary<int, Dictionary<double, string>> _ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, string outputFolder, List<int> revisedY, List<int> measure, char csvWriteSeparator, Dictionary<int, string> sortingOrder, int presentationJob, Dictionary<int, int> stopPresentationThread, ConcurrentDictionary<decimal, Thread> presentationThread)
        {
            var startFirstPageTime = DateTime.Now;  // Pagination for first page
            int yHeaderCol = 0;
            for (int cell = 0; cell < YXMdistinctList.Count; cell++)
            {
                if (YXMdistinctList[cell][0] == 0)
                    yHeaderCol++;
                else
                    break;
            }
            int xmHeaderRow = sortedXdimension.Count + 1;
            int Xpage = 1;
            int YXmaxColumn;

            if (requestDict[requestID].processButton == "moveColumn")
                YXmaxColumn = requestDict[requestID].pageXlengthCrosstab;
            else
                YXmaxColumn = requestDict[requestID].pageXlength;

            int Xcolumn = (YXmaxColumn - yHeaderCol - 1) / requestDict[requestID].measurement.Count;            
            if (Xcolumn < 1) Xcolumn = 1;
            double _XtotalPage = (XdistinctList[0].Count - 1) / Xcolumn;
            int XtotalPage = (int)Math.Floor(_XtotalPage);
            int XpageStart;
            int XpageEnd;

            XpageStart = 0 + (Xcolumn * (Xpage - 1));
            if ((Xpage - 1) < XtotalPage)
                XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 1));
            else
                XpageEnd = (Xcolumn - 1) + (Xcolumn * (Xpage - 2)) + ((XdistinctList[0].Count - 1) % Xcolumn);

            int Ypage = 1;
            int XYmaxRow;

            if (requestDict[requestID].processButton == "moveColumn")
                XYmaxRow = requestDict[requestID].pageYlengthCrosstab;
            else
                XYmaxRow = requestDict[requestID].pageYlength;

            int Yrow = XYmaxRow - xmHeaderRow;

            double _YtotalPage = (YdistinctList[0].Count - 1) / Yrow;
            int YtotalPage = (int)Math.Floor(_YtotalPage) + 1;
            int YpageStart;
            int YpageEnd;

            YpageStart = 1 + (Yrow * (Ypage - 1));
            if ((Ypage - 1) < YtotalPage)
                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 1));
            else
                YpageEnd = 1 + Yrow + (Yrow * (Ypage - 2)) + ((YdistinctList[0].Count - 1) % Yrow);

            if (requestDict[requestID].debugOutput == "Y")
            {
                Console.WriteLine("nPage " + Xpage + " Xcolumn " + Xcolumn + " XtotalPage" + XtotalPage + " XpageStart " + XpageStart + " XpageEnd " + XpageEnd);
                Console.WriteLine("nPage " + Ypage + " Yrow " + Yrow + " YtotalPage" + YtotalPage + " YpageStart " + YpageStart + " YpageEnd " + YpageEnd);
            }

            responseDict[requestID].endTime = DateTime.Now;            
            Console.WriteLine("End milliseconds: {0:MM/dd/yyy HH:mm:ss.fff}", DateTime.Now);

            ExportHTML currentExport = new ExportHTML();
            currentExport.ramDistinct2HtmlCrosstabTable(tableFact, userPreference, _ramKey2Valuegz, screenControl, YXMdistinctDrillKey, Xpage, XtotalPage, Ypage, YtotalPage, XpageStart, XpageEnd, yHeaderCol, YpageStart, YpageEnd, XdistinctList, requestID, requestDict, responseDict, YXMdistinctList,distinctXramKey2Value, distinctYramKey2Value, outputFolder, sortingOrder);           

            FinalEvent currentLoop = new FinalEvent();
            currentLoop.crosstabReportEvent(ramDetailgz, tableFact, userPreference, screenControl, YXMdistinctDrillKey, crosstabAddress2DrillSetDict, distinctSet, unsorted2SortedCheksum, distinctYramKey2Value, distinctList2DrillDown, requestID, requestDict, responseDict, Xpage, XpageStart, XpageEnd, Xcolumn, yHeaderCol, YpageStart, YpageEnd, Yrow, Ypage, YtotalPage, XtotalPage, htmlTable, XdistinctList, YdistinctList, YXMdistinctList, _ramKey2Valuegz, ramKey2Valuegz, distinctXramKey2Value, distinctYramKey2Value, outputFolder, revisedY, measure, csvWriteSeparator, sortingOrder, presentationJob, stopPresentationThread, presentationThread);
            
        }
    }
}
