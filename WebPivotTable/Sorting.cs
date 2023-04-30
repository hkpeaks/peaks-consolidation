using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace youFast
{
    public class Sorting
    {
        public decimal sortingChecksum { get; set; }
        public int sortingOrder { get; set; }
        public void sortingY(List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, decimal requestID, string outputFolder, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, List<double>> YdistinctList, List<int> sortedYdimension, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key, Dictionary<int, Dictionary<double, double>> distinctYramKey2Order, Dictionary<int, Dictionary<double, double>> distinctYramOrder2Key, Dictionary<int, List<double>> copyYdistinctList, List<int> yDimension,  Dictionary<int, List<double>> distinctList, List<int> revisedY)
        {          
            
            for (int i = 0; i < yDimension.Count; i++)
            {  
                distinctYramKey2Order[i] = ramKey2Order[yDimension[i]];
                distinctYramOrder2Key[i] = ramOrder2Key[yDimension[i]];
            }          

            // select X, Y dimension from distinctList to output XdistinctList and YdistinctList
            Distinct currentdistinct = new Distinct();   
            copyYdistinctList = currentdistinct.distinctDB(distinctList, distinctRamKey2Value, revisedY); // get distinct distinctList by selected Y dimensions    
            Dictionary<int, List<double>> tempYdistinctList = new Dictionary<int, List<double>>();
            List<decimal> YdimensionSortingChecksumList = new List<decimal>(); // ChecksumList for Sorting Y dimensions           
            List<decimal> YdimensionChecksumList = new List<decimal>(); // ChecksumList Y dimensions without sorting                      
            List<Sorting> checksum2OrderY = new List<Sorting>();
            int eachChecksum2OrderRow = 0;
            StringBuilder csvString = new StringBuilder();

            if (requestDict[requestID].sortYdimension == "A" || requestDict[requestID].sortYdimension == "D") // Sort Y Dimension ////////////////////////////////////////////
            {
                var startSortYTime = DateTime.Now;
                if (requestDict[requestID].debugOutput == "Y")
                {
                    if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                        Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "YdistinctList.csv"))
                    {
                        csvString.Append("distinctYramKey2Value[0][i]" + "," + "distinctYramKey2Value[i][copyYdistinctList[i][j]]" + "," + "YdistinctList[i][j]" + Environment.NewLine);
                        for (int i = 0; i < copyYdistinctList.Count; i++) // output YdistinctList
                        {
                            for (int j = 0; j < copyYdistinctList[i].Count; j++)
                                csvString.Append(distinctYramKey2Value[i][0] + "," + distinctYramKey2Value[i][copyYdistinctList[i][j]] + "," + copyYdistinctList[i][j] + Environment.NewLine);
                        }
                        toDisk.Write(csvString);
                        toDisk.Close();
                        csvString.Clear();
                    }
                }
                
                for (int i = 0; i < copyYdistinctList.Count; i++) // convert key to order and save as tempYdistinctList
                {
                    tempYdistinctList.Add(i, new List<double>());

                    for (int j = 0; j < copyYdistinctList[i].Count; j++)
                        tempYdistinctList[i].Add(distinctYramKey2Order[i][copyYdistinctList[i][j]]); // convert master key to sorting order of the key                                            
                }

                Distinct getXY = new Distinct(); // get checkSum List with and without sorting

                // return Y dimensionSortingChecksumList
              
                YdimensionSortingChecksumList = getXY.getXYcheckSumList(tempYdistinctList, distinctYramKey2Value, sortedYdimension);
                for (int i = 0; i < YdimensionSortingChecksumList.Count; i++)
                {
                    var checksum2OrderYRow = new Sorting
                    {
                        sortingChecksum = YdimensionSortingChecksumList[i],
                        sortingOrder = i
                    };
                    checksum2OrderY.Add(checksum2OrderYRow);
                }               

                if (requestDict[requestID].sortYdimension == "A")
                {
                    var sortChecksum2OrderY = from eachChecksum2OrderY in checksum2OrderY
                                              orderby eachChecksum2OrderY.sortingChecksum ascending
                                              select eachChecksum2OrderY;

                    outputsortChecksum2OrderY(sortChecksum2OrderY);
                }

                if (requestDict[requestID].sortYdimension == "D")
                {
                    var sortChecksum2OrderY = from eachChecksum2OrderY in checksum2OrderY
                                              orderby eachChecksum2OrderY.sortingChecksum descending
                                              select eachChecksum2OrderY;

                    outputsortChecksum2OrderY(sortChecksum2OrderY);
                }

                void outputsortChecksum2OrderY(IOrderedEnumerable<Sorting> sortChecksum2OrderY) // output sorting result to csv and to YdistinctList
                {
                    for (int i = 0; i < copyYdistinctList.Count; i++)
                    {
                        foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                        {                          
                            var key = distinctListChecksum[eachChecksum2OrderY.sortingOrder];
                            if (eachChecksum2OrderY.sortingOrder != 0 && !unsorted2SortedCheksum.ContainsKey(key))
                            {
                                unsorted2SortedCheksum.Add(key, eachChecksum2OrderY.sortingChecksum);
                                distinctSet.Add(key);                                   
                            }                                               
                        }
                        eachChecksum2OrderRow++;
                    }

                  

                    if (requestDict[requestID].debugOutput == "Y")
                    {
                        if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                            Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                        using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "sortChecksum2OrderY.csv"))
                        {
                            eachChecksum2OrderRow = 0;
                            csvString.Append("column" + "," + "sortingChecksum" + "," + "sortingOrder" + "," + "YdistinctList[i][eachChecksum2OrderY.sortingOrder]" + "," + "distinctYramKey2Value[i][YdistinctList[i][eachChecksum2OrderY.sortingOrder]]" + Environment.NewLine);

                            for (int i = 0; i < copyYdistinctList.Count; i++)
                            {
                                csvString.Append("0" + "," + "0" + "," + distinctYramKey2Value[i][copyYdistinctList[i][0]] + "," + copyYdistinctList[i][0] + Environment.NewLine);

                                foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                                    if (eachChecksum2OrderY.sortingOrder != 0)
                                        csvString.Append(distinctYramKey2Value[i][0] + "," + eachChecksum2OrderY.sortingChecksum + "," + eachChecksum2OrderY.sortingOrder + "," + copyYdistinctList[i][eachChecksum2OrderY.sortingOrder] + "," + distinctYramKey2Value[i][copyYdistinctList[i][eachChecksum2OrderY.sortingOrder]] + Environment.NewLine);

                                eachChecksum2OrderRow++;
                            }
                            toDisk.Write(csvString);
                            toDisk.Close();
                            csvString.Clear();
                        }
                    }

                    eachChecksum2OrderRow = 0;
                    if (YdistinctList.ContainsKey(0))                    
                        YdistinctList.Remove(0);

                    for (int i = 0; i < copyYdistinctList.Count; i++)
                    {
                        YdistinctList.Add(i, new List<double>());
                        YdistinctList[i].Add(0);
                        foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                        {
                            if (eachChecksum2OrderY.sortingOrder != 0)
                            {
                                YdistinctList[i].Add(copyYdistinctList[i][eachChecksum2OrderY.sortingOrder]);
                            }
                            eachChecksum2OrderRow++;
                        }
                    }

                    for (int i = copyYdistinctList.Count; i < distinctList.Count; i++)
                    {
                        YdistinctList.Add(i, new List<double>());
                        YdistinctList[i].Add(0);
                        foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                        {
                            if (eachChecksum2OrderY.sortingOrder != 0)
                            {
                                YdistinctList[i].Add(distinctList[i][eachChecksum2OrderY.sortingOrder]);
                            }
                            eachChecksum2OrderRow++;
                        }
                    }
                }
            }
        }
        public void sortingXY(List<decimal> distinctListChecksum, List<decimal> distinctSet, Dictionary<decimal, decimal> unsorted2SortedCheksum, decimal requestID, string outputFolder, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value,  Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, List<int> sortedXdimension, List<int> sortedYdimension, Dictionary<int, Dictionary<double, double>> ramKey2Order, Dictionary<int, Dictionary<double, double>> ramOrder2Key, Dictionary<int, Dictionary<double, double>> distinctXramKey2Order, Dictionary<int, Dictionary<double, double>> distinctXramOrder2Key, Dictionary<int, Dictionary<double, double>> distinctYramKey2Order, Dictionary<int, Dictionary<double, double>> distinctYramOrder2Key, Dictionary<int, List<double>> copyXdistinctList, Dictionary<int, List<double>> copyYdistinctList, List<int> crosstabDimension, List<int> yDimension,  Dictionary<int, List<double>> distinctList, List<int> revisedX, List<int> revisedY)
        {
            // reorganize master (key to value) for X,Y distinctList by assigned "=" function
            for (int i = 0; i < crosstabDimension.Count; i++)
            {  
                distinctXramKey2Order[i] = ramKey2Order[crosstabDimension[i]];
                distinctXramOrder2Key[i] = ramOrder2Key[crosstabDimension[i]];
            }

            for (int i = 0; i < yDimension.Count; i++)
            {
               // distinctYramKey2Value[i] = ramKey2Valuegz[yDimension[i]];
                distinctYramKey2Order[i] = ramKey2Order[yDimension[i]];
                distinctYramOrder2Key[i] = ramOrder2Key[yDimension[i]];
            }

            // select X, Y dimension from distinctList to output XdistinctList and YdistinctList
            Distinct currentdistinct = new Distinct();                      
            copyXdistinctList = currentdistinct.distinctDB(distinctList, distinctRamKey2Value, revisedX); // get distinct distinctList by selected X dimensions 
            copyYdistinctList = currentdistinct.distinctDB(distinctList, distinctRamKey2Value, revisedY); // get distinct distinctList by selected Y dimensions          

            Dictionary<int, List<double>> tempXdistinctList = new Dictionary<int, List<double>>();
            Dictionary<int, List<double>> tempYdistinctList = new Dictionary<int, List<double>>();        
            List<decimal> XdimensionSortingChecksumList = new List<decimal>(); // ChecksumList for Sorting of X dimensions
            List<decimal> YdimensionSortingChecksumList = new List<decimal>(); // ChecksumList for Sorting Y dimensions            
            List<Sorting> checksum2OrderX = new List<Sorting>();
            List<Sorting> checksum2OrderY = new List<Sorting>();            
            int eachChecksum2OrderRow = 0;
            StringBuilder csvString = new StringBuilder();

            if (requestDict[requestID].sortXdimension == "A" || requestDict[requestID].sortXdimension == "D") // Sort X Dimension ////////////////////////////////////////////
            {
                var startSortXTime = DateTime.Now;
                if (requestDict[requestID].debugOutput == "Y")
                {
                    if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                        Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "XdistinctList.csv"))
                    {
                        csvString.Append("distinctXramKey2Value[0][i]" + "," + "distinctXramKey2Value[i][XdistinctList[i][j]]" + "," + "XdistinctList[i][j]" + Environment.NewLine);
                        for (int i = 0; i < copyXdistinctList.Count; i++) // output XdistinctList
                        {
                            for (int j = 0; j < copyXdistinctList[i].Count; j++)
                                csvString.Append(distinctXramKey2Value[i][0] + "," + distinctXramKey2Value[i][copyXdistinctList[i][j]] + "," + copyXdistinctList[i][j] + Environment.NewLine);
                        }
                        toDisk.Write(csvString);
                        toDisk.Close();
                        csvString.Clear();
                    }
                }
                
                for (int i = 0; i < copyXdistinctList.Count; i++) // convert key to order and save as tempXdistinctList
                {
                    tempXdistinctList.Add(i, new List<double>());

                    for (int j = 0; j < copyXdistinctList[i].Count; j++)
                        tempXdistinctList[i].Add(distinctXramKey2Order[i][copyXdistinctList[i][j]]); // convert master key to sorting order of the key                             
                }

                // return Y dimensionSortingChecksumList
                Distinct getXY = new Distinct();               
              
                XdimensionSortingChecksumList = getXY.getXYcheckSumList(tempXdistinctList, distinctXramKey2Value, sortedXdimension);

                for (int i = 0; i < XdimensionSortingChecksumList.Count; i++)
                {
                    var checksum2OrderXRow = new Sorting
                    {
                        sortingChecksum = XdimensionSortingChecksumList[i],
                        sortingOrder = i
                    };
                    checksum2OrderX.Add(checksum2OrderXRow);
                }               

                if (requestDict[requestID].sortXdimension == "A")  // Sort checksum2OrderX by ascending or descending 
                //if (dataSortingOrder[0] == "sortAscending")
                {
                    var sortChecksum2OrderX = from eachChecksum2OrderX in checksum2OrderX
                                              orderby eachChecksum2OrderX.sortingChecksum ascending
                                              select eachChecksum2OrderX;

                    outputsortChecksum2OrderX(sortChecksum2OrderX);
                }

                if (requestDict[requestID].sortXdimension == "D")              
                {
                    var sortChecksum2OrderX = from eachChecksum2OrderX in checksum2OrderX
                                              orderby eachChecksum2OrderX.sortingChecksum descending
                                              select eachChecksum2OrderX;

                    outputsortChecksum2OrderX(sortChecksum2OrderX);
                }              
            }

            void outputsortChecksum2OrderX(IOrderedEnumerable<Sorting>sortChecksum2OrderX) // output sorting result to csv and to XdistinctList
            {
                if (requestDict[requestID].debugOutput == "Y")
                {
                    if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                        Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "sortChecksum2OrderX.csv"))
                    {
                        eachChecksum2OrderRow = 0;
                        csvString.Append("column" + "," + "sortingChecksum" + "," + "sortingOrder" + "," + "XdistinctList[i][eachChecksum2OrderX.sortingOrder]" + "," + "distinctXramKey2Value[i][XdistinctList[i][eachChecksum2OrderX.sortingOrder]]" + Environment.NewLine);

                        for (int i = 0; i < copyXdistinctList.Count; i++)
                        {
                            csvString.Append("0" + "," + "0" + "," + distinctXramKey2Value[i][copyXdistinctList[i][0]] + "," + copyXdistinctList[i][0] + Environment.NewLine);

                            foreach (var eachChecksum2OrderX in sortChecksum2OrderX)
                                if (eachChecksum2OrderX.sortingOrder != 0)
                                    csvString.Append(distinctXramKey2Value[i][0] + "," + eachChecksum2OrderX.sortingChecksum + "," + eachChecksum2OrderX.sortingOrder + "," + copyXdistinctList[i][eachChecksum2OrderX.sortingOrder] + "," + distinctXramKey2Value[i][copyXdistinctList[i][eachChecksum2OrderX.sortingOrder]] + Environment.NewLine);

                            eachChecksum2OrderRow++;
                        }
                        toDisk.Write(csvString);
                        toDisk.Close();
                        csvString.Clear();
                    }
                }

                eachChecksum2OrderRow = 0;
                
                for (int i = 0; i < copyXdistinctList.Count; i++)
                {
                    XdistinctList.Add(i, new List<double>());
                    XdistinctList[i].Add(0);
                    foreach (var eachChecksum2OrderX in sortChecksum2OrderX)
                    {
                        if (eachChecksum2OrderX.sortingOrder != 0)
                        {
                            XdistinctList[i].Add(copyXdistinctList[i][eachChecksum2OrderX.sortingOrder]);
                        }
                        eachChecksum2OrderRow++;
                    }
                }
            }

            if (requestDict[requestID].sortYdimension == "A" || requestDict[requestID].sortYdimension == "D") // Sort Y Dimension ////////////////////////////////////////////
            {
                var startSortYTime = DateTime.Now;
                if (requestDict[requestID].debugOutput == "Y")
                {
                    if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                        Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                    using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "YdistinctList.csv"))
                    {
                        csvString.Append("distinctYramKey2Value[0][i]" + "," + "distinctYramKey2Value[i][copyYdistinctList[i][j]]" + "," + "YdistinctList[i][j]" + Environment.NewLine);
                        for (int i = 0; i < copyYdistinctList.Count; i++) // output YdistinctList
                        {
                            for (int j = 0; j < copyYdistinctList[i].Count; j++)
                                csvString.Append(distinctYramKey2Value[i][0] + "," + distinctYramKey2Value[i][copyYdistinctList[i][j]] + "," + copyYdistinctList[i][j] + Environment.NewLine);                            
                        }
                        toDisk.Write(csvString);
                        toDisk.Close();
                        csvString.Clear();
                    }
                }
                
                for (int i = 0; i < copyYdistinctList.Count; i++) // convert key to order and save as tempYdistinctList
                {
                    tempYdistinctList.Add(i, new List<double>());

                    for (int j = 0; j < copyYdistinctList[i].Count; j++)
                        tempYdistinctList[i].Add(distinctYramKey2Order[i][copyYdistinctList[i][j]]); // convert master key to sorting order of the key
                }

                // return Y dimensionSortingChecksumList
                Distinct getXY = new Distinct();
              
                YdimensionSortingChecksumList = getXY.getXYcheckSumList(tempYdistinctList, distinctYramKey2Value, sortedYdimension);

                for (int i = 0; i < YdimensionSortingChecksumList.Count; i++)
                {
                    var checksum2OrderYRow = new Sorting
                    {
                        sortingChecksum = YdimensionSortingChecksumList[i],
                        sortingOrder = i
                    };
                    checksum2OrderY.Add(checksum2OrderYRow);
                }               

                if (requestDict[requestID].sortYdimension == "A")               
                {
                    var sortChecksum2OrderY = from eachChecksum2OrderY in checksum2OrderY
                                              orderby eachChecksum2OrderY.sortingChecksum ascending
                                              select eachChecksum2OrderY;                   

                    outputsortChecksum2OrderY(sortChecksum2OrderY);
                }


                if (requestDict[requestID].sortYdimension == "D")              
                {
                    var sortChecksum2OrderY = from eachChecksum2OrderY in checksum2OrderY
                                              orderby eachChecksum2OrderY.sortingChecksum descending
                                              select eachChecksum2OrderY;

                    outputsortChecksum2OrderY(sortChecksum2OrderY);
                }

                void outputsortChecksum2OrderY(IOrderedEnumerable< Sorting> sortChecksum2OrderY) // output sorting result to csv and to YdistinctList
                {
                    if (requestDict[requestID].debugOutput == "Y")
                    {
                        if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                            Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                        using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "sortChecksum2OrderY.csv"))
                        {
                            eachChecksum2OrderRow = 0;
                            csvString.Append("column" + "," + "sortingChecksum" + "," + "sortingOrder" + "," + "YdistinctList[i][eachChecksum2OrderY.sortingOrder]" + "," + "distinctYramKey2Value[i][YdistinctList[i][eachChecksum2OrderY.sortingOrder]]" + Environment.NewLine);

                            for (int i = 0; i < copyYdistinctList.Count; i++)
                            {
                                csvString.Append("0" + "," + "0" + "," + distinctYramKey2Value[i][copyYdistinctList[i][0]] + "," + copyYdistinctList[i][0] + Environment.NewLine);

                                foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                                    if (eachChecksum2OrderY.sortingOrder != 0)
                                    {   
                                        csvString.Append(distinctYramKey2Value[i][0] + "," + eachChecksum2OrderY.sortingChecksum + "," + eachChecksum2OrderY.sortingOrder + "," + copyYdistinctList[i][eachChecksum2OrderY.sortingOrder] + "," + distinctYramKey2Value[i][copyYdistinctList[i][eachChecksum2OrderY.sortingOrder]] + Environment.NewLine);
                                    }

                                eachChecksum2OrderRow++;
                            }
                            toDisk.Write(csvString);
                            toDisk.Close();
                            csvString.Clear();
                        }
                    }
                    
                    for (int i = 0; i < copyYdistinctList.Count; i++)
                    {
                        foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                        {
                            var key = distinctListChecksum[eachChecksum2OrderY.sortingOrder];
                            if (eachChecksum2OrderY.sortingOrder != 0 && !unsorted2SortedCheksum.ContainsKey(key))
                            {
                                unsorted2SortedCheksum.Add(key, eachChecksum2OrderY.sortingChecksum);
                                distinctSet.Add(key);
                            }
                        }
                        eachChecksum2OrderRow++;
                    }                   

                    eachChecksum2OrderRow = 0;
                    
                    for (int i = 0; i < copyYdistinctList.Count; i++)
                    {
                        YdistinctList.Add(i, new List<double>());
                        YdistinctList[i].Add(0);
                        foreach (var eachChecksum2OrderY in sortChecksum2OrderY)
                        {
                            if (eachChecksum2OrderY.sortingOrder != 0)
                            {
                                YdistinctList[i].Add(copyYdistinctList[i][eachChecksum2OrderY.sortingOrder]);
                            }
                            eachChecksum2OrderRow++;
                        }
                    }
                }
            }
        }
    }
}
