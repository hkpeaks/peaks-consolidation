using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace youFast
{
    public class Distinct
    {   
        public Dictionary<int, List<double>> distinctDB( Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, List<int> dimension)
        {
            Dictionary<int, List<double>> currentdistinct = new Dictionary<int, List<double>>();

            int unique = 0;

            for (int i = 0; i < dimension.Count; i++)
                currentdistinct.Add(i, new List<double>());

            for (int i = 0; i < dimension.Count; i++)
                currentdistinct[i].Add(distinctList[dimension[i]][0]); // add dimension column

            List<Double> elementCount = new List<double>();
            for (int i = dimension.Count; i > 0; i--)
                elementCount.Add(distinctRamKey2Value[dimension[i - 1]].Count); // master item for each dimension column

            List<Double> logElementCount = new List<double>();

            for (int i = dimension.Count - 1; i > 0; i--)
            {
                if (i == dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(distinctRamKey2Value[dimension[i]].Count, 10) + 0.5000001), 0));
                if (i < dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(distinctRamKey2Value[dimension[i]].Count, 10) + 0.5000001), 0) + logElementCount[dimension.Count - 2 - i]);
            }

            List<Double> factor = new List<double>();
            for (int i = 0; i < (dimension.Count - 1); i++)
                factor.Add(Math.Pow(10, logElementCount[i]));

            Dictionary<decimal, int> distinctDimensionChecksumList = new Dictionary<decimal, int>();

            decimal distinctDimensionChecksum;

            for (int line = 1; line < distinctList[0].Count; line++)
            {
                distinctDimensionChecksum = 0;
                distinctSum(line);
            }

            void distinctSum(int line)
            {
                for (int i = 0; i < dimension.Count; i++) // convert multiple dimension value to an unique number 
                {
                    if (i < dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(distinctList[dimension[i]][line] * factor[dimension.Count - 2 - i]);
                    if (i == dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(distinctList[dimension[i]][line]);
                }

                if (!distinctDimensionChecksumList.ContainsKey(distinctDimensionChecksum))
                {
                    unique++;
                    distinctDimensionChecksumList.Add(distinctDimensionChecksum, unique);

                    for (int i = 0; i < dimension.Count; i++)
                        currentdistinct[i].Add(distinctList[dimension[i]][line]); // add dimension value for first unique item                   
                }
                else // addition = current value + last value of the same key
                {
                    // distinct only without sum on amount           
                }
            }
            return currentdistinct;
        }        
        public List<decimal> getXYcheckSumList(Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, List<int> dimension)
        {
            Dictionary<int, List<double>> currentdistinct = new Dictionary<int, List<double>>();

            decimal XYdimensionChecksum;
            List<decimal> XYdimensionChecksumList = new List<decimal>(); // record checksum for each multiple dimension            
            
            for (int i = 0; i < dimension.Count; i++)
                currentdistinct.Add(i, new List<double>());

            for (int i = 0; i < dimension.Count; i++)
                currentdistinct[i].Add(distinctList[dimension[i]][0]); // add dimension column

            List<Double> elementCount = new List<double>();
            for (int i = dimension.Count; i > 0; i--)
                elementCount.Add(distinctRamKey2Value[dimension[i - 1]].Count); // master item for each dimension column

            List<Double> logElementCount = new List<double>();

            for (int i = dimension.Count - 1; i > 0; i--)
            {
                if (i == dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(distinctRamKey2Value[dimension[i]].Count, 10) + 0.5000001), 0));
                if (i < dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(distinctRamKey2Value[dimension[i]].Count, 10) + 0.5000001), 0) + logElementCount[dimension.Count - 2 - i]);
            }

            List<Double> factor = new List<double>();
            for (int i = 0; i < (dimension.Count - 1); i++)
                factor.Add(Math.Pow(10, logElementCount[i]));

            for (int line = 0; line < distinctList[0].Count; line++)
            {
                XYdimensionChecksum = 0;
                distinctSum(line);
            }

            void distinctSum(int line)
            {
                for (int i = 0; i < dimension.Count; i++) // convert multiple dimension value to an unique number 
                {
                    if (i < dimension.Count - 1) XYdimensionChecksum = XYdimensionChecksum + Convert.ToDecimal(distinctList[dimension[i]][line] * factor[dimension.Count - 2 - i]);
                    if (i == dimension.Count - 1) XYdimensionChecksum = XYdimensionChecksum + Convert.ToDecimal(distinctList[dimension[i]][line]);
                }

                XYdimensionChecksumList.Add(XYdimensionChecksum);

                for (int i = 0; i < dimension.Count; i++)
                    currentdistinct[i].Add(distinctList[dimension[i]][line]); // add dimension value for first unique item                   
            }
            return XYdimensionChecksumList;
        }       
        public (Dictionary<int, List<double>> _distinctList, Dictionary<decimal, List<int>> _distinctList2DrillDown, List<decimal> _distinctListChecksum, Dictionary<decimal, int> _distinctDimensionChecksumList) filter2DistinctDB(ConcurrentQueue<int> checkSegmentThreadCompleted, int currentSegment, List<int> ramDetailSegment, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, Dictionary<int, List<int>> measurementColumn, bool noMultipleMeasurementColumn, Dictionary<int, List<string>> measurementOperator, Dictionary<int, List<double>> measurementRange,  Dictionary<int, List<double>> ramDetailgz, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<int> dimension, List<int> measure, Dictionary<int, Dictionary<double, string>> dimensionCriteria, List<int> crosstabDimension, StringBuilder debug)
        {
            List<decimal> _distinctListChecksum = new List<decimal>();
            Dictionary<decimal, List<int>> _distinctList2DrillDown = new Dictionary<decimal, List<int>>();
            Dictionary<int, List<double>> _distinctList = new Dictionary<int, List<double>>(); // selected X + Y + M dimensions
            Dictionary<int, Dictionary<decimal, double>> _distinctDimensionChecksum2Measure = new Dictionary<int, Dictionary<decimal, double>>();            
            Dictionary<decimal, int> _distinctDimensionChecksumList = new Dictionary<decimal, int>();
            decimal selectRecord = 0;
            responseDict[requestID].sourcedRecordCount = Convert.ToDecimal(ramDetailgz[0].Count - 1);


            var fromAddress = ramDetailSegment[currentSegment];
            var toAddress = ramDetailSegment[currentSegment + 1];

            // distinct initization
            decimal distinctDimensionChecksum;            

            for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                _distinctDimensionChecksum2Measure.Add(i, new Dictionary<decimal, double>());

            int unique = 0;
            
            for (int i = 0; i < (dimension.Count + measure.Count); i++)
                _distinctList.Add(i, new List<double>());
             
            for (int i = 0; i < dimension.Count; i++)
                _distinctList[i].Add(ramDetailgz[dimension[i]][0]); // add dimension column

            for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                _distinctList[i].Add(ramDetailgz[measure[i - dimension.Count]][0]); // add measure column 
                               

            _distinctListChecksum.Add(0); // store a list of distinctDimensionChecksum

            List<Double> elementCount = new List<double>();
            for (int i = dimension.Count; i > 0; i--)
                elementCount.Add(ramKey2Valuegz[dimension[i - 1]].Count); // master item for each dimension column

            List<Double> logElementCount = new List<double>();

            for (int i = dimension.Count - 1; i > 0; i--)
            {
                if (i == dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[dimension[i]].Count, 10) + 0.5000001), 0));
                if (i < dimension.Count - 1) logElementCount.Add(Math.Round((Math.Log(ramKey2Valuegz[dimension[i]].Count, 10) + 0.5000001), 0) + logElementCount[dimension.Count - 2 - i]);
            }

            List<Double> factor = new List<double>();
            for (int i = 0; i < (dimension.Count - 1); i++)
                factor.Add(Math.Pow(10, logElementCount[i]));


            // filter initization
            bool isSelectString = true;
            bool isSelectEitherNum = true;
            bool isSelectNum = true;
            int filterColumn = 0;
            int countSameColumn;
            int findcountColNo;
            int countAtMeasureColNo;           
            int countColNo;

            Dictionary<int, List<bool>> isSelectNumList = new Dictionary<int, List<bool>>(); // to obtain an unique list of column ID when filter column is number

            responseDict[requestID].removeCrosstabMeasure = false;

            for (int i = 0; i < measurementColumn[0].Count; i++)
            {
                if (isSelectNumList.ContainsKey(measurementColumn[0][i]) == false)
                    isSelectNumList.Add(measurementColumn[0][i], new List<bool>());
            }

            Dictionary<int, int> filterByDimensionCriteria = new Dictionary<int, int>(); // to obtain an unique list of column ID when filter column is text

            if (measurementColumn[0].Count == 0) // if not having number filter
            {
                for (int i = 0; i < dimensionCriteria.Count; i++) // loop each text fitler
                {
                    if (dimensionCriteria[i].Count > 0)  // do not record column ID if null selected text
                    {
                        filterByDimensionCriteria.Add(filterColumn, i); // filterColumn is order number of column ID                        
                        filterColumn++;
                    }
                }
               
                if (requestDict[requestID].measureType == "sum")
                {
                   // Console.WriteLine("currentSegment " + currentSegment + " fromAddress " + fromAddress + " toAddress " + toAddress);
                    for (int line = fromAddress; line < toAddress; line++)                    
                    {
                        isSelectCurrentLineOfString(line);
                        if (isSelectString == true)                      
                           distinctSum(line);  
                    }                                  
                }               

                if (requestDict[requestID].measureType == "max")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);
                        if (isSelectString == true)
                            distinctMax(line);
                    }
                }

                if (requestDict[requestID].measureType == "min")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);
                        if (isSelectString == true)
                            distinctMin(line);
                    }                
                }

                
                if (requestDict[requestID].measureType == "average")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);
                        if (isSelectString == true)
                            distinctSum(line);
                    }
                    
                    findcountColNo = 0;
                    do
                    {
                        findcountColNo++;
                    } while (ramKey2Valuegz[findcountColNo][0].ToString() != "Fact" || findcountColNo < ramKey2Valuegz.Count - 1);

                    countColNo = _distinctList.Count - 1 + findcountColNo + 1 - ramKey2Valuegz.Count;
                    countAtMeasureColNo = measure.Count - 1 + findcountColNo + 1 - ramKey2Valuegz.Count;

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        for (int j = 1; j < _distinctList[i].Count; j++)
                        { 
                            if(i != countColNo)
                                _distinctList[i][j] = Math.Round((Double)(_distinctList[i][j] / (_distinctList[countColNo][j])),2);
                        }   
                }
                
            }

            if (measurementColumn[0].Count == 1) // if only one numeric filter
            {                
                for (int i = 0; i < dimensionCriteria.Count; i++)
                {
                    if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                    {
                        filterByDimensionCriteria.Add(filterColumn, i);
                        filterColumn++;
                    }
                }

                if (requestDict[requestID].measureType == "sum")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line); 
                    }
                }

                if (requestDict[requestID].measureType == "max")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctMax(line);
                    }                
                }

                if (requestDict[requestID].measureType == "min")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctMin(line);
                    }                  
                }
               
                if (requestDict[requestID].measureType == "average")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line);
                    }

                    findcountColNo = 0;
                    do
                    {
                        findcountColNo++;
                    } while (ramKey2Valuegz[findcountColNo][0].ToString() != "Fact" || findcountColNo < ramKey2Valuegz.Count - 1);

                    countColNo = _distinctList.Count - 1 + findcountColNo + 1 - ramKey2Valuegz.Count;

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        for (int j = 1; j < _distinctList[i].Count; j++)
                        {
                            if (i != countColNo)
                                _distinctList[i][j] = Math.Round((Double)(_distinctList[i][j] / (_distinctList[countColNo][j])), 2);
                        }
                }                
            }


            if (measurementColumn[0].Count > 1 && noMultipleMeasurementColumn == true) // if more than one numeric filter but no duplication of column ID
            {
                for (int i = 0; i < dimensionCriteria.Count; i++)
                {
                    if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                    {
                        filterByDimensionCriteria.Add(filterColumn, i);
                        filterColumn++;
                    }
                }

                if (requestDict[requestID].measureType == "sum")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line); // check wehter numeric filter can match with current line number => return true or false

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line);  // if true, current line will be used to distinct
                    }
                }

                if (requestDict[requestID].measureType == "max")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line); // check wehter numeric filter can match with current line number => return true or false

                        if (isSelectNum && isSelectString == true)
                            distinctMax(line);  // if true, current line will be used to distinct
                    }                  
                }

                if (requestDict[requestID].measureType == "min")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line); // check wehter numeric filter can match with current line number => return true or false

                        if (isSelectNum && isSelectString == true)
                            distinctMin(line);  // if true, current line will be used to distinct
                    }                 
                }
               
                if (requestDict[requestID].measureType == "average")
                {
                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithNoColumnDuplication(line); // check wehter numeric filter can match with current line number => return true or false

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line);  // if true, current line will be used to distinct
                    }

                    findcountColNo = 0;
                    do
                    {
                        findcountColNo++;
                    } while (ramKey2Valuegz[findcountColNo][0].ToString() != "Fact" || findcountColNo < ramKey2Valuegz.Count - 1);

                    countColNo = _distinctList.Count - 1 + findcountColNo + 1 - ramKey2Valuegz.Count;

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        for (int j = 1; j < _distinctList[i].Count; j++)
                        {
                            if (i != countColNo)
                                _distinctList[i][j] = Math.Round((Double)(_distinctList[i][j] / (_distinctList[countColNo][j])), 2);
                        }
                }               
            }

            if (measurementColumn[0].Count > 1 && noMultipleMeasurementColumn == false) // if more than one numeric filter and have duplication of column ID
            {   
                if (requestDict[requestID].measureType == "sum")
                {
                    for (int i = 0; i < dimensionCriteria.Count; i++)
                    {
                        if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                        {  
                            filterByDimensionCriteria.Add(filterColumn, i);
                            filterColumn++;
                        }
                    }

                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)                       
                            filterNumberWithColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line);                        
                    }
                }

                if (requestDict[requestID].measureType == "max")
                {
                    for (int i = 0; i < dimensionCriteria.Count; i++)
                    {
                        if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                        {
                            filterByDimensionCriteria.Add(filterColumn, i);
                            filterColumn++;
                        }
                    }

                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctMax(line);
                    }
                }

                if (requestDict[requestID].measureType == "min")
                {
                    for (int i = 0; i < dimensionCriteria.Count; i++)
                    {
                        if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                        {
                            filterByDimensionCriteria.Add(filterColumn, i);
                            filterColumn++;
                        }
                    }

                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctMin(line);
                    }
                }
             
                if (requestDict[requestID].measureType == "average")
                {
                    for (int i = 0; i < dimensionCriteria.Count; i++)
                    {
                        if (dimensionCriteria[i].Count > 0)  // skip dimension with null criteria
                        {
                            filterByDimensionCriteria.Add(filterColumn, i);
                            filterColumn++;
                        }
                    }

                    for (int line = fromAddress; line < toAddress; line++)
                    {
                        isSelectCurrentLineOfString(line);

                        if (isSelectString == true)
                            filterNumberWithColumnDuplication(line);

                        if (isSelectNum && isSelectString == true)
                            distinctSum(line);
                    }
                   
                    findcountColNo = 0;
                    do
                    {
                        findcountColNo++;
                    } while (ramKey2Valuegz[findcountColNo][0].ToString() != "Fact" || findcountColNo < ramKey2Valuegz.Count - 1);

                    countColNo = _distinctList.Count - 1 + findcountColNo + 1 - ramKey2Valuegz.Count;

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        for (int j = 1; j < _distinctList[i].Count; j++)
                        {
                            if (i != countColNo)
                                _distinctList[i][j] = Math.Round((Double)(_distinctList[i][j] / (_distinctList[countColNo][j])), 2);
                        }
                }
                
            }

            if (crosstabDimension.Count > 0) // if x dimension is selected, create mapping with source(Checksum) and Target Amount(0), Amount(1), Amount(n)
            {
                for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                {
                    _distinctDimensionChecksum2Measure[i].Clear();
                    for (int j = 0; j < _distinctList[i].Count; j++)
                        _distinctDimensionChecksum2Measure[i].Add(_distinctListChecksum[j], _distinctList[i][j]);
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append(Environment.NewLine);
                debug.Append("dimension.Count: " + dimension.Count);
                debug.Append(Environment.NewLine);
                debug.Append("distinctDimensionChecksum2Measure.Count: " + _distinctDimensionChecksum2Measure.Count);
                debug.Append(Environment.NewLine);

                for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                {
                    debug.Append("distinctDimensionChecksum2Measure[" + i + "].Count " + _distinctDimensionChecksum2Measure[i].Count);
                    debug.Append(Environment.NewLine);
                }

                debug.Append(Environment.NewLine);
            }
            responseDict[requestID].selectedRecordCount = responseDict[requestID].selectedRecordCount + selectRecord;
            checkSegmentThreadCompleted.Enqueue(currentSegment);            

            _distinctListChecksum.RemoveAt(0);

            
            for (int i = 0; i < dimension.Count; i++)
                _distinctList[i].RemoveAt(0);

            for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                _distinctList[i].RemoveAt(0);     

           // Console.WriteLine("after " + currentSegment + " " + _distinctList.Count + " " + _distinctList[2][10] + " " + _distinctList[0].Count + " " + _distinctList2DrillDown.Count + " " + _distinctListChecksum.Count + " " + _distinctDimensionChecksumList.Count);           

            return (_distinctList, _distinctList2DrillDown, _distinctListChecksum, _distinctDimensionChecksumList);

            void filterNumberWithNoColumnDuplication(int line)
            {
                isSelectNum = true;

                for (int i = 0; i < measurementColumn[0].Count; i++) // filter range match condition - no duplication of column ID
                {
                    if (measurementOperator[0][i].ToString() == "=" && measurementOperator[1][i].ToString() == "=")
                        isSelectNum = isSelectNum && ramDetailgz[measurementColumn[0][i]][line] == measurementRange[0][i] || ramDetailgz[measurementColumn[0][i]][line] == measurementRange[1][i];

                    if (measurementOperator[0][i].ToString() == ">=" && measurementOperator[1][i].ToString() == "<=")
                        isSelectNum = isSelectNum && ramDetailgz[measurementColumn[0][i]][line] >= measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] <= measurementRange[1][i];

                    if (measurementOperator[0][i].ToString() == ">" && measurementOperator[1][i].ToString() == "<")
                        isSelectNum = isSelectNum && ramDetailgz[measurementColumn[0][i]][line] > measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] < measurementRange[1][i];

                    if (measurementOperator[0][i].ToString() == ">=" && measurementOperator[1][i].ToString() == "<")
                        isSelectNum = isSelectNum && ramDetailgz[measurementColumn[0][i]][line] >= measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] < measurementRange[1][i];

                    if (measurementOperator[0][i].ToString() == ">" && measurementOperator[1][i].ToString() == "=<")
                        isSelectNum = isSelectNum && ramDetailgz[measurementColumn[0][i]][line] > measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] <= measurementRange[1][i];
                }
            }

            void filterNumberWithColumnDuplication(int line)
            {
                for (int i = 0; i < measurementColumn[0].Count; i++) // filter range match condition
                {
                    if (measurementOperator[0][i].ToString() == "=" && measurementOperator[1][i].ToString() == "=")
                        isSelectNumList[measurementColumn[0][i]].Add(ramDetailgz[measurementColumn[0][i]][line] == measurementRange[0][i] || ramDetailgz[measurementColumn[0][i]][line] == measurementRange[1][i]);

                    if (measurementOperator[0][i].ToString() == ">=" && measurementOperator[1][i].ToString() == "<=")
                        isSelectNumList[measurementColumn[0][i]].Add(ramDetailgz[measurementColumn[0][i]][line] >= measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] <= measurementRange[1][i]);

                    if (measurementOperator[0][i].ToString() == ">" && measurementOperator[1][i].ToString() == "<")
                        isSelectNumList[measurementColumn[0][i]].Add(ramDetailgz[measurementColumn[0][i]][line] > measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] < measurementRange[1][i]);

                    if (measurementOperator[0][i].ToString() == ">=" && measurementOperator[1][i].ToString() == "<")
                        isSelectNumList[measurementColumn[0][i]].Add(ramDetailgz[measurementColumn[0][i]][line] >= measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] < measurementRange[1][i]);

                    if (measurementOperator[0][i].ToString() == ">" && measurementOperator[1][i].ToString() == "=<")
                        isSelectNumList[measurementColumn[0][i]].Add(ramDetailgz[measurementColumn[0][i]][line] > measurementRange[0][i] && ramDetailgz[measurementColumn[0][i]][line] <= measurementRange[1][i]);
                }

                countSameColumn = 0;
                isSelectNum = true;

                foreach (KeyValuePair<int, List<bool>> element in isSelectNumList)
                {
                    isSelectEitherNum = false;
                    for (int i = 0; i < isSelectNumList[element.Key].Count; i++)
                    {
                        if (isSelectNumList[element.Key].Count == 1) isSelectEitherNum = isSelectNumList[element.Key][i];
                        if (isSelectNumList[element.Key].Count > 1) isSelectEitherNum = isSelectEitherNum || isSelectNumList[element.Key][i];
                    }
                    isSelectNum = isSelectNum && isSelectEitherNum;
                    isSelectNumList[element.Key].Clear();
                    countSameColumn++;
                }
            }

            void isSelectCurrentLineOfString(int line)
            {
                distinctDimensionChecksum = 0;
                isSelectString = true;

                for (int i = 0; i < filterColumn; i++) // filter exact match condition
                    isSelectString = isSelectString && dimensionCriteria[filterByDimensionCriteria[i]].ContainsKey(ramDetailgz[filterByDimensionCriteria[i]][line]);                
            }
          
            void distinctSum(int line)
            {
                selectRecord++;
                for (int i = 0; i < dimension.Count; i++) // convert multiple dimension value to an unique number 
                {
                    if (i < dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line] * factor[dimension.Count - 2 - i]);
                    if (i == dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line]);
                }

                if (!_distinctDimensionChecksumList.ContainsKey(distinctDimensionChecksum))
                {
                    unique++;
                    _distinctDimensionChecksumList.Add(distinctDimensionChecksum, unique); // building a list of distinctDimensionChecksum

                    for (int i = 0; i < dimension.Count; i++)
                        _distinctList[i].Add(ramDetailgz[dimension[i]][line]); // add dimension value for first unique item                

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        _distinctList[i].Add(ramDetailgz[measure[i - dimension.Count]][line]); // add measure number by first unique dimension value                    

                    _distinctListChecksum.Add(distinctDimensionChecksum); // add calculated multidimension checksum to the list - distinctListChecksum                                                                          

                    if (requestDict[requestID].processButton == "drillRow")
                    {
                        _distinctList2DrillDown.Add(distinctDimensionChecksum, new List<int>());  
                        _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                    }
                }
                else // addition = current value + last value of the same key
                {
                    var add = _distinctDimensionChecksumList[distinctDimensionChecksum]; // return unique line number    

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        _distinctList[i][add] = Math.Round((Double)(_distinctList[i][add] + ramDetailgz[measure[i - dimension.Count]][line]), 2); // sum measure column by dimension column                               

                    if (requestDict[requestID].processButton == "drillRow")
                        _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                }
            }            

            void distinctMax(int line)
            {
                selectRecord++;
                for (int i = 0; i < dimension.Count; i++) // convert multiple dimension value to an unique number 
                {
                    if (i < dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line] * factor[dimension.Count - 2 - i]);
                    if (i == dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line]);
                }

                if (!_distinctDimensionChecksumList.ContainsKey(distinctDimensionChecksum))
                {
                    unique++;
                    _distinctDimensionChecksumList.Add(distinctDimensionChecksum, unique); // building a list of distinctDimensionChecksum

                    for (int i = 0; i < dimension.Count; i++)
                        _distinctList[i].Add(ramDetailgz[dimension[i]][line]); // add dimension value for first unique item

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        _distinctList[i].Add(ramDetailgz[measure[i - dimension.Count]][line]); // add measure number by first unique dimension value

                    _distinctListChecksum.Add(distinctDimensionChecksum); // add calculated multidimension checksum to the list - distinctListChecksum

                    if (requestDict[requestID].processButton == "drillRow")
                    {
                        _distinctList2DrillDown.Add(distinctDimensionChecksum, new List<int>());
                        _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                    }
                }

                else // addition = current value + last value of the same key
                {
                    var add = _distinctDimensionChecksumList[distinctDimensionChecksum]; // return unique line number     
                  
                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        if (ramDetailgz[measure[i - dimension.Count]][line] > _distinctList[i][add])
                        { 
                            _distinctList[i][add] = ramDetailgz[measure[i - dimension.Count]][line];

                            if (requestDict[requestID].processButton == "drillRow")
                            {
                                if (_distinctList2DrillDown[distinctDimensionChecksum].Count <= (i - dimension.Count))
                                    _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                                else
                                    _distinctList2DrillDown[distinctDimensionChecksum][i - dimension.Count] = line;                                
                            }                           
                        }
                }
            }

            void distinctMin(int line)
            {
                selectRecord++;
                for (int i = 0; i < dimension.Count; i++) // convert multiple dimension value to an unique number 
                {
                    if (i < dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line] * factor[dimension.Count - 2 - i]);
                    if (i == dimension.Count - 1) distinctDimensionChecksum = distinctDimensionChecksum + Convert.ToDecimal(ramDetailgz[dimension[i]][line]);
                }

                if (!_distinctDimensionChecksumList.ContainsKey(distinctDimensionChecksum))
                {
                    unique++;
                    _distinctDimensionChecksumList.Add(distinctDimensionChecksum, unique); // building a list of distinctDimensionChecksum

                    for (int i = 0; i < dimension.Count; i++)
                        _distinctList[i].Add(ramDetailgz[dimension[i]][line]); // add dimension value for first unique item

                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        _distinctList[i].Add(ramDetailgz[measure[i - dimension.Count]][line]); // add measure number by first unique dimension value

                    _distinctListChecksum.Add(distinctDimensionChecksum); // add calculated multidimension checksum to the list - distinctListChecksum

                    if (requestDict[requestID].processButton == "drillRow")
                    {
                        _distinctList2DrillDown.Add(distinctDimensionChecksum, new List<int>());
                        _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                    }
                }
                else // addition = current value + last value of the same key
                {
                    var add = _distinctDimensionChecksumList[distinctDimensionChecksum]; // return unique line number                       
                   
                    for (int i = dimension.Count; i < (dimension.Count + measure.Count); i++)
                        if (ramDetailgz[measure[i - dimension.Count]][line] < _distinctList[i][add])
                        { 
                            _distinctList[i][add] = ramDetailgz[measure[i - dimension.Count]][line];

                            if (requestDict[requestID].processButton == "drillRow")
                            {
                                if (_distinctList2DrillDown[distinctDimensionChecksum].Count <= (i - dimension.Count))
                                    _distinctList2DrillDown[distinctDimensionChecksum].Add(line);
                                else
                                    _distinctList2DrillDown[distinctDimensionChecksum][i - dimension.Count] = line;
                            }
                        }
                }
            }           
        }
        
    }
}