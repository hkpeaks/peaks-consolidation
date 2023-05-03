using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace youFast
{
    public class Crosstab
    {       
        public void crosstabByCopyZero(Dictionary<int, List<double>> YXMdistinctDrillKey, Dictionary<decimal, int> distinctDimensionChecksumList, Dictionary<double, decimal> crosstabAddress2DrillSetDict, char csvWriteSeparator, string outputFolder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, List<int> measure,  Dictionary<int, List<double>> distinctList, Dictionary<int, List<double>> XdistinctList, Dictionary<int, List<double>> YdistinctList, Dictionary<int, string> sortedRevisedXY, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctYramKey2Value, List<int> sortedXdimension, List<int> sortedYdimension, Dictionary<int, List<double>> YXMdistinctList, StringBuilder debug, int xAddressCol, int yAddressCol)
        {
            Dictionary<int, List<double>> XofSummarisedList = new Dictionary<int, List<double>>(); // X dimensions of SummarisedFullList
            Dictionary<int, List<double>> YofSummarisedList = new Dictionary<int, List<double>>(); // Y dimensions of SummarisedFullList           
            List<decimal> XdimensionChecksumList = new List<decimal>(); // ChecksumList for X dimensions
            List<decimal> YdimensionChecksumList = new List<decimal>(); // ChecksumList for Y dimensions
            Dictionary<decimal, double> XdimensionChecksum2RecordNo = new Dictionary<decimal, double>();  // ChecksumList for X dimensions --> RecordNo mapping
            Dictionary<decimal, double> YdimensionChecksum2RecordNo = new Dictionary<decimal, double>();  // ChecksumList for Y dimensions --> RecordNo mapping           
            List<decimal> XofSummarisedListChecksumList = new List<decimal>();  // ChecksumList for X dimensions of SummarisedFullList
            List<decimal> YofSummarisedListChecksumList = new List<decimal>();  // ChecksumList for Y dimensions of SummarisedFullList             

            int x = 0; int y = 0;

            for (int i = 0; i < sortedRevisedXY.Count; i++)
            {
                if (sortedRevisedXY[i] == "X")
                {
                    XofSummarisedList.Add(x, new List<double>());
                    if (requestDict[requestID].debugOutput == "Y")
                    {
                        debug.Append("x " + x + " " + i);
                        debug.Append(Environment.NewLine);
                    }

                    for (int j = 0; j < distinctList[0].Count; j++)
                        XofSummarisedList[x].Add(distinctList[i][j]);

                    x++;
                }

                if (sortedRevisedXY[i] == "Y")
                {
                    YofSummarisedList.Add(y, new List<double>());
                    if (requestDict[requestID].debugOutput == "Y")
                    {
                        debug.Append("y " + y + " " + i);
                        debug.Append(Environment.NewLine);
                    }

                    for (int j = 0; j < distinctList[0].Count; j++)
                        YofSummarisedList[y].Add(distinctList[i][j]);
                    y++;
                }
            }

            Distinct getXY = new Distinct();
          
            XdimensionChecksumList = getXY.getXYcheckSumList(XdistinctList, distinctXramKey2Value, sortedXdimension);
            YdimensionChecksumList = getXY.getXYcheckSumList(YdistinctList, distinctYramKey2Value, sortedYdimension);
            XofSummarisedListChecksumList = getXY.getXYcheckSumList(XofSummarisedList, distinctXramKey2Value, sortedXdimension);
            YofSummarisedListChecksumList = getXY.getXYcheckSumList(YofSummarisedList, distinctYramKey2Value, sortedYdimension);          

            ExportCSV currentExport = new ExportCSV();

            if (requestDict[requestID].debugOutput == "Y")
            {
                currentExport.ramTable2CSV(XdistinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "XdistinctList" + ".csv");
                currentExport.ramTable2CSV(YdistinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "YdistinctList" + ".csv");
                currentExport.ramTable2CSV(XofSummarisedList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "XofSummarisedList" + ".csv");
                currentExport.ramTable2CSV(YofSummarisedList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "YofSummarisedList" + ".csv");
            }

            for (int i = 0; i < XdimensionChecksumList.Count; i++)
                XdimensionChecksum2RecordNo.Add(XdimensionChecksumList[i], i);

            for (int i = 0; i < YdimensionChecksumList.Count; i++)
                YdimensionChecksum2RecordNo.Add(YdimensionChecksumList[i], i);

            if (requestDict[requestID].debugOutput == "Y")
            {
                debug.Append("distinctList: " + distinctList.Count + " distinctList[0]: " + distinctList[0].Count);
                debug.Append(Environment.NewLine);
            }

            if (distinctList.ContainsKey(xAddressCol))
            {
                distinctList.Remove(xAddressCol);
                distinctList.Add(xAddressCol, new List<double>());
                distinctList[xAddressCol].Add(0);
            }

            if (!distinctList.ContainsKey(xAddressCol))
            {
                distinctList.Add(xAddressCol, new List<double>());
                distinctList[xAddressCol].Add(0);
            }

            if (distinctList.ContainsKey(yAddressCol))
            {
                distinctList.Remove(yAddressCol);
                distinctList.Add(yAddressCol, new List<double>());
                distinctList[yAddressCol].Add(0);
            }

            if (!distinctList.ContainsKey(yAddressCol))
            {
                distinctList.Add(yAddressCol, new List<double>());
                distinctList[yAddressCol].Add(0);
            }

            for (int i = 1; i < distinctList[0].Count; i++)
            {              
                distinctList[xAddressCol].Add(XdimensionChecksum2RecordNo[XofSummarisedListChecksumList[i]]);
                distinctList[yAddressCol].Add(YdimensionChecksum2RecordNo[YofSummarisedListChecksumList[i]]);
            }
           
            List<decimal> _distinctDimensionChecksumList = new List<decimal>();
            foreach (var pair in distinctDimensionChecksumList)           
                _distinctDimensionChecksumList.Add(pair.Key);
          
            for (int i = 1; i < distinctList[0].Count; i++)
            {
                var key = distinctList[xAddressCol][i] * 100000 + distinctList[yAddressCol][i];
                crosstabAddress2DrillSetDict.Add(key, _distinctDimensionChecksumList[i - 1]);              
            }

            if (requestDict[requestID].debugOutput == "Y")
                currentExport.ramTable2CSV(distinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "SummarisedListwithX&Yaddress" + ".csv");           

            // fill all cell of the final output by zero          
            
            int start0Col = YdistinctList.Count;
            int end0Col = YdistinctList.Count + ((XdistinctList[0].Count - 1) * measure.Count);
           
            for (int i = 0; i < YdistinctList.Count; i++)
            { 
                YXMdistinctList[i] = YdistinctList[i];
                YXMdistinctDrillKey[i] = YdistinctList[i];
            }            

            var startAdd0Time = DateTime.Now;

            List<double> source = new List<double>();
            for (int j = 0; j < YdistinctList[0].Count; j++)
                source.Add(0);

            for (int i = start0Col; i < end0Col; i++) // add zero for X and M column
            {
                YXMdistinctList.Add(i, new List<double>());
                YXMdistinctList[i].AddRange(source);
                YXMdistinctDrillKey.Add(i, new List<double>());
                YXMdistinctDrillKey[i].AddRange(source);
            }           

            if (requestDict[requestID].debugOutput == "Y")
            { 
                currentExport.ramTable2CSV(YXMdistinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "YXMdistinctListwithZeroValue" + ".csv");
                currentExport.ramTable2CSV(YXMdistinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "YXMdistinctDrillKeywithZeroValue" + ".csv");
            }

            int targetCol;
            int targetRow;

            for (int xValue = 1; xValue < XdistinctList[0].Count; xValue++) // fill number if target cell is not zero                   
            {
                for (int m = 0; m < measure.Count; m++) // read distinctList number and write to YXMdistinctList Table Header
                {
                    targetCol = start0Col + ((xValue - 1) * measure.Count) + m;
                    YXMdistinctList[targetCol][0] = xValue;
                    YXMdistinctDrillKey[targetCol][0] = xValue;
                }
            }
           

            for (int m = measure.Count; m >= 1; m--) // read distinctList number and write to YXMdistinctList Table Body
            {
                for (int YXMrow = 1; YXMrow < distinctList[0].Count; YXMrow++)
                {
                    targetCol = start0Col + (Convert.ToInt32(distinctList[xAddressCol][YXMrow]) * measure.Count) - m;
                    targetRow = Convert.ToInt32(distinctList[yAddressCol][YXMrow]);
                    YXMdistinctList[targetCol][targetRow] = distinctList[xAddressCol - m][YXMrow];
                    YXMdistinctDrillKey[targetCol][targetRow] = Convert.ToDouble(crosstabAddress2DrillSetDict[distinctList[xAddressCol][YXMrow] * 100000 + distinctList[yAddressCol][YXMrow]]);
                }
            }
            
            if (requestDict[requestID].debugOutput == "Y")
            {
                currentExport.ramTable2CSV(YXMdistinctList, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "fastYXMdistinctList" + ".csv");
                currentExport.ramTable2CSV(YXMdistinctDrillKey, csvWriteSeparator, outputFolder, "\\" + "Debug" + "\\" + "fastYXMdistinctDrillKey" + ".csv");
                debug.Append("XdimensionChecksumList.Count " + XdimensionChecksumList.Count); debug.Append(Environment.NewLine);
                debug.Append("YdimensionChecksumList.Count " + YdimensionChecksumList.Count); debug.Append(Environment.NewLine);
                debug.Append("");
            }
        }
    }
}
