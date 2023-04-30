using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace youFast
{
    public class ExportHTML
    {      
        public StringBuilder ramdistint2displayFilterDropDownList(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<p>");
            html.AppendLine("<select id=\"displayFilterDropDownList\" class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\" name=\"addRow\" id=\"AddRow\" onchange=\"addFilter(this)\">");
            html.AppendLine("<option value=\"Add Filter\">Add Filter</option>");

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");
                html.AppendLine("<option value = " + trimWhitespace + " > ");
                var temp = trimWhitespace + " </option>" + Environment.NewLine;
                html.AppendLine(temp.Replace("#", " "));
            }

            html.AppendLine("</select>");
            html.AppendLine("<p class=\"w3-text w3-col m1\"></p>");    
            html.AppendLine("<button id=\"exe2\" class=\"w3-button w3-round-xlarge w3-green\" onclick=\"moveColumn()\"><b>Move Column</b></button>");
            html.AppendLine("<button id=\"exe\" class=\"w3-button w3-round-xlarge w3-green\" onclick=\"drillRow()\"><b>Drill Row</b></button>");
            html.AppendLine("</control>");
            html.AppendLine("</p>");
            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "displayFilterDropDownList.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2SelectFile(Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, string sourceFolder, string db1Folder)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<p>");
            //
            if (requestDict[requestID2Queue].processID == "refreshSelectFileList")
            { 
               html.AppendLine("<select id=\"refreshSelectFileList\" class=\"w3-select w3-col m3 w3-display-topright w3-border w3-hover-border-Blue\" name=\"addFile\" onclick=\"clearCurrentReport()\" onchange=\"selectFile(this)\">");
               html.AppendLine("<option value=\"Select File\">" + requestDict[requestID2Queue].importFile.ToString() + "</option>");                
            }
                      

            if (requestDict[requestID2Queue].processID == "selectFile")
            { 
                html.AppendLine("<select id=\"addDataFile\" class=\"w3-select w3-col m3 w3-display-topright w3-border w3-hover-border-Blue\" name=\"addFile\"  onclick=\"clearCurrentReport()\" onchange=\"selectFile(this)\">");
                html.AppendLine("<option value=\"Select File\">Select File</option>");
            }           

            string[] importfile = Directory.GetFiles(sourceFolder.ToString(), "*.csv");

            foreach (string filePath in importfile)
            {
                var fileNameLength = filePath.Length - 1 - filePath.LastIndexOf((char)92);
                var fileName = filePath.Substring(filePath.LastIndexOf((char)92) + 1, fileNameLength);

                var trimWhitespace = fileName.Replace(" ", "#");
                html.AppendLine("<option style = \"color:red\" value = " + trimWhitespace + ">");
                html.AppendLine(fileName);
            }
          
            string[] distinctDB = Directory.GetFiles(db1Folder.ToString(), "ramDetail*.db", SearchOption.AllDirectories);

            foreach (string filePath in distinctDB)
            {         
                int lastOneMatch = filePath.Substring(0, filePath.LastIndexOf((char)92) + 1).LastIndexOf((char)92);
                int lastTwoMatch = filePath.Substring(0, filePath.LastIndexOf((char)92) - 1).LastIndexOf((char)92);
                var fileName = filePath.Substring(lastTwoMatch + 1, lastOneMatch - lastTwoMatch - 1);

                var trimWhitespace = fileName.Replace(" ", "#");
                if (remK2V.ContainsKey(fileName.ToString()))
                    html.AppendLine("<option style = \"color:blue\" value = " + trimWhitespace + " > ");
                else
                    html.AppendLine("<option style = \"color:green\" value = " + trimWhitespace + " > ");

                html.AppendLine(fileName);
            }
            
            html.AppendLine("</select>");
          

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                   Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "selectFile.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
           }
           return html;
        }
        public StringBuilder ramdistinct2AddFilter(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, int serialID, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {   
            List<string> sortedValue = new List<string>();
            Dictionary<string, string> columnName2ID = new Dictionary<string, string>();
            for (int i = 0; i < ramKey2Valuegz.Count; i++)
                columnName2ID.Add(ramKey2Valuegz[i][0], i.ToString());
            
            string trimWhitespace;
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            StringBuilder html = new StringBuilder();
            var addColumnName = requestDict[requestID2Queue].filterColumn.Replace("#", " ");
            string addColumnID = columnName2ID[addColumnName];
            bool success = Int32.TryParse(addColumnID, out int number);
            for (int j = 1; j < ramKey2Valuegz[number].Count; j++)
            { 
                sortedValue.Add(ramKey2Valuegz[number][j]);               
            }

            trimWhitespace = addColumnName;
            trimWhitespace = trimWhitespace.Replace(" ", "#");

            sortedValue.Sort();

            if (ramKey2Valuegz[number].Count > 1)
            {
                html.AppendLine("<selection-criteria id=\"addFilter" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column0\" id=\"column" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <option value=" + "\"" + trimWhitespace + "\"" + ">" + addColumnName + "</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column1\" id=\"start" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\">=\">>=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\">\">></option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column2\" id=\"start" + trimWhitespace + serialID.ToString() + "\">");
                html.Append("    <option value=></option>");

                for (int j = 0; j < sortedValue.Count; j++)
                {
                    html.Append("    <option value=");
                    html.Append("\"");
                    html.Append(sortedValue[j]);
                    html.AppendLine("\">");

                    if (addColumnName.ToUpper().Contains("DATE"))
                    {
                        isDateNumber = double.TryParse(sortedValue[j], out dateNumber);
                        if (isDateNumber == true)
                        {
                            if (dateNumber > 1000 && dateNumber < 401770)
                            {
                                dateValue = DateTime.FromOADate(dateNumber);
                                dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                html.AppendLine(dateFormat);
                            }
                        }
                        else
                            html.AppendLine(sortedValue[j]);
                    }
                    else
                        html.AppendLine(sortedValue[j]);

                    html.AppendLine("</option>");
                    html.AppendLine(Environment.NewLine);
                }

                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column3\" id=\"end" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\"<=\"><=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\"<\"><</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column4\" id=\"end" + trimWhitespace + serialID.ToString() + "\">");
                html.Append("    <option value=></option>");

                for (int j = 0; j < sortedValue.Count; j++)
                {
                    html.Append("   <option value=");
                    html.Append("\"");
                    html.Append(sortedValue[j]);
                    html.AppendLine("\">");

                    if (addColumnName.ToUpper().Contains("DATE"))
                    {
                        isDateNumber = double.TryParse(sortedValue[j], out dateNumber);
                        if (isDateNumber == true)
                        {
                            if (dateNumber > 1000 && dateNumber < 401770)
                            {
                                dateValue = DateTime.FromOADate(dateNumber);
                                dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                html.AppendLine(dateFormat);
                            }
                        }
                        else
                            html.AppendLine(sortedValue[j]);
                    }
                    else
                        html.AppendLine(sortedValue[j]);

                    html.AppendLine("</option>");
                    html.AppendLine(Environment.NewLine);
                }

                html.AppendLine("</select>");
                html.AppendLine("    <button class=\"w3-button w3-col m1 w3-large\" onclick=\"deleteSelectionCriteria(this)\" value=\"addFilter" + trimWhitespace + serialID.ToString() + "\" name=\"column5\" id=\"delete" + trimWhitespace + serialID.ToString() + "\"><i class=\"w3-margin-center material-icons w3-large\">clear</i></button>");
                html.AppendLine("    <br></br>");
                html.AppendLine("</selection-criteria>");
            }

            if (ramKey2Valuegz[number].Count == 1)
            {
                html.AppendLine("<selection-criteria id=\"addFilter" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column0\" id=\"column" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <option value=" + "\"" + trimWhitespace + "\"" + ">" + addColumnName + "</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column1\" id=\"start" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\">=\">>=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\">\">></option>");
                html.AppendLine("</select>");
                html.AppendLine("<input class=\"w3-input w3-col m2 w3-border w3-hover-border-red\" name=\"column2\" id=\"start" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("</input>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column3\" id=\"end" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\"<=\"><=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\"<\"><</option>");
                html.AppendLine("</select>");
                html.AppendLine("<input class=\"w3-input w3-col m2 w3-border w3-hover-border-red\" name=\"column4\" id=\"end" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("</input>");
                html.AppendLine("    <button class=\"w3-button w3-col m1 w3-large\" onclick=\"deleteSelectionCriteria(this)\" value=\"addFilter" + trimWhitespace + serialID.ToString() + "\" name=\"column5\" id=\"delete" + trimWhitespace + serialID.ToString() + "\"><i class=\"w3-margin-center material-icons w3-large\">clear</i></button>");
                html.AppendLine("    <br></br>");                
                html.AppendLine("</selection-criteria>");
            }

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addFilterHTML.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }       
        public StringBuilder ramdistinct2DisplaySelectedFilterValue(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, int serialID, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<string> displayRandomSelectedValue)
        {
            List<string> sortedValue = new List<string>();
            Dictionary<string, string> columnName2ID = new Dictionary<string, string>();
            for (int i = 0; i < ramKey2Valuegz.Count; i++)
                columnName2ID.Add(ramKey2Valuegz[i][0], i.ToString());
           
            string trimWhitespace;
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;
            int shortestDropDownList = 0;
            int findColumn = 0;
            int finalColumn = 0;
            do
            {
                if (ramKey2Valuegz[findColumn].Count > 1)
                {
                    if (ramKey2Valuegz[findColumn].Count > shortestDropDownList && ramKey2Valuegz[findColumn].Count < 101)
                    {
                        shortestDropDownList = ramKey2Valuegz[findColumn].Count;
                        finalColumn = findColumn;
                    }
                }
                findColumn++;
            } while (findColumn < ramKey2Valuegz.Count);

            StringBuilder html = new StringBuilder();
            var addColumnName = displayRandomSelectedValue[0].Replace("#", " ");
            string addColumnID = columnName2ID[addColumnName];
            bool success = Int32.TryParse(addColumnID, out int number);
            for (int j = 1; j < ramKey2Valuegz[number].Count; j++)
                sortedValue.Add(ramKey2Valuegz[number][j]);

            trimWhitespace = addColumnName;
            trimWhitespace = trimWhitespace.Replace(" ", "#");

            sortedValue.Sort();

            if (ramKey2Valuegz[finalColumn].Count > 1)
            {
                html.AppendLine("<selection-criteria id=\"addFilter" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column0\" id=\"column" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <option value=" + "\"" + trimWhitespace + "\"" + ">" + addColumnName + "</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column1\" id=\"start" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\">=\">>=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\">\">></option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column2\" id=\"start" + trimWhitespace + serialID.ToString() + "\">");
                html.Append("   <option value=" + displayRandomSelectedValue[1] + ">");

                var startColumn = displayRandomSelectedValue[1].Replace(";", ":");
                startColumn = startColumn.Replace("|", "/");

                if (displayRandomSelectedValue[0].ToString().ToUpper().Contains("DATE"))
                {
                    isDateNumber = double.TryParse(startColumn, out dateNumber);
                    if (isDateNumber == true)
                    {
                        if (dateNumber > 1000 && dateNumber < 401770)
                        {
                            dateValue = DateTime.FromOADate(dateNumber);
                            dateFormat = dateValue.ToString("dd.MMM.yyyy");
                            startColumn = dateFormat;
                        }
                    }
                }
               
                html.Append(startColumn);                
                html.AppendLine("</option>");
                html.AppendLine("");                
                for (int j = 0; j < sortedValue.Count; j++)
                {
                    html.Append("    <option value=");
                    html.Append("\"");
                    html.Append(sortedValue[j]);
                    html.AppendLine("\">");

                   
                    if (addColumnName.ToUpper().Contains("DATE"))
                    {                       
                        isDateNumber = double.TryParse(sortedValue[j], out dateNumber);
                        if (isDateNumber == true)
                        {
                            if (dateNumber > 1000 && dateNumber < 401770)
                            {
                                dateValue = DateTime.FromOADate(dateNumber);
                                dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                html.AppendLine(dateFormat);                               
                            }
                        }
                        else
                            html.AppendLine(sortedValue[j]);
                    }
                    else
                        html.AppendLine(sortedValue[j]);

                    html.AppendLine("</option>");
                    html.AppendLine(Environment.NewLine);
                }

                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column3\" id=\"end" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\"<=\"><=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\"<\"><</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column4\" id=\"end" + trimWhitespace + serialID.ToString() + "\">");
                html.Append("   <option value=" + displayRandomSelectedValue[2] + ">");    
                
                var endColumn = displayRandomSelectedValue[2].Replace(";", ":");
                endColumn = endColumn.Replace("|", "/");                

                if (displayRandomSelectedValue[0].ToString().ToUpper().Contains("DATE"))
                {
                    isDateNumber = double.TryParse(endColumn, out dateNumber);
                    if (isDateNumber == true)
                    {
                        if (dateNumber > 1000 && dateNumber < 401770)
                        {
                            dateValue = DateTime.FromOADate(dateNumber);
                            dateFormat = dateValue.ToString("dd.MMM.yyyy");
                            endColumn = dateFormat;
                        }
                    }
                }

                html.Append(endColumn);
                html.AppendLine("</option>");
                html.AppendLine("");

                for (int j = 0; j < sortedValue.Count; j++)
                {
                    html.Append("   <option value=");
                    html.Append("\"");
                    html.Append(sortedValue[j]);
                    html.AppendLine("\">");

                    if (addColumnName.ToUpper().Contains("DATE"))
                    {
                        isDateNumber = double.TryParse(sortedValue[j], out dateNumber);
                        if (isDateNumber == true)
                        {
                            if (dateNumber > 1000 && dateNumber < 401770)
                            {
                                dateValue = DateTime.FromOADate(dateNumber);
                                dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                html.AppendLine(dateFormat);
                            }
                        }
                        else
                            html.AppendLine(sortedValue[j]);
                    }
                    else
                        html.AppendLine(sortedValue[j]);

                    html.AppendLine("</option>");
                    html.AppendLine(Environment.NewLine);
                }

                html.AppendLine("</select>");
                html.AppendLine("    <button class=\"w3-button w3-col m1 w3-large\" onclick=\"deleteSelectionCriteria(this)\" value=\"addFilter" + trimWhitespace + serialID.ToString() + "\" name=\"column5\" id=\"delete" + trimWhitespace + serialID.ToString() + "\"><i class=\"w3-margin-center material-icons w3-large\">clear</i></button>");
                html.AppendLine("    <br></br>");
                html.AppendLine("</selection-criteria>");
            }

            if (ramKey2Valuegz[number].Count == 1)
            {
                html.AppendLine("<selection-criteria id=\"addFilter" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <select class=\"w3-select w3-col m2 w3-border w3-hover-border-red\" name=\"column0\" id=\"column" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("    <option value=" + "\"" + trimWhitespace + "\"" + ">" + addColumnName + "</option>");
                html.AppendLine("</select>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column1\" id=\"start" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\">=\">>=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\">\">></option>");
                html.AppendLine("</select>");
                html.AppendLine("<input class=\"w3-input w3-col m1 w3-border w3-hover-border-red\" name=\"column2\" id=\"start" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("</input>");
                html.AppendLine("<select class=\"w3-select w3-col m1 w3-border w3-hover-border-red\" name=\"column3\" id=\"end" + trimWhitespace + serialID.ToString() + "Option\">");
                html.AppendLine("    <option value=\"<=\"><=</option>");
                html.AppendLine("    <option value=\"=\">=</option>");
                html.AppendLine("    <option value=\"<\"><</option>");
                html.AppendLine("</select>");
                html.AppendLine("<input class=\"w3-input w3-col m1 w3-border w3-hover-border-red\" name=\"column4\" id=\"end" + trimWhitespace + serialID.ToString() + "\">");
                html.AppendLine("</input>");
                html.AppendLine("    <button class=\"w3-button w3-col m1 w3-large\" onclick=\"deleteSelectionCriteria(this)\" value=\"addFilter" + trimWhitespace + serialID.ToString() + "\" name=\"column5\" id=\"delete" + trimWhitespace + serialID.ToString() + "\"><i class=\"w3-margin-center material-icons w3-large\">clear</i></button>");
                html.AppendLine("    <br></br>");
                html.AppendLine("</selection-criteria>");
            }

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addFilterHTML.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2Crosstab(ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<select id=\"addCrosstab\" class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\">");           
            html.AppendLine("<option value=\"Add Crosstab Dimension\">Reorganise in X Direction</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count > 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");
                    html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"crosstabDimension\" value=" + trimWhitespace + " id=\"crosstab" + i.ToString() + "\"" + "\">");
                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;                 
                    html.AppendLine(temp.Replace("#", " ") + "(" + (tableFact[requestDict[requestID2Queue].importFile][10 + i] - 1) + ")");
                }
            }
            html.AppendLine("</form>");
            html.AppendLine("<br></br>");
            html.AppendLine("</select>");
            html.AppendLine("</selection-criteria>");

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addCrosstab.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2CrosstabUpdate(ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, string outputFolder, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<select id=\"addCrosstab\" class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\">");
            html.AppendLine("<option value=\"Add Crosstab Dimension\">Reorganise in X Direction</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");
           
            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count > 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");

                    if (requestDict[requestID].crosstabOrder != null)
                    { 
                        if (requestDict[requestID].crosstabOrder.Contains(ramKey2Valuegz[i][0]))
                            html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"crosstabDimension\" value=" + trimWhitespace + " id=\"crosstab" + i.ToString() + "\"" + " checked=\"checked\"" + "\">");
                        else
                            html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"crosstabDimension\" value=" + trimWhitespace + " id=\"crosstab" + i.ToString() + "\"" + "\">");
                    }
                    
                    if (requestDict[requestID].crosstabOrder == null)                        
                            html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"crosstabDimension\" value=" + trimWhitespace + " id=\"crosstab" + i.ToString() + "\"" + "\">");

                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;                   
                    html.AppendLine(temp.Replace("#", " ") + "(" + (tableFact[requestDict[requestID].importFile][10 + i] - 1) + ")");
                }
            }
            html.AppendLine("</form>");
            html.AppendLine("<br></br>");
            html.AppendLine("</select>");
            html.AppendLine("</selection-criteria>");

            if (requestDict[requestID].debugOutput == "N")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addCrosstab.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2Measurement(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {   
            StringBuilder html = new StringBuilder();
            html.AppendLine("<select id=\"addMeasurement\" <select class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\" name=\"changeType\" id=\"measureType\" onchange=\"changeMeasureType(this)\">");
            html.AppendLine("<option value=\"sum\">Sum On Measurement</option>");
            html.AppendLine("<option value=\"max\">Max On Measurement</option>");
            html.AppendLine("<option value=\"min\">Min On Measurement</option>");
            html.AppendLine("<option value=\"average\">Average On Measurement</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count == 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");                

                    if (ramKey2Valuegz[i][0].ToString() == "Fact")
                        html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"measurement\" value=" + trimWhitespace + " id=\"measurement" + i.ToString() + "\"" + " checked=\"checked\"" + "\">");
                    else
                        html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"measurement\" value=" + trimWhitespace + " id=\"measurement" + i.ToString() + "\"");

                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;
                    html.AppendLine(temp.Replace("#", " "));
                }
            }
            html.AppendLine("</form>");
            html.AppendLine("<br></br>");
            html.AppendLine("</select>");

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addMeasurement.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2AllMeasurement(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<select id=\"addMeasurement\" <select class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\" name=\"changeType\" id=\"measureType\" onchange=\"changeMeasureType(this)\">");
            html.AppendLine("<option value=\"sum\">Sum On Measurement</option>");
            html.AppendLine("<option value=\"max\">Max On Measurement</option>");
            html.AppendLine("<option value=\"min\">Min On Measurement</option>");
            html.AppendLine("<option value=\"average\">Average On Measurement</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");
           
            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count == 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");
                    html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"measurement\" value=" + trimWhitespace + " id=\"measurement" + i.ToString() + "\"" + " checked=\"checked\"");                   
                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;
                    html.AppendLine(temp.Replace("#", " "));
                }
            }
            
            html.AppendLine("</form>");
            html.AppendLine("</select>");

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addMeasurement.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }     
        public StringBuilder ramdistint2DisplaySelectedColumn(ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, List<string> selectDistinctCol)
        {
            StringBuilder html = new StringBuilder();                 
            html.AppendLine("<select id=\"addDisplayColumn\" <select class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\" name=\"addCrosstab\" id=\"addSelection\" onchange=\"addDisplayColumn(this)\">");            
            html.AppendLine("<option value=\"Add Distinct Dimension\">Find Unique Set</option>");
            html.AppendLine("<option value=\"Add Crosstab Dimension\">Reorganise in X Direction</option>");
            html.AppendLine("<option value=\"Add All Distinct Dimension\">Add All Distinct Dimension</option>");
            html.AppendLine("<option value=\"Add All Measurement\">Add All Measurement</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count > 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");

                    if (selectDistinctCol.Contains(trimWhitespace))
                        html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"distinctDimension\" ondblclick=\"drillRowBySingleDimension(this)\" value=" + trimWhitespace + " id=\"distinct" + i.ToString() + "\"" + " checked=\"checked\">");
                   
                    else
                        html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"distinctDimension\" ondblclick=\"drillRowBySingleDimension(this)\" value=" + trimWhitespace + " id=\"distinct" + i.ToString() + "\"" + ">");
                   
                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;
                    html.AppendLine(temp.Replace("#", " ") + "(" + (tableFact[requestDict[requestID2Queue].importFile][10 + i] - 1) + ")");
                }
            }

            html.AppendLine("</form>");
            html.AppendLine("<br></br>");
            html.AppendLine("</select>");

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addDisplayColumn.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }
        public StringBuilder ramdistint2DisplayAllColumn(string outputFolder, decimal requestID2Queue, ConcurrentDictionary<decimal, clientMachine.request> requestDict, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            html.AppendLine("<select id=\"addDisplayColumn\" <select class=\"w3-select w3-col m2 w3-border w3-hover-border-Blue\" name=\"addCrosstab\" id=\"addSelection\" onchange=\"addDisplayColumn(this)\">");
            html.AppendLine("<option value=\"Add Distinct Dimension\">Find Unique Set</option>");
            html.AppendLine("<option value=\"Add Crosstab Dimension\">Reorganise in X Direction</option>");
            html.AppendLine("<option value=\"Add All Distinct Dimension\">Add All Distinct Dimension</option>");            
            html.AppendLine("<option value=\"Add All Measurement\">Add All Measurement</option>");
            html.AppendLine("<form class=\"w3-container w2-card-4\">");

            for (int i = 0; i < ramKey2Valuegz.Count; i++)
            {
                if (ramKey2Valuegz[i].Count > 1)
                {
                    var trimWhitespace = ramKey2Valuegz[i][0].Replace(" ", "#");
                    html.AppendLine("<input class=\"w3-check\" type=\"checkbox\" name=\"distinctDimension\" value=" + trimWhitespace + " id=\"distinct" + i.ToString() + "\"" + " checked=\"checked\">");                    
                    var temp = "<label>" + trimWhitespace + "</label>" + Environment.NewLine;
                    html.AppendLine(temp.Replace("#", " "));
                }
            }

            html.AppendLine("</form>");
            html.AppendLine("<br></br>");
            html.AppendLine("</select>");

            if (requestDict[requestID2Queue].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + "addDisplayColumn.txt"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                }
            }
            return html;
        }              
        public void ramDistinct2HtmlYMtable(ConcurrentDictionary<string, clientMachine.userPreference> userPreference, ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, Dictionary<string, Dictionary<int, Dictionary<double, string>>> remK2V, string sourceFolder, string db1Folder, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, int Ypage, double YtotalPage, int YpageStart, int YpageEnd, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict,  Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, string outputFolder, Dictionary<int, string> dataSortingOrder)
        {
            StringBuilder html = new StringBuilder();
            decimal nextPageID;
            responseDict[requestID].updateCrosstab = false;            

            if (requestDict[requestID].processID == "pageMove" || requestDict[requestID].processID == "displayPrecision")
                nextPageID = requestDict[2].nextPageID;
            else
                nextPageID = responseDict[requestID].requestID;

            html.AppendLine("<output-report id=" + nextPageID + " name=\"response\"" + "  #fast=" + requestDict[requestID].importFile + ">");
            html.AppendLine("<div class=\"w3-container\" id=submitForm value=\"0\">");          
            html.AppendLine("<p></p>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type =\"button\"> Request: " + nextPageID + "</button>");

            if (requestDict.ContainsKey(2) == true)
            {
                if (requestDict[2].sortingOrder == "sortDescending")
                {
                    dataSortingOrder[0] = "sortDescending";
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }

                else if (requestDict[2].sortingOrder == "sortAscending")
                {
                    dataSortingOrder[0] = "sortAscending";
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }
                else
                {
                    if (dataSortingOrder[0] == "sortDescending")
                        html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");

                    if (dataSortingOrder[0] == "sortAscending")
                        html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }
            }

            else
            {
                if (requestDict[requestID].sortingOrder == "sortAscending" || dataSortingOrder[0] == "sortAscending")
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");

                if (requestDict[requestID].sortingOrder == "sortDescending" || dataSortingOrder[0] == "sortDescending")
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
            }
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"openReportByWindows(this)\" value=" + nextPageID + " id=\"openReportByWindows\"><i class=\"material-icons\">open_with</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageUp(this)\" value=" + nextPageID + " id=\"up\"><i class=\"material-icons\">arrow_upward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick =\"pageEndDown(this)\" value=" + nextPageID + " id=\"pageEndDown\" type=\"button\" style=\"font-weight:bold\"> Y" + Ypage + "/" + YtotalPage + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageDown(this)\" value=" + nextPageID + " id=\"down\"><i class=\"material-icons\">arrow_downward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageHome(this)\" value=" + nextPageID + " id=\"pageHome\"><i class=\"material-icons\">home</i></button></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"nextPrecision(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">visibility</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"filterCount\" type=\"button\"> Filter: " + string.Format("{0:#,0}", responseDict[requestID].selectedRecordCount) + " of " + string.Format("{0:#,0}", responseDict[requestID].sourcedRecordCount) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"distinctCount\" type=\"button\"> Set: " + string.Format("{0:#,0}", responseDict[requestID].distinctCount) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"time\" type=\"button\"> Duration: " + String.Format("{0:F3}", (responseDict[requestID].endTime - responseDict[requestID].startTime).TotalSeconds) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"table\" type=\"button\"> Table: " + requestDict[requestID].importFile.ToString() + "</button>");
            html.AppendLine("</div>");
            html.AppendLine("<div class=\"w3-container\">");
            html.AppendLine("<div class=\"w3-row w3-border\">");
            html.AppendLine("<div class=\"w3-half w3-container\" id=\"leftTable\">");
            html.AppendLine("<table class=\"w3-table w3-bordered w3-table-all\">");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<thead>");
            html.AppendLine("<tr>");

            if (requestDict[requestID].processButton == "moveColumn") 
                html.AppendLine("<th class=\"w3-left-align\"><button class=\"w3-btn w3-white w3-round-large\">" + "Set" + "</button></th>");
            if (requestDict[requestID].processButton == "drillRow")
                html.AppendLine("<th class=\"w3-left-align\">" + "Set" + "</th>");

            for (int cell = 0; cell < distinctList.Count; cell++)
            {
                if (distinctRamKey2Value[cell].Count == 1 && requestDict[requestID].processButton == "moveColumn")
                    html.AppendLine("<th class=\"w3-right-align\"><button class=\"w3-btn w3-white w3-round-large\">" + distinctRamKey2Value[cell][distinctList[cell][0]] + "</button></th>");

                if (distinctRamKey2Value[cell].Count == 1 && requestDict[requestID].processButton == "drillRow")
                    html.AppendLine("<th class=\"w3-right-align\">" + distinctRamKey2Value[cell][distinctList[cell][0]] + "</th>");

                if (distinctRamKey2Value[cell].Count > 1)
                {
                    var trimWhitespace = distinctRamKey2Value[cell][distinctList[cell][0]];
                    trimWhitespace = trimWhitespace.Replace(" ", "#");

                    if (requestDict[requestID].processButton == "moveColumn")
                        html.AppendLine("<th class=\"w3-left-align\"><button class=\"w3-btn w3-white w3-round-large\" onclick=\"rotatePairDimension(this)\" draggable=\"true\" ondragend=\"moveToCrosstab(this)\" value=" + nextPageID + " name=\"distinctOrder\"" + " id=" + trimWhitespace + " > " + distinctRamKey2Value[cell][distinctList[cell][0]] + "</button></th>");
                    
                    if (requestDict[requestID].processButton == "drillRow")
                        html.AppendLine("<th class=\"w3-left-align\" name=\"distinctOrder\"" + " id=" + trimWhitespace + " > " + distinctRamKey2Value[cell][distinctList[cell][0]] + "</th>");
                }
            }
           
            if (YpageEnd > distinctList[0].Count)
                YpageEnd = distinctList[0].Count;

            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;
            string textStyle = "class=\"w3-right-align w3-text-blue\"";

            if (requestDict[requestID].processButton == "moveColumn")
            { 
                for (var line = YpageStart; line < YpageEnd; line++)
                {
                    html.AppendLine("<tr class=\"w3-hover-pale-red\">");                 
                    html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " >" + line.ToString() + "</td>");

                    for (int cell = 0; cell < distinctList.Count; cell++)
                    {
                        if (distinctRamKey2Value[cell].Count == 1 && distinctRamKey2Value[cell][0] == "Fact")
                            html.AppendLine("<td class=\"w3-right-align w3-text-blue\" id=" + line.ToString() + "~" + nextPageID + " " + " >" + distinctList[cell][line].ToString() + "</td>");

                        if (distinctRamKey2Value[cell].Count == 1 && distinctRamKey2Value[cell][0] != "Fact")
                        {
                            if (distinctList[cell][line] > 0)
                                textStyle = "class=\"w3-right-align w3-text-blue\"";

                            if (distinctList[cell][line] < 0)
                                textStyle = "class=\"w3-right-align w3-text-red\"";

                            if (requestDict[requestID].precisionLevel == "Cent") 
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + textStyle + ">" + string.Format("{0:#,0.00}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Dollar") 
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + textStyle + ">" + string.Format("{0:#,0}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Thousand") 
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + textStyle + ">" + string.Format("{0:#,0,K}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Million") 
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + textStyle + ">" + string.Format("{0:#,0,,M}", distinctList[cell][line]).ToString() + "</td>");                                                    
                        }

                        if (distinctRamKey2Value[cell].Count > 1)
                        {
                            if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                            {
                                isDateNumber = double.TryParse(distinctRamKey2Value[cell][distinctList[cell][line]], out dateNumber);
                                if (isDateNumber == true)
                                {
                                    if (dateNumber > 1000 && dateNumber < 401770)
                                    {
                                        dateValue = DateTime.FromOADate(dateNumber);
                                        dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                        if (line > 0 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                            html.AppendLine("<td class=\"w3-text-yellow\">" + dateFormat + "</td>");
                                        else
                                            html.AppendLine("<td class=\"w3-text-brown\">" + dateFormat + "</td>");
                                    }
                                }
                                else
                                    if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                    html.AppendLine("<td class=\"w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                                else
                                    html.AppendLine("<td class=\"w3-text-brown\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                            }
                            else
                                if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                html.AppendLine("<td class=\"w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-text-brown\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                        }
                    }

                    html.AppendLine("</tr>");
                    html.AppendLine(Environment.NewLine);
                }
            }            

            if (requestDict[requestID].processButton == "drillRow")
            {
                for (var line = YpageStart; line < YpageEnd; line++)
                {
                    html.AppendLine("<tr class=\"w3-hover-pale-red\">");                   
                    html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + "onclick =\"drillDown(this)\" >" + line.ToString() + "</td>");

                    for (int cell = 0; cell < distinctList.Count; cell++)
                    {
                        if (distinctRamKey2Value[cell].Count == 1 && distinctRamKey2Value[cell][0] == "Fact")
                            html.AppendLine("<td class=\"w3-right-align w3-text-blue\" id=" + line.ToString() + "~" + nextPageID + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\" >" + distinctList[cell][line].ToString() + "</td>");

                        if (distinctRamKey2Value[cell].Count == 1 && distinctRamKey2Value[cell][0] != "Fact")
                        {
                            if (distinctList[cell][line] > 0)
                                textStyle = "class=\"w3-right-align w3-text-blue\"";

                            if (distinctList[cell][line] < 0)
                                textStyle = "class=\"w3-right-align w3-text-red\"";

                            if (requestDict[requestID].precisionLevel == "Cent")
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\"" + textStyle + ">" + string.Format("{0:#,0.00}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Dollar")
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\"" + textStyle + ">" + string.Format("{0:#,0}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Thousand")
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\"" + textStyle + ">" + string.Format("{0:#,0,K}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Million")
                                html.AppendLine("<td id=" + line.ToString() + "~" + nextPageID + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\"" + textStyle + ">" + string.Format("{0:#,0,,M}", distinctList[cell][line]).ToString() + "</td>");
                        }

                        if (distinctRamKey2Value[cell].Count > 1)
                        {
                            if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                            {
                                isDateNumber = double.TryParse(distinctRamKey2Value[cell][distinctList[cell][line]], out dateNumber);
                                if (isDateNumber == true)
                                {
                                    if (dateNumber > 1000 && dateNumber < 401770)
                                    {
                                        dateValue = DateTime.FromOADate(dateNumber);
                                        dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                        if (line > 0 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                            html.AppendLine("<td class=\"w3-text-yellow\">" + dateFormat + "</td>");
                                        else
                                            html.AppendLine("<td class=\"w3-text-brown\">" + dateFormat + "</td>");
                                    }
                                }
                                else
                                    if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                    html.AppendLine("<td class=\"w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                                else
                                    html.AppendLine("<td class=\"w3-text-brown\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                            }
                            else
                                if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                html.AppendLine("<td class=\"w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-text-brown\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                        }
                    }
                    html.AppendLine("</tr>");
                    html.AppendLine(Environment.NewLine);
                }
            }         
            html.AppendLine("</table>");
            html.AppendLine("</div>");
            html.AppendLine("<div onscroll=\"scrollTableStoreWindows()\" class=\"w3-half w3-container\" id=\"rightTable\">");


            html.AppendLine("<table class=\"w3-table w3-bordered w3-table-all\">");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<thead>");
            html.AppendLine("<tr>");
            html.AppendLine("<th class=\"w3-right-align\">" + "Fact" + "</th>");
            html.AppendLine("<th class=\"w3-left-align\">" + "Table Name" + "</th>");
            html.AppendLine("</tr>");

            List<string> fileList = new List<string>();

            string[] importfile = Directory.GetFiles(sourceFolder.ToString(), "*.csv");

            foreach (string filePath in importfile)
            {
                var fileNameLength = filePath.Length - 1 - filePath.LastIndexOf((char)92);
                var fileName = filePath.Substring(filePath.LastIndexOf((char)92) + 1, fileNameLength);
                fileList.Add(fileName);
            }

            for (int i = 0; i < fileList.Count; i++)
            {
                html.AppendLine("<tr class=\"w3-hover-pale-yellow\">");

               
                html.AppendLine("<td class=\"w3-right-align w3-text-red\" name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + "0" + "</td>");
                html.AppendLine("<td class=\"w3-left-align w3-text-red\"  name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + fileList[i].ToString() + "</td>");
               
                html.AppendLine("</tr>");
            }


            fileList.Clear();
            string[] distinctDB = Directory.GetFiles(db1Folder.ToString(), "ramDetail*.db", SearchOption.AllDirectories);

            foreach (string filePath in distinctDB)
            {              
                int lastOneMatch = filePath.Substring(0, filePath.LastIndexOf((char)92) + 1).LastIndexOf((char)92);
                int lastTwoMatch = filePath.Substring(0, filePath.LastIndexOf((char)92) - 1).LastIndexOf((char)92);
                var fileName = filePath.Substring(lastTwoMatch + 1, lastOneMatch - lastTwoMatch - 1);
                fileList.Add(fileName);
            }      

            for (int i = 0; i < fileList.Count; i++)
            {  
                html.AppendLine("<tr class=\"w3-hover-pale-yellow\">");

                if(remK2V.ContainsKey(fileList[i].ToString()))
                {
                    if(tableFact.ContainsKey(fileList[i].ToString()))
                    {                        
                        html.AppendLine("<td class=\"w3-right-align w3-text-blue\" name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + tableFact[fileList[i].ToString()][8].ToString() + "</td>");
                        html.AppendLine("<td class=\"w3-left-align w3-text-blue\"  name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + fileList[i].ToString() + "</td>");
                    }
                }
                else
                { 
                    html.AppendLine("<td class=\"w3-right-align w3-text-green\"  name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + "0" + "</td>");
                    html.AppendLine("<td class=\"w3-left-align w3-text-green\"  name = \"onclick\" onclick=\"selectFromFileTable(this)\"" + " " + "id=" + fileList[i].ToString() + " >" + fileList[i].ToString() + "</td>");
                }
                html.AppendLine("</tr>");            
            }            

            html.AppendLine(Environment.NewLine);
            html.AppendLine("</table>");   
            html.AppendLine("</div>");
            html.AppendLine("</div>");
            html.AppendLine("</output-report>");

            responseDict[requestID].html = html;

            ExportHTML currentExport = new ExportHTML();

            requestDict[requestID].crosstabOrder = null;
            if (!screenControl.ContainsKey(requestID))
            {
                screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                screenControl[requestID].Add("displayCrosstalDimension", new StringBuilder());
                screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2CrosstabUpdate(tableFact, outputFolder, requestID, requestDict, ramKey2Valuegz));
                responseDict[requestID].updateCrosstab = true;
            }
            else
            {
                if(screenControl[requestID].ContainsKey("displayCrosstalDimension"))
                { 
                    screenControl[requestID]["displayCrosstalDimension"].Clear();
                    screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2CrosstabUpdate(tableFact, outputFolder, requestID, requestDict, ramKey2Valuegz));
                    responseDict[requestID].updateCrosstab = true;
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + requestID.ToString() + ".html"))
                {
                    toDisk.Write(responseDict[requestID].html);
                    toDisk.Close();
                }
            }           
        }        
        public void drillDown2HtmlYMtable(Dictionary<int, List<double>> ramDetailgz, List<int> _drillSet, int Ypage, double YtotalPage, int YpageStart, int YpageEnd, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string outputFolder, List<decimal> distinctSet, Dictionary<decimal, List<int>> distinctList2DrillDown, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            decimal nextPageID;

            if (requestDict[requestID].processID == "pageMove" || requestDict[requestID].processID == "displayPrecision")
                nextPageID = requestDict[2].nextPageID;
            else
                nextPageID = responseDict[requestID].requestID;

            html.AppendLine("<output-drillDownReport id=" + nextPageID + " name=\"response\"" + "  #fast=" + requestDict[requestID].importFile + ">");
            html.AppendLine("<div class=\"w3-container\" id=submitForm value=\"0\">");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"openReportByWindowsDrill(this)\" value=" + nextPageID + " id=\"openReportByWindows\"><i class=\"material-icons\">open_with</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageUpDrill(this)\" value=" + nextPageID + " id=\"up\"><i class=\"material-icons\">arrow_upward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick =\"pageEndDownDrill(this)\" value=" + nextPageID + " id=\"pageEndDown\" type=\"button\" style=\"font-weight:bold\"> Y" + Ypage + "/" + YtotalPage + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageDownDrill(this)\" value=" + nextPageID + " id=\"down\"><i class=\"material-icons\">arrow_downward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageHomeDrill(this)\" value=" + nextPageID + " id=\"pageHome\"><i class=\"material-icons\">home</i></button></button>");
            html.AppendLine("</div>");
            html.AppendLine("<div class=\"w3-container w3-responsive\">");
            html.AppendLine("<table class=\"w3-table w3-bordered w3-table-all\">");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<thead>");

            html.AppendLine("<tr>");
            html.AppendLine("<th class=\"w3-left-align\">" + "Set" + "</th>");

            for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
            {
                if (ramKey2Valuegz[cell].Count == 1)
                    html.AppendLine("<th class=\"w3-right-align\">" + ramKey2Valuegz[cell][0] + "</th>");

                if (ramKey2Valuegz[cell].Count > 1)               
                    html.AppendLine("<th class=\"w3-left-align\"> " + ramKey2Valuegz[cell][0] + "</th>");
              
            }            
            //distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][cell][line - 1]
            //ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]

            if (YpageEnd > distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count)
                YpageEnd = distinctList2DrillDown[distinctSet[_drillSet[0] - 1]].Count;

            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            Dictionary<int, int> decimalPlace = new Dictionary<int, int>();

            for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                if (ramKey2Valuegz[cell].Count == 1)
                    decimalPlace.Add(cell, 0);

            for (var line = YpageStart; line <= YpageEnd; line++)
            {
                for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {
                        var number = Convert.ToDecimal(ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]);

                        if ((number * 1) % 1 == 0)
                        {
                            if (1 >= decimalPlace[cell])
                                decimalPlace[cell] = 0;
                        }
                        else if ((number * 10) % 1 == 0)
                        {
                            if (1 > decimalPlace[cell])
                                decimalPlace[cell] = 1;
                        }
                        else if ((number * 100) % 1 == 0)
                        {
                            if (2 > decimalPlace[cell])
                                decimalPlace[cell] = 2;
                        }
                        else if ((number * 1000) % 1 == 0)
                        {
                            if (3 > decimalPlace[cell])
                                decimalPlace[cell] = 3;
                        }
                        else if ((number * 10000) % 1 == 0)
                        {
                            if (4 > decimalPlace[cell])
                                decimalPlace[cell] = 4;
                        }
                        else if ((number * 100000) % 1 == 0)
                        {
                            if (5 > decimalPlace[cell])
                                decimalPlace[cell] = 5;
                        }
                        else if ((number * 1000000) % 1 == 0)
                        {
                            if (6 > decimalPlace[cell])
                                decimalPlace[cell] = 6;
                        }
                        else if ((number * 10000000) % 1 == 0)
                        {
                            if (7 > decimalPlace[cell])
                                decimalPlace[cell] = 7;
                        }
                        else
                            decimalPlace[cell] = 8;
                    }
                }
            }

            string textStyle = "class=\"w3-right-align w3-text-red\"";

            for (var line = YpageStart; line <= YpageEnd; line++)
            {
                html.AppendLine("<tr class=\"w3-hover-pale-yellow\">");
                html.AppendLine("<td>" + _drillSet[0].ToString() + "." + line.ToString() + "</td>");
                for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {
                        if (ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]] > 0)
                            textStyle = "class=\"w3-right-align w3-text-blue\"";

                        if (ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]] < 0)
                            textStyle = "class=\"w3-right-align w3-text-red\"";

                        if (decimalPlace[cell] == 0) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 1) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.0}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 2) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.00}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 3) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 4) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.0000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 5) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.00000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 6) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.000000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 7) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.0000000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 8) 
                            html.AppendLine("<td " + textStyle + ">" + string.Format("{0:#,0.00000000}", ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]).ToString() + "</td>");                        
                    }

                    if (ramKey2Valuegz[cell].Count > 1)
                    {
                        if (ramKey2Valuegz[cell][0].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]], out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 2]]])
                                        html.AppendLine("<td class=\"w3-text-yellow\">" + dateFormat + "</td>");
                                    else
                                        html.AppendLine("<td class=\"w3-text-brown\">" + dateFormat + "</td>");
                                }
                            }
                            else
                                if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 2]]])
                                html.AppendLine("<td class=\"w3-text-light-yellow\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-text-brown\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] + "</td>");
                        }
                        else
                            if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 2]]])
                            html.AppendLine("<td class=\"w3-text-yellow\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] + "</td>");
                        else
                            html.AppendLine("<td class=\"w3-text-brown\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[distinctSet[_drillSet[0] - 1]][line - 1]]] + "</td>");
                    }
                }

                html.AppendLine("</tr>");
                html.AppendLine(Environment.NewLine);
            }

            html.AppendLine("</tbody>");
            html.AppendLine("</table>");
            html.AppendLine("</div>");
            html.AppendLine("</output-report>");

            responseDict[requestID].html = html;

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + requestID.ToString() + "DrillDown.html"))
                {
                    toDisk.Write(responseDict[requestID].html);
                    toDisk.Close();
                }
            }
        }        
        public void ramDistinct2HtmlCrosstabTable(ConcurrentDictionary<string, ConcurrentDictionary<int, int>> tableFact, ConcurrentDictionary<string, clientMachine.userPreference> userPreference, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz, Dictionary<decimal, Dictionary<string, StringBuilder>> screenControl, Dictionary<int, List<double>> YXMdistinctDrillKey, int Xpage, double XtotalPage, int Ypage, double YtotalPage, int XpageStart, int XpageEnd, int yHeaderCol, int YpageStart, int YpageEnd, Dictionary<int, List<double>> XdistinctList, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict,  Dictionary<int, List<double>> distinctList, Dictionary<int, Dictionary<double, string>> distinctXramKey2Value, Dictionary<int, Dictionary<double, string>> distinctRamKey2Value, string outputFolder, Dictionary<int, string> dataSortingOrder)
        {
            StringBuilder html = new StringBuilder();            
            decimal nextPageID;
            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;
            responseDict[requestID].updateCrosstab = false;

            if (requestDict[requestID].processID == "pageMove" || requestDict[requestID].processID == "displayPrecision")
                nextPageID = requestDict[2].nextPageID;
            else
                nextPageID = responseDict[requestID].requestID;

            html.AppendLine("<output-report id=" + nextPageID + " name=\"response\"" + " #fast=" + requestDict[requestID].importFile + ">");           
            html.AppendLine("<div class=\"w3-container\" id=submitForm value=\"0\">");          
            html.AppendLine("<p></p>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"requestID\" type=\"button\"> Request: " + nextPageID + "</button>");

            if (requestDict.ContainsKey(2) == true)
            {
                if (requestDict[2].sortingOrder == "sortDescending")
                {
                    dataSortingOrder[0] = "sortDescending";
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }

                else if (requestDict[2].sortingOrder == "sortAscending")
                {
                    dataSortingOrder[0] = "sortAscending";
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }
                else
                {
                    if (dataSortingOrder[0] == "sortDescending")
                        html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");

                    if (dataSortingOrder[0] == "sortAscending")
                        html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
                }
            }
            else
            {
                if (requestDict[requestID].sortingOrder == "sortAscending" || dataSortingOrder[0] == "sortAscending")
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortAscending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");

                if (requestDict[requestID].sortingOrder == "sortDescending" || dataSortingOrder[0] == "sortDescending")
                    html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"sortDescending(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">sort_by_alpha</i></button>");
            }            
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"openReportByWindows(this)\" value=" + nextPageID + " id=\"openReportByWindows\"><i class=\"material-icons\">open_with</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageUp(this)\" value=" + nextPageID + " id=\"up\"><i class=\"material-icons\">arrow_upward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick =\"pageEndDown(this)\" value=" + nextPageID + " id=\"pageEndDown\" type=\"button\" style=\"font-weight:bold\"> Y" + Ypage + "/" + YtotalPage + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageDown(this)\" value=" + nextPageID + " id=\"down\"><i class=\"material-icons\">arrow_downward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageHome(this)\" value=" + nextPageID + " id=\"pageHome\"><i class=\"material-icons\">home</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageLeft(this)\" value=" + nextPageID + " id=\"left\"><i class=\"material-icons\">arrow_back</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick =\"pageEndRight(this)\" value=" + nextPageID + " id=\"pageEndRight\" type=\"button\" style=\"font-weight:bold\"> X" + Xpage + "/" + (XtotalPage + 1) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageRight(this)\" value=" + nextPageID + " id=\"right\"><i class=\"material-icons\">arrow_forward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"nextPrecision(this)\" value=" + nextPageID + " id =\"next\"><i class=\"material-icons\">visibility</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"filterCount\" type=\"button\"> Filter: " + string.Format("{0:#,0}", responseDict[requestID].selectedRecordCount) + " of " + string.Format("{0:#,0}", responseDict[requestID].sourcedRecordCount) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"distinctCount\" type=\"button\"> Set: " + string.Format("{0:#,0}", responseDict[requestID].distinctCount) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"crosstabCount\" type=\"button\"> Crosstab: " + string.Format("{0:#,0}", responseDict[requestID].crosstabCount) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"time\" type=\"button\"> Duration: " + String.Format("{0:F3}", (responseDict[requestID].endTime - responseDict[requestID].startTime).TotalSeconds) + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" id=\"table\" type=\"button\"> Table: " + requestDict[requestID].importFile.ToString() + "</button>");
            html.AppendLine("</div>");
            html.AppendLine("<div class=\"w3-container w3-responsive\">");
            html.AppendLine("<p></p>");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<table class=\"w3-table w3-bordered w3-table-all\">");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<thead>");

            XtotalPage = XtotalPage + 1;
            string trimWhitespace;

            List<string> crosstab = new List<string>();

            if (requestDict[requestID].crosstabOrder !=null)
                requestDict[requestID].crosstabOrder.Clear();

            for (int j = 0; j < XdistinctList.Count; j++)
            {
                html.AppendLine("<tr>");
                html.AppendLine("<th></th>");

                for (int cell = 0; cell < yHeaderCol - 1; cell++)
                    html.AppendLine("<th></th>");

                trimWhitespace = distinctXramKey2Value[j][XdistinctList[j][0]];
                trimWhitespace = trimWhitespace.Replace(" ", "#");

                if (requestDict[requestID].processButton == "moveColumn")
                { 
                    html.AppendLine("<th><button class=\"w3-btn w3-white w3-text-teal w3-round-large\" onclick=\"rotatePairDimensionCrosstab(this)\" draggable=\"true\" ondragend=\"removeFromCrosstab(this)\"  value=" + nextPageID + " name=\"crosstabOrder\"" + " id=" + trimWhitespace + ">" + distinctXramKey2Value[j][XdistinctList[j][0]] + "</button></th>");
                    crosstab.Add(distinctXramKey2Value[j][XdistinctList[j][0]]);
                }
                
                if (requestDict[requestID].processButton == "drillRow")
                { 
                    html.AppendLine("<th class=\"w3-text-teal\">" + distinctXramKey2Value[j][XdistinctList[j][0]] + "</th>");
                    crosstab.Add(distinctXramKey2Value[j][XdistinctList[j][0]]);
                }

                for (int cell = XpageStart; cell <= XpageEnd; cell++)
                    for (int i = 0; i < requestDict[requestID].measurement.Count; i++)
                    {                       
                        trimWhitespace = distinctXramKey2Value[j][XdistinctList[j][0]];
                        trimWhitespace = trimWhitespace.Replace(" ", "#");

                        if (trimWhitespace.ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(distinctXramKey2Value[j][XdistinctList[j][cell + 1]], out dateNumber);
                           
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yy");
                                    if (i > 0 || (cell > 0 && distinctXramKey2Value[j][XdistinctList[j][cell + 1]] == distinctXramKey2Value[j][XdistinctList[j][cell]]))
                                        html.AppendLine("<td class=\"w3-center w3-text-yellow\">" + dateFormat + "</td>");
                                    else
                                        html.AppendLine("<td class=\"w3-center w3-text-brown\">" + dateFormat + "</td>");
                                }
                            }
                            else
                                if (i > 0 || (cell > 0 && distinctXramKey2Value[j][XdistinctList[j][cell + 1]] == distinctXramKey2Value[j][XdistinctList[j][cell]]))
                                   html.AppendLine("<td class=\"w3-center w3-text-yellow\">" + distinctXramKey2Value[j][XdistinctList[j][cell + 1]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-center w3-text-brown\">" + distinctXramKey2Value[j][XdistinctList[j][cell + 1]] + "</td>");                           
                        }
                        else
                            if (i > 0 || (cell > 0 && distinctXramKey2Value[j][XdistinctList[j][cell + 1]] == distinctXramKey2Value[j][XdistinctList[j][cell]]))
                               html.AppendLine("<td class=\"w3-center w3-text-yellow\">" + distinctXramKey2Value[j][XdistinctList[j][cell + 1]] + "</td>");
                        else
                            html.AppendLine("<td class=\"w3-center w3-text-brown\">" + distinctXramKey2Value[j][XdistinctList[j][cell + 1]] + "</td>");                       
                    }
                html.AppendLine("</tr>");
                html.AppendLine(Environment.NewLine);               
            }

            html.AppendLine("<tr>");            
            html.AppendLine("<th class=\"w3-left-align\">" + "Set" + "</th>");
            for (int cell = 0; cell < yHeaderCol; cell++)
            {              
                trimWhitespace = distinctRamKey2Value[cell][distinctList[cell][0]];
                trimWhitespace = trimWhitespace.Replace(" ", "#");

                if (requestDict[requestID].processButton == "moveColumn")
                    html.AppendLine("<th><button class=\"w3-btn w3-white w3-round-large\" onclick=\"rotatePairDimensionCrosstab(this)\" draggable=\"true\" ondragend=\"moveToCrosstab(this)\" value=" + nextPageID + " name=\"distinctOrder\"" + " id=" + trimWhitespace + ">" + distinctRamKey2Value[cell][distinctList[cell][0]] + "</button></th>");

                if (requestDict[requestID].processButton == "drillRow")
                    html.AppendLine("<th>" + distinctRamKey2Value[cell][distinctList[cell][0]] + "</th>");
            }

            for (var cell = XpageStart; cell <= XpageEnd; cell++)
                for (var i = 0; i < requestDict[requestID].measurement.Count; i++)
                    html.AppendLine("<th class=\"w3-right-align\">" + requestDict[requestID].measurement[i].Replace("#", " ") + "</th>");

            html.AppendLine("</tr>");
            html.AppendLine("</thead>");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<tbody>");            

            if (YpageEnd > distinctList[0].Count)
                YpageEnd = distinctList[0].Count;

            string textStyle = "class=\"w3-right-align w3-text-blue\"";

            for (var line = YpageStart; line < YpageEnd; line++)
            {
                html.AppendLine("<tr>");
                html.AppendLine("<td>" + line.ToString() + "</td>");
                for (int cell = 0; cell < yHeaderCol; cell++)
                {
                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count == 1)
                    {
                        if (distinctList[cell][line] == 0) html.AppendLine("<td class=\"w3-right-align\">" + "-" + "</td>");

                        if (distinctRamKey2Value[cell].Count == 1 && distinctRamKey2Value[cell][0] != "Fact")
                        {
                            if (distinctList[cell][line] > 0)
                                textStyle = "class=\"w3-right-align w3-text-blue\"";                               

                            if (distinctList[cell][line] < 0)
                                textStyle = "class=\"w3-right-align w3-text-red\"";

                            if (requestDict[requestID].precisionLevel == "Cent")
                                html.AppendLine("<td id=" + line.ToString() + "~" + textStyle + ">" + string.Format("{0:#,0.00}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Dollar")
                                html.AppendLine("<td id=" + line.ToString() + "~" + textStyle + ">" + string.Format("{0:#,0}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Thousand")
                                html.AppendLine("<td id=" + line.ToString() + "~" + textStyle + ">" + string.Format("{0:#,0,K}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Million")
                                html.AppendLine("<td id=" + line.ToString() + "~" +  textStyle + ">" + string.Format("{0:#,0,,M}", distinctList[cell][line]).ToString() + "</td>");
                        }
                    }                   
                    
                    if (cell < yHeaderCol && distinctRamKey2Value[cell].Count > 1)
                    { 
                        if (distinctRamKey2Value[cell][distinctList[cell][0]].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(distinctRamKey2Value[cell][distinctList[cell][line]], out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                        html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + dateFormat + "</td>");
                                    else
                                        html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" + dateFormat + "</td>");
                                }
                            }
                            else
                                if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                                html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" +  distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                        }
                        else
                            if (line > 1 && distinctRamKey2Value[cell][distinctList[cell][line]] == distinctRamKey2Value[cell][distinctList[cell][line - 1]])
                            html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                        else
                            html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" + distinctRamKey2Value[cell][distinctList[cell][line]] + "</td>");
                    }                    
                }

                if (requestDict[requestID].processButton == "drillRow")
                {
                    for (int cell = yHeaderCol + ((XpageStart) * requestDict[requestID].measurement.Count); cell <= yHeaderCol + ((XpageEnd) * requestDict[requestID].measurement.Count) + (requestDict[requestID].measurement.Count - 1); cell++)
                    {
                        if (distinctList[cell][line] == 0) 
                            html.AppendLine("<td class=\"w3-right-align\">" + "-" + "</td>");

                        if (distinctList[cell][line] != 0)
                            html.Append("<td id=" + YXMdistinctDrillKey[cell][line].ToString() + "~" + nextPageID + " " + "headers=" + line.ToString() + "." + (cell - yHeaderCol + 1).ToString() + " " + "ondblclick =\"changeDrillDownEvent(this)\"" + userPreference["system"].drillDownEventType + " =\"drillDown(this)\"");

                        if (distinctList[cell][line] > 0)
                            html.Append("class=\"w3-right-align w3-text-blue w3-hover-pale-red\">");

                        if (distinctList[cell][line] < 0)
                            html.Append("class=\"w3-right-align w3-text-red w3-hover-pale-blue\">");

                        if (distinctList[cell][line] != 0)
                        {                         
                            if (requestDict[requestID].precisionLevel == "Cent") 
                                html.AppendLine(string.Format("{0:#,0.00}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Dollar") 
                                html.AppendLine(string.Format("{0:#,0}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Thousand") 
                                html.AppendLine(string.Format("{0:#,0,K}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Million") 
                                html.AppendLine(string.Format("{0:#,0,,M}", distinctList[cell][line]).ToString() + "</td>");
                        }                       
                    }
                }
                else
                {
                    for (int cell = yHeaderCol + ((XpageStart) * requestDict[requestID].measurement.Count); cell <= yHeaderCol + ((XpageEnd) * requestDict[requestID].measurement.Count) + (requestDict[requestID].measurement.Count - 1); cell++)
                    {
                        if (distinctList[cell][line] == 0) 
                            html.AppendLine("<td class=\"w3-right-align\">" + "-" + "</td>");

                        if (distinctList[cell][line] > 0)
                            html.AppendLine("<td class=\"w3-right-align w3-text-blue\">");

                        if (distinctList[cell][line] < 0)
                            html.AppendLine("<td class=\"w3-right-align w3-text-red\">");

                        if (distinctList[cell][line] != 0)
                        {
                            if (requestDict[requestID].precisionLevel == "Cent")
                                html.AppendLine(string.Format("{0:#,0.00}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Dollar")
                                html.AppendLine(string.Format("{0:#,0}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Thousand")
                                html.AppendLine(string.Format("{0:#,0,K}", distinctList[cell][line]).ToString() + "</td>");
                            if (requestDict[requestID].precisionLevel == "Million")
                                html.AppendLine(string.Format("{0:#,0,,M}", distinctList[cell][line]).ToString() + "</td>");
                        }                        
                    }
                }               
                html.AppendLine("</tr>");
                html.AppendLine(Environment.NewLine);
            }
            html.AppendLine("</tbody>");
            html.AppendLine("</table>");
            html.AppendLine("</div>");          
            html.AppendLine("</output-report>");

            responseDict[requestID].html = html;

            ExportHTML currentExport = new ExportHTML();

            requestDict[requestID].crosstabOrder = crosstab;
            if(!screenControl.ContainsKey(requestID))
            { 
                screenControl.Add(requestID, new Dictionary<string, StringBuilder>());
                screenControl[requestID].Add("displayCrosstalDimension", new StringBuilder());
                screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2CrosstabUpdate(tableFact, outputFolder, requestID, requestDict, ramKey2Valuegz));
                responseDict[requestID].updateCrosstab = true;
            }
            else
            {
                if(screenControl[requestID].ContainsKey("displayCrosstalDimension"))
                { 
                    screenControl[requestID]["displayCrosstalDimension"].Clear();
                    screenControl[requestID]["displayCrosstalDimension"].Append(currentExport.ramdistint2CrosstabUpdate(tableFact, outputFolder, requestID, requestDict, ramKey2Valuegz));
                    responseDict[requestID].updateCrosstab = true;
                }
            }

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + requestID.ToString() + ".html"))
                {
                    toDisk.Write(responseDict[requestID].html);
                    toDisk.Close();
                }
            }           
        }
        public void drillDown2HtmlYMtableCrosstab(Dictionary<int, List<double>> ramDetailgz, List<decimal> _drillSet, int Ypage, double YtotalPage, int YpageStart, int YpageEnd, decimal requestID, ConcurrentDictionary<decimal, clientMachine.request> requestDict, ConcurrentDictionary<decimal, clientMachine.response> responseDict, string outputFolder, Dictionary<decimal, List<int>> distinctList2DrillDown, Dictionary<int, Dictionary<double, string>> ramKey2Valuegz)
        {
            StringBuilder html = new StringBuilder();
            decimal nextPageID;

            if (requestDict[requestID].processID == "pageMove" || requestDict[requestID].processID == "displayPrecision")
                nextPageID = requestDict[2].nextPageID;
            else
                nextPageID = responseDict[requestID].requestID;

            html.AppendLine("<output-drillDownReport id=" + nextPageID + " name=\"response\"" + "  #fast=" + requestDict[requestID].importFile + ">");
            html.AppendLine("<div class=\"w3-container\" id=submitForm value=\"0\">");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"openReportByWindowsDrill(this)\" value=" + nextPageID + " id=\"openReportByWindows\"><i class=\"material-icons\">open_with</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageUpDrill(this)\" value=" + nextPageID + " id=\"up\"><i class=\"material-icons\">arrow_upward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick =\"pageEndDownDrill(this)\" value=" + nextPageID + " id=\"pageEndDown\" type=\"button\" style=\"font-weight:bold\"> Y" + Ypage + "/" + YtotalPage + "</button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageDownDrill(this)\" value=" + nextPageID + " id=\"down\"><i class=\"material-icons\">arrow_downward</i></button>");
            html.AppendLine("<button class=\"w3-btn w3-white w3-round-large\" onclick=\"pageHomeDrill(this)\" value=" + nextPageID + " id=\"pageHome\"><i class=\"material-icons\">home</i></button></button>");
            html.AppendLine("</div>");
            html.AppendLine("<div class=\"w3-container w3-responsive\">");
            html.AppendLine("<table class=\"w3-table w3-bordered w3-table-all\">");
            html.AppendLine(Environment.NewLine);
            html.AppendLine("<thead>");

            html.AppendLine("<tr>");
            html.AppendLine("<th class=\"w3-left-align\">" + "Set" + "</th>");

            for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
            {
                if (ramKey2Valuegz[cell].Count == 1)
                    html.AppendLine("<th class=\"w3-right-align\">" + ramKey2Valuegz[cell][0] + "</th>");

                if (ramKey2Valuegz[cell].Count > 1)
                {
                    html.AppendLine("<th class=\"w3-left-align\">" + ramKey2Valuegz[cell][0] + "</th>");
                }
            }

            if (YpageEnd > distinctList2DrillDown[_drillSet[0]].Count)
                YpageEnd = distinctList2DrillDown[_drillSet[0]].Count;

            DateTime dateValue;
            bool isDateNumber;
            double dateNumber;
            string dateFormat;

            Dictionary<int, int> decimalPlace = new Dictionary<int, int>();

            for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                if (ramKey2Valuegz[cell].Count == 1)
                    decimalPlace.Add(cell, 0);

            for (var line = YpageStart; line <= YpageEnd; line++)
            {
                for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {
                        var number = Convert.ToDecimal(ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]);

                        if ((number * 1) % 1 == 0)
                        {
                            if (1 >= decimalPlace[cell])
                                decimalPlace[cell] = 0;
                        }
                        else if ((number * 10) % 1 == 0)
                        {
                            if (1 > decimalPlace[cell])
                                decimalPlace[cell] = 1;
                        }
                        else if ((number * 100) % 1 == 0)
                        {
                            if (2 > decimalPlace[cell])
                                decimalPlace[cell] = 2;
                        }
                        else if ((number * 1000) % 1 == 0)
                        {
                            if (3 > decimalPlace[cell])
                                decimalPlace[cell] = 3;
                        }
                        else if ((number * 10000) % 1 == 0)
                        {
                            if (4 > decimalPlace[cell])
                                decimalPlace[cell] = 4;
                        }
                        else if ((number * 100000) % 1 == 0)
                        {
                            if (5 > decimalPlace[cell])
                                decimalPlace[cell] = 5;
                        }
                        else if ((number * 1000000) % 1 == 0)
                        {
                            if (6 > decimalPlace[cell])
                                decimalPlace[cell] = 6;
                        }
                        else if ((number * 10000000) % 1 == 0)
                        {
                            if (7 > decimalPlace[cell])
                                decimalPlace[cell] = 7;
                        }
                        else
                            decimalPlace[cell] = 8;
                    }
                }
            }

            for (var line = YpageStart; line <= YpageEnd; line++)
            {
                html.AppendLine("<tr class=\"w3-hover-pale-yellow\">");
                //  html.AppendLine("<td>" + _drillSet[0].ToString() + "." + line.ToString() + "</td>");
                html.AppendLine("<td>" + requestDict[2].drillSetHeader + "." + line.ToString() + "</td>");
                for (int cell = 0; cell < ramKey2Valuegz.Count; cell++)
                {
                    if (ramKey2Valuegz[cell].Count == 1)
                    {

                        if (ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]] < 0)
                            html.AppendLine("<td class=\"w3-right-align w3-text-red\">");
                        else
                            html.AppendLine("<td class=\"w3-right-align w3-text-blue\">");

                        if (decimalPlace[cell] == 0)
                            html.AppendLine(string.Format("{0:#,0}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 1)
                            html.AppendLine(string.Format("{0:#,0.0}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 2)
                            html.AppendLine(string.Format("{0:#,0.00}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 3)
                            html.AppendLine(string.Format("{0:#,0.000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 4)
                            html.AppendLine(string.Format("{0:#,0.0000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 5)
                            html.AppendLine(string.Format("{0:#,0.00000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 6)
                            html.AppendLine(string.Format("{0:#,0.000000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 7)
                            html.AppendLine(string.Format("{0:#,0.0000000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                        if (decimalPlace[cell] == 8)
                            html.AppendLine(string.Format("{0:#,0.00000000}", ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]).ToString() + "</td>");
                    }


                    if (ramKey2Valuegz[cell].Count > 1)
                    {
                        if (ramKey2Valuegz[cell][0].ToUpper().Contains("DATE"))
                        {
                            isDateNumber = double.TryParse(ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]], out dateNumber);
                            if (isDateNumber == true)
                            {
                                if (dateNumber > 1000 && dateNumber < 401770)
                                {
                                    dateValue = DateTime.FromOADate(dateNumber);
                                    dateFormat = dateValue.ToString("dd.MMM.yyyy");
                                    if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 2]]])
                                        html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + dateFormat + "</td>");
                                    else
                                        html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" + dateFormat + "</td>");
                                }
                            }
                            else
                                if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 2]]])
                                html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] + "</td>");
                            else
                                html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] + "</td>");
                        }
                        else
                            if (line > 1 && ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] == ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 2]]])
                            html.AppendLine("<td class=\"w3-left-align w3-text-yellow\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] + "</td>");
                        else
                            html.AppendLine("<td class=\"w3-left-align w3-text-brown\">" + ramKey2Valuegz[cell][ramDetailgz[cell][distinctList2DrillDown[_drillSet[0]][line - 1]]] + "</td>");
                    }
                }

                html.AppendLine("</tr>");
                html.AppendLine(Environment.NewLine);
            }

            html.AppendLine("</tbody>");
            html.AppendLine("</table>");
            html.AppendLine("</div>");
            html.AppendLine("</output-report>");

            responseDict[requestID].html = html;

            if (requestDict[requestID].debugOutput == "Y")
            {
                if (!Directory.Exists(outputFolder + "\\" + "debug" + "\\"))
                    Directory.CreateDirectory(outputFolder + "\\" + "debug" + "\\");

                using (StreamWriter toDisk = new StreamWriter(outputFolder + "\\" + "debug" + "\\" + requestID.ToString() + "DrillDown.html"))
                {
                    toDisk.Write(responseDict[requestID].html);
                    toDisk.Close();
                }
            }
        }
      
    }
}