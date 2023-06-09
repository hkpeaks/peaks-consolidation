using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace youFast
{
    public class Json
    {   public void Json2VariableArray(string message, Dictionary<string, string> variable, Dictionary<string, List<string>> array)
        {            
            Dictionary<string, int> JsonSeparator = new Dictionary<string, int>();
            StringBuilder jsonField = new StringBuilder();
            StringBuilder jsonValue = new StringBuilder();
            JsonSeparator["curlyBracket"] = 0;
            JsonSeparator["colon"] = 0;
            JsonSeparator["comma"] = 0;
            JsonSeparator["doubleQuote"] = 0;
            JsonSeparator["squareBracket"] = 0;

            byte[] jsonByte = Encoding.ASCII.GetBytes(message);
           
            for (int i = 0; i < jsonByte.Length; i++)
            {
                if (jsonByte[i] == 123) // curlyBracket {                
                    JsonSeparator["curlyBracket"] = JsonSeparator["curlyBracket"] + 1;

                else if (jsonByte[i] == 34) // doubleQuote "               
                    JsonSeparator["doubleQuote"] = JsonSeparator["doubleQuote"] + 1;

                else if (jsonByte[i] == 58) // colon : 
                {
                    JsonSeparator["colon"] = JsonSeparator["colon"] + 1;
                    JsonSeparator["doubleQuote"] = 0;                   
                }

                else if (jsonByte[i] == 44) // comma ,                 
                    JsonSeparator["comma"] = JsonSeparator["comma"] + 1;

                else if (jsonByte[i] == 91) // squareBracket [              
                    JsonSeparator["squareBracket"] = JsonSeparator["squareBracket"] + 1;

                else if (jsonByte[i] == 32) // space
                {
                    // ignore
                }

                else if(i > 2 && jsonByte[i - 1] == 91 && jsonByte[i] == 93)
                {                   
                    JsonSeparator["squareBracket"] = 0;
                    JsonSeparator["colon"] = 0;
                    JsonSeparator["comma"] = 0;
                    jsonField.Clear();
                }

                else if (JsonSeparator["colon"] == 0 && jsonByte[i] != 93) // squareBracket ]                
                    jsonField.Append((char)(jsonByte[i]));                 
               
                else if (JsonSeparator["colon"] == 1)
                {
                    if (JsonSeparator["doubleQuote"] >= 2 && JsonSeparator["colon"] == 1 && JsonSeparator["squareBracket"] == 0)
                    {
                        JsonSeparator["doubleQuote"] = JsonSeparator["doubleQuote"] - 2;
                        JsonSeparator["colon"] = 0;
                        JsonSeparator["comma"] = 0;                       
                        variable.Add(jsonField.ToString(), jsonValue.ToString());
                        jsonField.Clear();
                        jsonValue.Clear();                      
                    }

                    if ((JsonSeparator["comma"] == 1 && JsonSeparator["colon"] == 1 && JsonSeparator["squareBracket"] == 1) || jsonByte[i] == 93)
                    {
                        JsonSeparator["comma"] = 0;
                        JsonSeparator["doubleQuote"] = 0;                      

                        if (jsonField.ToString().Trim().Count() > 0 && jsonValue.ToString().Trim().Count() > 0)
                        {
                            if (!array.ContainsKey(jsonField.ToString()))
                                array.Add(jsonField.ToString(), new List<string>());

                            array[jsonField.ToString()].Add(jsonValue.ToString());                          
                            jsonValue.Clear();

                            if (jsonByte[i] == 93)
                            {
                                JsonSeparator["squareBracket"] = 0;
                                JsonSeparator["colon"] = 0;
                                JsonSeparator["comma"] = 0;
                                jsonField.Clear();
                            }
                        }
                    }

                    if (jsonByte[i] != 93)
                    {
                        if (JsonSeparator["colon"] == 0)                      
                            jsonField.Append((char)(jsonByte[i]));                           
                        
                        else                      
                            jsonValue.Append((char)(jsonByte[i]));
                    }
                }

                else if (jsonByte[i] == 125) // curlyBracket }              
                    JsonSeparator["curlyBracket"] = JsonSeparator["curlyBracket"] - 1;
            }
        }              
        public void Json2VariableList(string message, decimal dictNo, Dictionary<string, string> variable, Dictionary<string, List<string>> array, ConcurrentDictionary<decimal, clientMachine.request> requestDict)
        {
            requestDict.TryAdd(dictNo, new clientMachine.request());

            if (variable.ContainsKey("processID"))
                requestDict[dictNo].processID = variable["processID"];

            if (variable.ContainsKey("processButton"))
                requestDict[dictNo].processButton = variable["processButton"];            

            if (variable.ContainsKey("dataset"))
                requestDict[dictNo].dataset = variable["dataset"];

            if (variable.ContainsKey("randomFilter"))
                requestDict[dictNo].randomFilter = variable["randomFilter"];

            if (variable.ContainsKey("userID"))
                requestDict[dictNo].userID = variable["userID"];

            if (variable.ContainsKey("drillType"))
                requestDict[dictNo].drillType = variable["drillType"];

            if (variable.ContainsKey("drillSet"))
            {
                bool success = decimal.TryParse(variable["drillSet"], out decimal num);
                requestDict[dictNo].drillSet = num;
            }

            if (variable.ContainsKey("drillSetHeader"))
                requestDict[dictNo].drillSetHeader = variable["drillSetHeader"];

            if (variable.ContainsKey("drillDownEventType"))
                requestDict[dictNo].drillDownEventType = variable["drillDownEventType"];

            if (variable.ContainsKey("drillSetCrosstab"))
            {
                bool success = decimal.TryParse(variable["drillSetCrosstab"], out decimal num);
                requestDict[dictNo].drillSetCrosstab = num;
            }

            if (variable.ContainsKey("importFile"))
                requestDict[dictNo].importFile = variable["importFile"];

            if (variable.ContainsKey("importType"))
                requestDict[dictNo].importType = variable["importType"];

            if (variable.ContainsKey("timeStamp"))
                requestDict[dictNo].timeStamp = variable["timeStamp"];

            if (variable.ContainsKey("debugOutput"))
                requestDict[dictNo].debugOutput = variable["debugOutput"];

            if (variable.ContainsKey("filterColumn"))
                requestDict[dictNo].filterColumn = variable["filterColumn"];

            if (variable.ContainsKey("direction"))
                requestDict[dictNo].direction = variable["direction"];

            if (variable.ContainsKey("sortingOrder"))
                requestDict[dictNo].sortingOrder = variable["sortingOrder"];

            if (variable.ContainsKey("openReport"))
                requestDict[dictNo].openReport = variable["openReport"];

            if (variable.ContainsKey("nextPageID"))
            {
                bool success = Decimal.TryParse(variable["nextPageID"], out decimal num);
                requestDict[dictNo].nextPageID = num;
            }

            if (variable.ContainsKey("cancelRequestID"))
            {
                bool success = Decimal.TryParse(variable["cancelRequestID"], out decimal num);
                requestDict[dictNo].cancelRequestID = num;
            }

            if (variable.ContainsKey("pageXlength"))
            {
                bool success = Int32.TryParse(variable["pageXlength"], out int num);
                requestDict[dictNo].pageXlength = num;
            }

            if (variable.ContainsKey("pageYlength"))
            {
                bool success = Int32.TryParse(variable["pageYlength"], out int num);
                requestDict[dictNo].pageYlength = num;
            }

            if (variable.ContainsKey("pageXlengthCrosstab"))
            {
                bool success = Int32.TryParse(variable["pageXlengthCrosstab"], out int num);
                requestDict[dictNo].pageXlengthCrosstab = num;
            }

            if (variable.ContainsKey("pageYlengthCrosstab"))
            {
                bool success = Int32.TryParse(variable["pageYlengthCrosstab"], out int num);
                requestDict[dictNo].pageYlengthCrosstab = num;
            }

            if (variable.ContainsKey("rotateDimension"))
                requestDict[dictNo].rotateDimension = variable["rotateDimension"];

            if (variable.ContainsKey("rotateDimensionFrom"))
                requestDict[dictNo].rotateDimensionFrom = variable["rotateDimensionFrom"];

            if (variable.ContainsKey("rotateDimensionTo"))
                requestDict[dictNo].rotateDimensionTo = variable["rotateDimensionTo"];

            if (variable.ContainsKey("sortXdimension"))
                requestDict[dictNo].sortXdimension = variable["sortXdimension"];

            if (variable.ContainsKey("sortYdimension"))
                requestDict[dictNo].sortYdimension = variable["sortYdimension"];

            if (variable.ContainsKey("precisionLevel"))
                requestDict[dictNo].precisionLevel = variable["precisionLevel"];

            if (variable.ContainsKey("moveColumnDirection"))
                requestDict[dictNo].moveColumnDirection = variable["moveColumnDirection"];

            if (variable.ContainsKey("moveColumnName"))
                requestDict[dictNo].moveColumnName = variable["moveColumnName"];

            if (variable.ContainsKey("addColumnType"))
                requestDict[dictNo].addColumnType = variable["addColumnType"];

            if (variable.ContainsKey("resetDimensionOrder"))
                requestDict[dictNo].resetDimensionOrder = variable["resetDimensionOrder"];

            if (variable.ContainsKey("measureType"))
                requestDict[dictNo].measureType = variable["measureType"];

            if (array.ContainsKey("column"))
                requestDict[dictNo].column = array["column"];
            else
                requestDict[dictNo].column = null;

            if (array.ContainsKey("startOption"))
                requestDict[dictNo].startOption = array["startOption"];
            else
                requestDict[dictNo].startOption = null;            

            if (array.ContainsKey("startColumnValue"))
                requestDict[dictNo].startColumnValue = array["startColumnValue"];
            else
                requestDict[dictNo].startColumnValue = null;

            if (array.ContainsKey("endOption"))
                requestDict[dictNo].endOption = array["endOption"];
            else
                requestDict[dictNo].endOption = null;

            if (array.ContainsKey("endColumnValue"))
                requestDict[dictNo].endColumnValue = array["endColumnValue"];
            else
                requestDict[dictNo].endColumnValue = null;

            if (array.ContainsKey("distinctDimension"))
                requestDict[dictNo].distinctDimension = array["distinctDimension"];
            else
                requestDict[dictNo].distinctDimension = null;

            if (array.ContainsKey("distinctOrder"))
                requestDict[dictNo].distinctOrder = array["distinctOrder"];
            else
                requestDict[dictNo].distinctOrder = null;

            if (array.ContainsKey("crosstabDimension"))
                requestDict[dictNo].crosstabDimension = array["crosstabDimension"];
            else
                requestDict[dictNo].crosstabDimension = null;

            if (array.ContainsKey("crosstabOrder"))
                requestDict[dictNo].crosstabOrder = array["crosstabOrder"];
            else
                requestDict[dictNo].crosstabOrder = null;

            if (array.ContainsKey("measurement"))
                requestDict[dictNo].measurement = array["measurement"];
            else
                requestDict[dictNo].measurement = null;
        }
    }
}
