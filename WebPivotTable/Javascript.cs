using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace youFast
{
    class Javascript
    {
        public void distinctDesktopHtml()
        {            
            StringBuilder html = new StringBuilder();
            string processButton;
            string getElement = "common";

            createWebLayout();

            startWebsocket();

            // SEND MESSAGE BY FUNCTION
            // pre-confirm process event            
            function_css();
            function_template();
            function_displayFilterDropDownList();
            function_clearCurrentReport();            
            function_scrollTableStoreWindows();
            function_selectFromFileTable();
            function_refreshSelectFileList();
            function_addFilter();
            function_addDisplayColumn();
            function_changeMeasureType();
            function_deleteSelectionCriteria();

            // confirm process event   
            function_runFactoryData();
            function_drillRow();
            function_getSelectionCriteriaElement();
            function_moveColumn();
            function_getSelectionCriteriaElement();
            function_selectFile();
            function_dragDropFile();
            function_drillRowBySingleDimension();
            function_getSelectionCriteriaElement();

            // interim event
            function_sortAscending();
            function_sortDescending();
            function_moveToCrosstab();
            function_removeFromCrosstab();
            function_rotatePairDimension();
            function_rotatePairDimension2Drilldown();
            function_rotatePairDimensionCrosstab();

            // final event
            function_openReportByWindows();
            function_openReportByWindowsDrill();
            function_keyboard();
            function_pageEndDown();
            function_pageEndDownDrill();
            function_pageEndRight();
            function_pageHome();
            function_pageHomeDrill();
            function_drillDown();
            function_changeDrillDownEvent();
            function_pageLeft();
            function_pageUp();
            function_pageUpDrill();
            function_pageDown();
            function_pageDownDrill();
            function_pageRight();
            function_nextPrecision();

            // RECEIVE MESSAGE BY CONDITION
            // reveive HTML and copy it to DOM by conditions
            startOnmessage();
            condition_css();
            condition_template();
            condition_displayFilterDropDownList();
            condition_refreshSelectFileList();
            condition_selectFile();
            condition_addFilter();
            condition_repalceFilter();
            condition_addDisplayColumn();
            condition_addCrosstab();
            condition_addMeasurement();
            condition_outputReport();
            condition_drillDownReport();
            condition_nextPageID();
            asyncfunction_followUpNextPageID();
            endOnmessage();

            endHTML();
            outputDebug();

            void createWebLayout()
            {
                html.AppendLine("<!DOCTYPE html>");
                html.AppendLine("<head>");
                html.AppendLine("<style id=\"css\">");
                html.AppendLine("</style>");
                html.AppendLine("<meta charset=\"UTF-8\">");
                html.AppendLine("<title>youFast</title>");
                html.AppendLine("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
                html.AppendLine("<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/icon?family=Material+Icons\">");
                html.AppendLine("</head>");
                html.AppendLine("<body>");

                // append and overwrite html
                html.AppendLine("<div class=\"w3-container w3-white\">");
                html.AppendLine("<p></p>");
                html.AppendLine("<template id=\"tmpl\">");               
                html.AppendLine("</template>");
                html.AppendLine("<overwrite id=\"demo\"></overwrite>");
                html.AppendLine("<overwrite id=\"addReport\"></overwrite>");
                html.AppendLine("<overwrite id=\"drillDownReport\"></overwrite>");
                html.AppendLine("<append display id=\"displayFilterDropDownList\"></append>");
                html.AppendLine("<overwrite display id=\"replaceFilter\"></overwrite>");
                html.AppendLine("<append display id=\"addFilter\"></append>");
                html.AppendLine("<p></p>");
                html.AppendLine("<overwrite display id=\"addDisplayColumn\"></overwrite>");
                html.AppendLine("<overwrite display id=\"addMeasurement\"></overwrite>");
                html.AppendLine("<overwrite display id=\"addCrosstab\"></overwrite>");
                html.AppendLine("</div>");
                html.AppendLine("<div class=\"w3-container w3-white\">");
                html.AppendLine("<overwrite display id=\"selectFile\"></overwrite>");
                html.AppendLine("</div>");
                html.AppendLine("<p></p>");
                html.AppendLine("");

                //javascript
                html.AppendLine("<script>");              
                html.AppendLine("let requestPool = new Array();");
                html.AppendLine("let rotateDimension = new Array();");
                html.AppendLine("let followup = new Array();");
                html.AppendLine("let reportName = \"\";");
                html.AppendLine("let tableStoreWindowsYscroll = 0;");
                html.AppendLine("let dragFile = 1;");
                html.AppendLine("let measureType = \"sum\";");
            }

            void startWebsocket()
            { 
                // generate select filter upon connection
                // html.AppendLine("wsUri = \"ws://192.168.1.195:5000/\",");
                html.AppendLine("wsUri = \"ws://127.0.0.1:5000/\",");
                html.AppendLine("websocket = new WebSocket(wsUri);");
                html.AppendLine("");
                html.AppendLine("websocket.onopen = function (e)");
                html.AppendLine("{");
                html.AppendLine("css();");
                //html.AppendLine("template();");
                html.AppendLine("displayFilterDropDownList();");
                html.AppendLine("runFactoryData();");
                html.AppendLine("};");
                html.AppendLine("");
                html.AppendLine("websocket.onclose = function (e)");
                html.AppendLine("{");
                html.AppendLine("");
                html.AppendLine("};");
                html.AppendLine("");

                //onerror
                html.AppendLine("websocket.onerror = function (e)");
                html.AppendLine("{");
                html.AppendLine("};");
                html.AppendLine("");
            }

            // pre-confirm process event

            void function_css()
            {
                html.AppendLine("function css()");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"css\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_template()
            {
                html.AppendLine("function template()");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"template\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_displayFilterDropDownList()
            {
                html.AppendLine("function displayFilterDropDownList()");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"selectFile\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_clearCurrentReport()
            {
                html.AppendLine("function clearCurrentReport()");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"displayFilterDropDownList\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addFilter\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addDisplayColumn\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addCrosstab\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addMeasurement\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"addReport\").innerHTML =  \"\";");
                html.AppendLine("};");
                html.AppendLine("");
            }           

            void function_scrollTableStoreWindows()
            {
                html.AppendLine("function scrollTableStoreWindows()");
                html.AppendLine("{");
                html.AppendLine("var position = document.getElementById(\"rightTable\");");
                html.AppendLine("var x = position.scrollLeft;");
                html.AppendLine("tableStoreWindowsYscroll = position.scrollTop;");
                //html.AppendLine("console.log(x);");
                //html.AppendLine("console.log(tableStoreWindowsYscroll);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_selectFromFileTable()
            {
                html.AppendLine("function selectFromFileTable(data)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"displayFilterDropDownList\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addFilter\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addDisplayColumn\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addCrosstab\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addMeasurement\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"addReport\").innerHTML =  \"\";");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"importSelectedFile\",");
                html.AppendLine("\"randomFilter\": \"Y\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"importFile\": data.id");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_refreshSelectFileList()
            {
                html.AppendLine("function refreshSelectFileList(data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"refreshSelectFileList\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"importFile\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_addFilter()
            {
                html.AppendLine("function addFilter (data)");
                html.AppendLine("{");
                html.AppendLine("if(data.value != \"Add Filter\")");
                html.AppendLine("{");
                html.AppendLine("");
                html.AppendLine("var x = document.getElementById(\"distinct0\");");
                html.AppendLine("if(x == null)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"addFilter\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"importFile\" : reportName,");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"filterColumn\": data.value");
                html.AppendLine("};");
                html.AppendLine("}");
                html.AppendLine("else");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"addFilter\",");
                html.AppendLine("\"importFile\" : reportName,");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"filterColumn\": data.value");
                html.AppendLine("};");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("data.value = \"Add Filter\";");
                html.AppendLine("");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_addDisplayColumn()
            {
                html.AppendLine("function addDisplayColumn (data)");
                html.AppendLine("{");
                html.AppendLine("if(data.value != \"Add Distinct Dimension\")");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"addDisplayColumn\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"importFile\" : reportName,");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"addColumnType\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("data.value = \"Add Distinct Dimension\";");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_changeMeasureType()
            {
                html.AppendLine("function changeMeasureType (data)");
                html.AppendLine("{");
                html.AppendLine("measureType = data.value;");
                html.AppendLine("};");
                html.AppendLine("");
                html.AppendLine("");
            }

            void function_deleteSelectionCriteria()
            {
                html.AppendLine("function deleteSelectionCriteria(data)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(data.value).innerHTML = \"\";");
                html.AppendLine("};");
                html.AppendLine("");
            }

            // confirm process event
            
            void function_runFactoryData()
            {
                html.AppendLine("function runFactoryData()");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"importSelectedFile\",");                
                html.AppendLine("\"randomFilter\": \"Y\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"importType\": \"null\",");                
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"importFile\": \"factoryData.csv\"");               
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("{");
                html.AppendLine("process(json);");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("async function process(json)");
                html.AppendLine("{");
                html.AppendLine("for (let i = 1; i <= 1 ; i++)");
                html.AppendLine("{");
                html.AppendLine("await sleep(1);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void function_drillRow()
            {
                html.AppendLine("function drillRow()");
                html.AppendLine("{");
                html.AppendLine("dragFile = 1;");
                processButton = "drillRow";
            }

            void function_moveColumn()
            {
                html.AppendLine("function moveColumn()");
                html.AppendLine("{");
                html.AppendLine("dragFile = 0;");
                processButton = "moveColumn";
            }

            void function_drillRowBySingleDimension()
            {
                html.AppendLine("function drillRowBySingleDimension(data)");
                html.AppendLine("{");
                html.AppendLine("let elem = document.createElement('div');");
                html.AppendLine("elem.append(tmpl.content.cloneNode(true));");
                html.AppendLine("document.body.append(elem);");
                html.AppendLine("dragFile = 0;");
                processButton = "drillRow";
                getElement = "drillRowBySingleDimension";               
            }

            void function_getSelectionCriteriaElement()
            {  
                html.AppendLine("let element = document.querySelectorAll ('[name=\"column0\"]');");
                html.AppendLine("var column = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("column[i] = document.getElementById(element[i].id).value;");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"column1\"]');");
                html.AppendLine("var startOption = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("startOption[i] = document.getElementById(element[i].id).value;");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"column2\"]');");
                html.AppendLine("var startColumnValue = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("{");
                html.AppendLine("startColumnValue[i] = document.getElementById(element[i].id).value.split(\":\").join(\";\");");
                html.AppendLine("startColumnValue[i] = startColumnValue[i].split(\"/\").join(\"|\");");
                html.AppendLine("if(startColumnValue[i].length == 0)");
                html.AppendLine("startColumnValue[i] = \"blankValue\";");
                html.AppendLine("}");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"column3\"]');");
                html.AppendLine("var endOption = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("endOption[i] = document.getElementById(element[i].id).value;");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"column4\"]');");
                html.AppendLine("var endColumnValue = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("{");
                html.AppendLine("endColumnValue[i] = document.getElementById(element[i].id).value.split(\":\").join(\";\");");
                html.AppendLine("endColumnValue[i] = endColumnValue[i].split(\"/\").join(\"|\");");
                html.AppendLine("if(endColumnValue[i].length == 0)");
                html.AppendLine("endColumnValue[i] = \"blankValue\";");
                html.AppendLine("}");
                html.AppendLine("");

                if (getElement != "common")
                {
                    html.AppendLine("var distinctDimension = new Array();");
                    html.AppendLine("distinctDimension[0] = data.value;");
                    html.AppendLine("");

                    html.AppendLine("var distinctOrder = new Array();");
                    html.AppendLine("distinctOrder[0] = data.value;");
                    html.AppendLine("");
                }
                else
                {
                    html.AppendLine("element = document.querySelectorAll ('[name=\"distinctDimension\"]');");
                    html.AppendLine("var box = new Array();");
                    html.AppendLine("var dimension = 0;");
                    html.AppendLine("var distinctDimension = new Array();");
                    html.AppendLine("for (i = 0; i < element.length; i++)");
                    html.AppendLine("{");
                    html.AppendLine("box[i] = document.getElementById(element[i].id);");
                    html.AppendLine("if (box[i].checked == true)");
                    html.AppendLine("{");
                    html.AppendLine("distinctDimension[dimension] = element[i].value;");
                    html.AppendLine("dimension++;");
                    html.AppendLine("}");
                    html.AppendLine("}");
                    html.AppendLine("");

                    html.AppendLine("element = document.querySelectorAll ('[name=\"distinctOrder\"]');");
                    html.AppendLine("var box = new Array();");
                    html.AppendLine("var order = 0;");
                    html.AppendLine("var distinctOrder = new Array();");
                    html.AppendLine("for (i = 0; i < element.length; i++)");
                    html.AppendLine("{");
                    html.AppendLine("box[i] = document.getElementById(element[i].id);");
                    html.AppendLine("distinctOrder[order] = element[i].id;");
                    html.AppendLine("order++;");
                    html.AppendLine("}");
                    html.AppendLine("");
                }

                html.AppendLine("element = document.querySelectorAll ('[name=\"crosstabDimension\"]');");
                html.AppendLine("var box = new Array();");
                html.AppendLine("var dimension = 0;");
                html.AppendLine("var crosstabDimension = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("{");
                html.AppendLine("box[i] = document.getElementById(element[i].id);");
                html.AppendLine("if (box[i].checked == true)");
                html.AppendLine("{");
                html.AppendLine("crosstabDimension[dimension] = element[i].value;");
                html.AppendLine("dimension++;");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"crosstabOrder\"]');");
                html.AppendLine("var box = new Array();");
                html.AppendLine("var order = 0;");
                html.AppendLine("var crosstabOrder = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("{");
                html.AppendLine("box[i] = document.getElementById(element[i].id);");
                html.AppendLine("crosstabOrder[order] = element[i].id;");
                html.AppendLine("order++;");
                html.AppendLine("}");
                html.AppendLine("");

                html.AppendLine("element = document.querySelectorAll ('[name=\"measurement\"]');");
                html.AppendLine("var box = new Array();");
                html.AppendLine("var dimension = 0;");
                html.AppendLine("var measurement = new Array();");
                html.AppendLine("for (i = 0; i < element.length; i++)");
                html.AppendLine("{");
                html.AppendLine("box[i] = document.getElementById(element[i].id);");
                html.AppendLine("if (box[i].checked == true)");
                html.AppendLine("{");
                html.AppendLine("measurement[dimension] = element[i].value;");
                html.AppendLine("dimension++;");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"runReport\",");
                html.AppendLine("\"processButton\":" + "\"" + processButton + "\"" + ",");
                html.AppendLine("\"randomFilter\": \"N\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"pageXlength\": \"20\",");
                html.AppendLine("\"pageYlength\": \"16\",");
                html.AppendLine("\"pageXlengthCrosstab\": \"20\",");
                html.AppendLine("\"pageYlengthCrosstab\": \"33\",");
                html.AppendLine("\"sortingOrder\": \"sortAscending\",");
                html.AppendLine("\"sortXdimension\":\"A\",");
                html.AppendLine("\"sortYdimension\":\"A\",");
                html.AppendLine("\"precisionLevel\" : \"Dollar\",");
                html.AppendLine("\"importFile\" : reportName,");
                html.AppendLine("\"measureType\":measureType,");
                html.AppendLine("\"column\":column,");
                html.AppendLine("\"startOption\":startOption,");
                html.AppendLine("\"startColumnValue\":startColumnValue,");
                html.AppendLine("\"endOption\":endOption,");
                html.AppendLine("\"endColumnValue\":endColumnValue,");
                html.AppendLine("\"distinctDimension\": distinctDimension,");
                html.AppendLine("\"distinctOrder\": distinctOrder,");
                html.AppendLine("\"crosstabDimension\": crosstabDimension,");
                html.AppendLine("\"crosstabOrder\": crosstabOrder,");
                html.AppendLine("\"measurement\": measurement,");
                html.AppendLine("\"cancelRequestID\":  document.getElementById(\"submitForm\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("{");
                html.AppendLine("process(json);");

                if(processButton == "drillRow")
                { 
                    html.AppendLine("document.getElementById(\"exe\").outerHTML = document.getElementById(\"exe\").outerHTML.replace(\"green\", \"red\");");
                    html.AppendLine("document.getElementById(\"exe\").innerHTML = \"Processing\";");
                }
                else
                {
                    html.AppendLine("document.getElementById(\"exe2\").outerHTML = document.getElementById(\"exe2\").outerHTML.replace(\"green\", \"red\");");
                    html.AppendLine("document.getElementById(\"exe2\").innerHTML = \"Processing\";");
                }

                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("async function process(json)");
                html.AppendLine("{");
                html.AppendLine("for (let i = 1; i <= 1 ; i++)");
                html.AppendLine("{");
                html.AppendLine("await sleep(1);");
                html.AppendLine("websocket.send(json);");                
                html.AppendLine("}");
                html.AppendLine("}");             
                html.AppendLine("");
            }           

            void function_selectFile()
            {
                html.AppendLine("function selectFile(data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"importSelectedFile\",");
                html.AppendLine("\"randomFilter\": \"Y\",");
                html.AppendLine("\"userID\": \"system\",");                
                html.AppendLine("\"importType\": \"null\" ,");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"importFile\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_dragDropFile()
            {
                html.AppendLine("document.ondrop = function(event)");
                html.AppendLine("{");
                html.AppendLine("if(dragFile == 1)");
                html.AppendLine("{");                
                html.AppendLine("var d = new Date();");
                html.AppendLine("var yy = d.getFullYear();");
                html.AppendLine("var MM = d.getMonth();");
                html.AppendLine("if (MM < 10) MM = \"0\" + MM;");
                html.AppendLine("var dd = d.getDay();");
                html.AppendLine("if (dd < 10) dd = \"0\" + dd;");
                html.AppendLine("var hh = d.getHours();");
                html.AppendLine("if (hh < 10) hh = \"0\" + hh;");
                html.AppendLine("var mm = d.getMinutes();");
                html.AppendLine("if (mm < 10) mm = \"0\" + mm;");
                html.AppendLine("var ss = d.getSeconds();");
                html.AppendLine("if (ss < 10) ss = \"0\" + ss;");
                html.AppendLine("var ms = d.getMilliseconds();");
                html.AppendLine("if (ms < 1000) ms = \"0\" + ms;");
                html.AppendLine("if (ms < 100) ms = \"0\" + ms;");
                html.AppendLine("if (ms < 10) ms = \"0\" + ms;");
                html.AppendLine("var timeStamp = yy + MM + dd + \"-\" + hh + mm + ss + \"-\" + ms;");
                html.AppendLine("");
                html.AppendLine("");
                html.AppendLine("event.preventDefault();");
                html.AppendLine("var i;");
                html.AppendLine("var fileCount = event.dataTransfer.files.length;");
                html.AppendLine("for (i = 0; i < fileCount; i++)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"importSelectedFile\",");
                html.AppendLine("\"randomFilter\": \"Y\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"timeStamp\": timeStamp ,");
                html.AppendLine("\"importType\": \"overwrite\" ,");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"resetDimensionOrder\": \"Y\",");
                html.AppendLine("\"importFile\": event.dataTransfer.files[i].name");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("}");                
                html.AppendLine("document.ondragover = function(event)");
                html.AppendLine("{");
                html.AppendLine("if(dragFile == 1)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"demo\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"displayFilterDropDownList\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addFilter\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addDisplayColumn\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addCrosstab\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addMeasurement\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"addReport\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"drillDownReport\").innerHTML =  \"\";");               
                html.AppendLine("event.preventDefault();");
                html.AppendLine("}");
                html.AppendLine("}");
            }

            // interim event

            void function_sortAscending()
            { 
                html.AppendLine("function sortAscending (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"dataSorting\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"N\",");
                html.AppendLine("\"sortingOrder\": \"sortDescending\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_sortDescending()
            { 
                html.AppendLine("function sortDescending (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"dataSorting\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"N\",");
                html.AppendLine("\"sortingOrder\": \"sortAscending\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_moveToCrosstab()
            { 
                html.AppendLine("function moveToCrosstab (data)");
                html.AppendLine("{");
                html.AppendLine("dragFile = 0;");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"moveToCrosstab\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"processButton\": \"moveColumn\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"N\",");
                html.AppendLine("\"moveColumnDirection\": \"Y2X\",");
                html.AppendLine("\"moveColumnName\":  data.id,");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_removeFromCrosstab()
            { 
                html.AppendLine("function removeFromCrosstab (data)");
                html.AppendLine("{");
                html.AppendLine("dragFile = 0;");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"removeFromCrosstab\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"processButton\": \"moveColumn\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"N\",");
                html.AppendLine("\"moveColumnDirection\": \"X2Y\",");
                html.AppendLine("\"moveColumnName\":  data.id,");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_rotatePairDimension()
            {
                html.AppendLine("function rotatePairDimension(data)");
                html.AppendLine("{");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(rotateDimension.length < 2)");
                html.AppendLine("{");
                html.AppendLine("if(!rotateDimension.includes(data.id))");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.push(data.id);");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("if(rotateDimension.length > 2)");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("if(rotateDimension.length == 2)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"rotateDimension\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"Y\",");
                html.AppendLine("\"rotateDimensionFrom\": rotateDimension[0],");
                html.AppendLine("\"rotateDimensionTo\":  rotateDimension[1],");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void function_rotatePairDimension2Drilldown()
            {
                html.AppendLine("function rotatePairDimension2Drilldown(data)");
                html.AppendLine("{");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(rotateDimension.length < 2)");
                html.AppendLine("{");
                html.AppendLine("if(!rotateDimension.includes(data.id))");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.push(data.id);");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("if(rotateDimension.length > 2)");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("if(rotateDimension.length == 2)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"rotateDimension\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"Y\",");
                html.AppendLine("\"rotateDimensionFrom\": rotateDimension[0],");
                html.AppendLine("\"rotateDimensionTo\":  rotateDimension[1],");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void function_rotatePairDimensionCrosstab()
            { 
                html.AppendLine("function rotatePairDimensionCrosstab(data)");
                html.AppendLine("{");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(rotateDimension.length < 2)");
                html.AppendLine("{");
                html.AppendLine("if(!rotateDimension.includes(data.id))");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.push(data.id);");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("if(rotateDimension.length > 2)");
                html.AppendLine("{");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("if(rotateDimension.length == 2)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"rotateDimensionCrosstab\",");
                html.AppendLine("\"processButton\": \"moveColumn\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"rotateDimension\": \"Y\",");
                html.AppendLine("\"rotateDimensionFrom\": rotateDimension[0],");
                html.AppendLine("\"rotateDimensionTo\":  rotateDimension[1],");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("rotateDimension.length = 0;");
                html.AppendLine("data.onclick = \"\";");
                html.AppendLine("data.ondragend= \"\";");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }           

            void function_openReportByWindows()
            { 
                html.AppendLine("function openReportByWindows (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"openReportByWindows\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"openReport\": \"Y\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_openReportByWindowsDrill()
            { 
                html.AppendLine("function openReportByWindowsDrill (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"openReportByWindows\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"openReport\": \"Y\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_keyboard()
            { 
                html.AppendLine("document.onkeydown = function(event) {");
                html.AppendLine("switch (event.keyCode) {");
                html.AppendLine("case 35:");
                html.AppendLine("var object1 =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageEndDown\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"pageEndDown\").value");
                html.AppendLine("};");
                html.AppendLine("var json1 = JSON.stringify(object1);");
                html.AppendLine("websocket.send(json1);");
                html.AppendLine("for (let step = 0; step < 1000; step++) {");
                html.AppendLine("console.log('sleep');");
                html.AppendLine("}");
                html.AppendLine("");
                html.AppendLine("var object2 =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageEndRight\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"pageEndRight\").value");
                html.AppendLine("};");
                html.AppendLine("var json2 = JSON.stringify(object2);");
                html.AppendLine("websocket.send(json2);");
                html.AppendLine("");
                html.AppendLine("break;");
                html.AppendLine("case 36:");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageHome\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"pageHome\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("break;");
                html.AppendLine("case 37:");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Left\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"left\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("break;");
                html.AppendLine("case 38:");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Up\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"up\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("break;");
                html.AppendLine("case 39:");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Right\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"right\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("break;");
                html.AppendLine("case 40:");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Down\",");
                html.AppendLine("\"nextPageID\": document.getElementById(\"down\").value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageEndDown()
            { 
                html.AppendLine("function pageEndDown (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageEndDown\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageEndDownDrill()
            { 
                html.AppendLine("function pageEndDownDrill (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageEndDown\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageEndRight()
            { 
                html.AppendLine("function pageEndRight (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageEndRight\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageHome()
            { 
                html.AppendLine("function pageHome (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageHome\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageHomeDrill()
            { 
                html.AppendLine("function pageHomeDrill (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"pageHome\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_drillDown()
            { 
                html.AppendLine("function drillDown (data)");
                html.AppendLine("{");
                html.AppendLine("var dataLen = data.id.length + 1 - data.id.search(\"~\");");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"drillDown\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": data.id.substring(0, data.id.search(\"~\")),");
                html.AppendLine("\"drillSetHeader\": data.headers,");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"direction\": \"null\",");
                html.AppendLine("\"nextPageID\": data.id.substring(data.id.search(\"~\") + 1, data.id.length),");
                html.AppendLine("\"pageXlength\": \"20\",");
                html.AppendLine("\"pageYlength\": \"20\",");
                html.AppendLine("\"drillDownEventType\": \"Null\",");
                html.AppendLine("\"importFile\" : reportName,");
                html.AppendLine("};");
                html.AppendLine("if(followup.length == 0);");
                html.AppendLine("{");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("console.log(data.id);");                
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_changeDrillDownEvent()
            {
                html.AppendLine("function changeDrillDownEvent (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"changeDrillDownEvent\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"nextPageID\": data.value,");
                html.AppendLine("\"drillDownEventType\": \"Next\"");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageLeft()
            { 
                html.AppendLine("function pageLeft (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Left\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageUp()
            { 
                html.AppendLine("function pageUp (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Up\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageUpDrill()
            { 
                html.AppendLine("function pageUpDrill (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Up\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageDown()
            { 
                html.AppendLine("function pageDown (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Down\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageDownDrill()
            { 
                html.AppendLine("function pageDownDrill (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"processButton\": \"drillRow\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"set\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Down\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_pageRight()
            { 
                html.AppendLine("function pageRight (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"pageMove\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"direction\": \"Right\",");
                html.AppendLine("\"nextPageID\": data.value");
                html.AppendLine("};");
                html.AppendLine("");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }

            void function_nextPrecision()
            { 
                html.AppendLine("function nextPrecision (data)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"nextPrecision\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"drillType\": \"null\",");
                html.AppendLine("\"drillSet\": \"0\",");
                html.AppendLine("\"nextPageID\": data.value,");
                html.AppendLine("\"precisionLevel\": \"Next\"");
                html.AppendLine("};");
                html.AppendLine("var json = JSON.stringify(object);");
                html.AppendLine("websocket.send(json);");
                html.AppendLine("};");
                html.AppendLine("");
            }           

            // reveive HTML and copy it to DOM by conditions

            void startOnmessage()
            {
                html.AppendLine("websocket.onmessage = function (e)");
                html.AppendLine("{");
            }

            void condition_css()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"{\") == true)");
                html.AppendLine("document.getElementById(\"css\").innerHTML = e.data;");
                html.AppendLine("");
            }

            void condition_template()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"script\") == true)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"tmpl\").innerHTML = e.data;");
               // html.AppendLine("console.log(e.data);");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_displayFilterDropDownList()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"displayFilterDropDownList\") == true)");
                html.AppendLine("{");
                html.AppendLine("var para = document.createElement(\"socket\");");
                html.AppendLine("para.innerHTML = e.data;");
                html.AppendLine("document.getElementById(\"displayFilterDropDownList\").appendChild(para);");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_refreshSelectFileList()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"refreshSelectFileList\") == true)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"selectFile\").innerHTML = \"\";");
                html.AppendLine("var current = document.getElementById(\"selectFile\").innerHTML;");
                html.AppendLine("if(current.substring(0, 100).includes(\"addFile\") == false)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"selectFile\").innerHTML = e.data;");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_selectFile()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"addDataFile\") == true)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"displayFilterDropDownList\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addFilter\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addDisplayColumn\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addCrosstab\").innerHTML = \"\";");
                html.AppendLine("document.getElementById(\"addMeasurement\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"addReport\").innerHTML =  \"\";");
                html.AppendLine("document.getElementById(\"drillDownReport\").innerHTML =  \"\";");
                html.AppendLine("var current = document.getElementById(\"selectFile\").innerHTML;");
                html.AppendLine("if(current.substring(0, 100).includes(\"addFile\") == false)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"selectFile\").innerHTML = e.data;");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_addFilter()
            {
                html.AppendLine("if(e.data.substring(0, 40).includes(\"addFilter\") == true)");
                html.AppendLine("{");
                html.AppendLine("var para = document.createElement(\"socket\");");
                html.AppendLine("para.innerHTML = e.data;");
                html.AppendLine("document.getElementById(\"addFilter\").appendChild(para);");
                html.AppendLine("document.getElementById(\"exe\").innerHTML = \"Drill Row\";");
                html.AppendLine("document.getElementById(\"exe2\").innerHTML = \"Move Column\";");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_repalceFilter()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"replaceFilter\") == true)");
                html.AppendLine("document.getElementById(\"replaceFilter\").innerHTML = e.data;");
                html.AppendLine("");
            }

            void condition_addDisplayColumn()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"addDisplayColumn\") == true)");
                html.AppendLine("document.getElementById(\"addDisplayColumn\").innerHTML = e.data;");
                html.AppendLine("");
            }

            void condition_addCrosstab()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"addCrosstab\") == true)");
                html.AppendLine("{");
                html.AppendLine("// document.getElementById(\"addCrosstab\").innerHTML = e.data;");
                html.AppendLine("//var str = e.data;");
                html.AppendLine("// var res = str.replace(\"#\", \" \");");
                html.AppendLine("document.getElementById(\"addCrosstab\").innerHTML = e.data;");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_addMeasurement()
            {
                html.AppendLine("if(e.data.substring(0, 50).includes(\"addMeasurement\") == true)");
                html.AppendLine("document.getElementById(\"addMeasurement\").innerHTML = e.data;");
                html.AppendLine("");
            }

            void condition_outputReport()
            {
                html.AppendLine("if(e.data.substring(0, 20).includes(\"output-report id\") == true) // receive report to capture requestID & remove the request id from array");
                html.AppendLine("{");                
                html.AppendLine("document.getElementById(\"addReport\").innerHTML = e.data; // replace data to display report");
                html.AppendLine("document.getElementById(\"drillDownReport\").innerHTML = \"\"; // replace data to null report");
                html.AppendLine("var position = document.getElementById(\"rightTable\");");
                html.AppendLine("if(position != null)");
                html.AppendLine("{");
                html.AppendLine("position.scrollTop = tableStoreWindowsYscroll ;");
                html.AppendLine("}");
                html.AppendLine("reportName = e.data.substring((e.data.search(\"#fast=\") + 6), (e.data.search(\">\") - e.data.search(\"reportname\") - 1))");
                html.AppendLine("let returnElement = document.querySelectorAll ('[name=\"response\"]');");
                html.AppendLine("var response = new Array();");
                html.AppendLine("for (i = 0; i < returnElement.length; i++)");
                html.AppendLine("response[i] = document.getElementById(returnElement[i].id).value;");
                html.AppendLine("");
                html.AppendLine("let index = requestPool.findIndex(request => request === returnElement[0].id);");
                html.AppendLine("document.getElementById(\"submitForm\").value = returnElement[0].id; // add request to submitForm button");
                html.AppendLine("let removedRequestID = requestPool.splice(index, 1); // remove requestID after receiving report ==> stop sending same requestID");
                html.AppendLine("if(document.getElementById(\"exe\") != null)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"exe\").outerHTML = document.getElementById(\"exe\").outerHTML.replace(\"red\", \"green\");");
                html.AppendLine("document.getElementById(\"exe\").innerHTML = \"Drill Row\";");
                html.AppendLine("}");
                html.AppendLine("if(document.getElementById(\"exe2\") != null)");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"exe2\").outerHTML = document.getElementById(\"exe2\").outerHTML.replace(\"red\", \"green\");");
                html.AppendLine("document.getElementById(\"exe2\").innerHTML = \"Move Column\";");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_drillDownReport()
            {
                html.AppendLine("if(e.data.substring(0, 30).includes(\"output-drillDownReport id\") == true) // receive report to capture requestID & remove the request id from array");
                html.AppendLine("{");
                html.AppendLine("document.getElementById(\"drillDownReport\").innerHTML = e.data; // replace data to display report");
                html.AppendLine("reportName = e.data.substring((e.data.search(\"#fast=\") + 6), (e.data.search(\">\") - e.data.search(\"reportname\") - 1))");
                html.AppendLine("let returnElement = document.querySelectorAll ('[name=\"response\"]');");
                html.AppendLine("var response = new Array();");
                html.AppendLine("for (i = 0; i < returnElement.length; i++)");
                html.AppendLine("response[i] = document.getElementById(returnElement[i].id).value;");
                html.AppendLine("");
                html.AppendLine("let index = requestPool.findIndex(request => request === returnElement[0].id);");
                html.AppendLine("document.getElementById(\"submitForm\").value = returnElement[0].id; // add request to submitForm button");
                html.AppendLine("let removedRequestID = requestPool.splice(index, 1); // remove requestID after receiving report ==> stop sending same requestID");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void condition_nextPageID()
            {
                html.AppendLine("if(e.data.substring(0, 20).includes(\"nextPageID\") == true) // server confirm client request and to provide request id and notify processing status");
                html.AppendLine("{");
                html.AppendLine("var n = e.data.search(\"RequestID\");");
                html.AppendLine("var m = e.data.search(\"Processing\");");
                html.AppendLine("var requestID = e.data.substring(11, 40).trim();");
                html.AppendLine("requestPool.push(requestID);");
                html.AppendLine("// console.log(\"Current requestID \" + requestPool[requestPool.length - 1]);");
                html.AppendLine("followUpNextPageID(requestID);");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void asyncfunction_followUpNextPageID()
            {
                // follow up request
                html.AppendLine("async function followUpNextPageID(requestID)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"receiveReport\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"requestID\" : requestID");
                html.AppendLine("};");
                html.AppendLine("");
                html.AppendLine("var json = JSON.stringify(object);");

                html.AppendLine("for (let i = 1; i <= 10 ; i++)");
                html.AppendLine("{");
                html.AppendLine("followup.push(i);");
                html.AppendLine("if(requestPool.findIndex(request => request === requestID) < 0)  // If no element exists -1 is returned");
                html.AppendLine("{");
                html.AppendLine("followup.length = 0;");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("await sleep(1);");
                html.AppendLine("websocket.send(requestID);");
                html.AppendLine("}");
                html.AppendLine("for (let i = 1; i <= 2 ; i++)");
                html.AppendLine("{");
                html.AppendLine("followup.push(i);");
                html.AppendLine("if(requestPool.findIndex(request => request === requestID) < 0)  // If no element exists -1 is returned");
                html.AppendLine("{");
                html.AppendLine("followup.length = 0;");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("await sleep(500);");
                html.AppendLine("websocket.send(requestID);");
                html.AppendLine("}");
                html.AppendLine("}");
                html.AppendLine("");

                // condition = RequestID
                html.AppendLine("if(e.data.substring(0, 150).includes(\"RequestID\") == true) // server confirm client request and to provide request id and notify processing status");
                html.AppendLine("{");
                html.AppendLine("var n = e.data.search(\"RequestID\");");
                html.AppendLine("var m = e.data.search(\"Processing\");");
                html.AppendLine("var requestID = e.data.substring((n+11), (m-3)).trim();");
                html.AppendLine("requestPool.push(requestID);");
                html.AppendLine("followUpRequest(requestID);");
                html.AppendLine("}");
                html.AppendLine("};");
                html.AppendLine("");

                // sleep function
                html.AppendLine("function sleep(ms)");
                html.AppendLine("{");
                html.AppendLine("return new Promise(resolve => setTimeout(resolve, ms));");
                html.AppendLine("}");
                html.AppendLine("");

                // follow up request
                html.AppendLine("async function followUpRequest(requestID)");
                html.AppendLine("{");
                html.AppendLine("var object =");
                html.AppendLine("{");
                html.AppendLine("\"processID\": \"receiveReport\",");
                html.AppendLine("\"userID\": \"system\",");
                html.AppendLine("\"debugOutput\": \"N\",");
                html.AppendLine("\"requestID\" : requestID");
                html.AppendLine("};");
                html.AppendLine("");
                html.AppendLine("var json = JSON.stringify(object);");

                html.AppendLine("for (let i = 1; i <= 10 ; i++)");
                html.AppendLine("{");
                html.AppendLine("followup.push(i);");
                html.AppendLine("if(requestPool.findIndex(request => request === requestID) < 0)  // If no element exists -1 is returned");
                html.AppendLine("{");
                html.AppendLine("followup.length = 0;");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("await sleep(1);");
                html.AppendLine("websocket.send(requestID);");
                html.AppendLine("}");

                html.AppendLine("for (let i = 1; i <= 100 ; i++)");
                html.AppendLine("{");
                html.AppendLine("followup.push(i);");
                html.AppendLine("if(requestPool.findIndex(request => request === requestID) < 0)");
                html.AppendLine("{");
                html.AppendLine("followup.length = 0;");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("await sleep(10);");
                html.AppendLine("websocket.send(requestID);");
                html.AppendLine("}");

                html.AppendLine("for (let i = 1; i <= 1000 ; i++)");
                html.AppendLine("{");
                html.AppendLine("followup.push(i);");
                html.AppendLine("if(requestPool.findIndex(request => request === requestID) < 0)");
                html.AppendLine("{");
                html.AppendLine("followup.length = 0;");
                html.AppendLine("break;");
                html.AppendLine("}");
                html.AppendLine("await sleep(100);");
                html.AppendLine("websocket.send(requestID);");
                html.AppendLine("}");
                html.AppendLine("");
            }

            void endOnmessage()
            {
                html.AppendLine("}");
            }

            void endHTML()
            { 
                html.AppendLine("</script>");
                html.AppendLine("");
                html.AppendLine("</body>");
            }

            void outputDebug()
            { 
                using (StreamWriter toDisk = new StreamWriter("uWeb.html"))
                {
                    toDisk.Write(html);
                    toDisk.Close();
                    html.Clear();
                }

                FileInfo fi = new FileInfo("uWeb.html");
                if (fi.Exists)
                {
                    try
                    {
                        using (Process exeProcess = Process.Start(@"uWeb.html"))
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
    }
}
