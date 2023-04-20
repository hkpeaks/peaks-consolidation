using System.Collections.Generic;
using System.Data;
using System.Text;


namespace youFastConversion
{
    public class csv2DataTablesetting
    {
        public int columnThread = 100;
        public int rowThread = 100;  
    }

    public class csv2HTMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;  
    }

    public class csv2JSONsetting
    {
        public int columnThread = 100;
        public int rowThread = 100; 
        public string tableName { get; set; }
    }

    public class csv2XMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;  
        public string tableName { get; set; }
    }

    public class dataTable2CSVsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;
        public string separator = ",";       
    }

    public class dataTable2HTMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100; 
    }

    public class dataTable2JSONsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;       
        public string tableName { get; set; }
    }

    public class dataTable2XMLsetting
    {
        public int columnThread = 100;
        public int rowThread = 100;        
        public string tableName { get; set; }
    }

    public class binaram
    {
        public int validateRow { get; set; } // number of rows for validation
        public Dictionary<int, int> tableColumnCountExceptionList { get; set; } // record non-qualified CSV info, first row is number of cell for header row         
        public int fileByteLength { get; set; } // total number of bytes of a file bytestream       
        public Dictionary<int, string> dataType { get; set; }  // date type: date, text, number
        public Dictionary<int, string> columnName { get; set; }  // assume first row is column name
        public Dictionary<int, List<double>> factTable { get; set; } // maximum number of keys for each dimension: 65,535
        public Dictionary<int, Dictionary<double, string>> key2Value { get; set; } // key to value lookup
        public Dictionary<int, Dictionary<string, double>> value2Key { get; set; } // value to key loopup

        public binaram csv2Binaram(csv2BinaramInput currentInput)
        {
            csv2BinaramDataFlow currentProcess = new csv2BinaramDataFlow();
            binaram currentOutput = currentProcess.csv2Binaram(currentInput);
            return currentOutput;
        } 

        public DataTable csv2DataTable(csv2BinaramInput currentInput, csv2DataTablesetting setCSV2DataTable)
        {
            // csv to Binaram
            csv2BinaramInput setCSV2Binaram = new csv2BinaramInput();
            setCSV2Binaram.columnThread = setCSV2DataTable.columnThread;
            setCSV2Binaram.rowThread = setCSV2DataTable.rowThread;
            csv2BinaramDataFlow process1 = new csv2BinaramDataFlow();
            binaram output1 = process1.csv2Binaram(currentInput);

            // Binaram to DataTable
            binaram2DataTablesetting setBinaram2DataTable = new binaram2DataTablesetting();
            setBinaram2DataTable.rowThread = setCSV2DataTable.rowThread;            
            binaram2DataTabledataFlow process2 = new binaram2DataTabledataFlow();
            DataTable output2 = process2.binaram2DataTableMultithread(output1, setBinaram2DataTable);

            return output2;
        }

        public StringBuilder csv2HTML(csv2BinaramInput currentInput, csv2HTMLsetting setCSV2HTML)
        {
            // csv to Binaram
            csv2BinaramInput setCSV2Binaram = new csv2BinaramInput();
            setCSV2Binaram.columnThread = setCSV2HTML.columnThread;
            setCSV2Binaram.rowThread = setCSV2HTML.rowThread;
            csv2BinaramDataFlow process1 = new csv2BinaramDataFlow();
            binaram output1 = process1.csv2Binaram(currentInput);

            // Binaram to HTML
            binaram2HTMLsetting setBinaram2HTML = new binaram2HTMLsetting();            
            setBinaram2HTML.rowThread = setCSV2HTML.rowThread;           
            binaram2HTMLdataFlow process2 = new binaram2HTMLdataFlow();
            StringBuilder output2 = process2.binaram2HTMLMultithread(output1, setBinaram2HTML);

            return output2;
        }

        public StringBuilder csv2JSON(csv2BinaramInput currentInput, csv2JSONsetting setCSV2JSON)
        {
            // csv to Binaram
            csv2BinaramInput setCSV2Binaram = new csv2BinaramInput();
            setCSV2Binaram.columnThread = setCSV2JSON.columnThread;
            setCSV2Binaram.rowThread = setCSV2JSON.rowThread;
            csv2BinaramDataFlow process1 = new csv2BinaramDataFlow();
            binaram output1 = process1.csv2Binaram(currentInput);

            // Binaram to JSON
            binaram2JSONsetting setBinaram2JSON = new binaram2JSONsetting();
            setBinaram2JSON.rowThread = setCSV2JSON.rowThread;            
            setBinaram2JSON.tableName = setCSV2JSON.tableName;                        
            binaram2JSONdataFlow process2 = new binaram2JSONdataFlow();
            StringBuilder output2 = process2.binaram2JSONMultithread(output1, setBinaram2JSON);

            return output2;
        }

        public StringBuilder csv2XML(csv2BinaramInput currentInput, csv2XMLsetting setCSV2XML)
        {
            // csv to Binaram
            csv2BinaramInput setCSV2Binaram = new csv2BinaramInput();
            setCSV2Binaram.columnThread = setCSV2XML.columnThread;
            setCSV2Binaram.rowThread = setCSV2XML.rowThread;
            csv2BinaramDataFlow process1 = new csv2BinaramDataFlow();
            binaram output1 = process1.csv2Binaram(currentInput);

            // Binaram to XML
            binaram2XMLsetting setBinaram2XML = new binaram2XMLsetting();
            setBinaram2XML.rowThread = setCSV2XML.rowThread;            
            setBinaram2XML.tableName = setCSV2XML.tableName;
            binaram2XMLdataFlow process2 = new binaram2XMLdataFlow();
            StringBuilder output2 = process2.binaram2XMLMultithread(output1, setBinaram2XML);

            return output2;
        }       
       
        public StringBuilder binaram2CSVMultithread(binaram currentInput, binaram2CSVsetting currentSetting)
        {
            binaram2CSVdataFlow currentProcess = new binaram2CSVdataFlow();
            StringBuilder currentOutput = currentProcess.binaram2CSVMultithread(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder binaram2JSONMultithread(binaram currentInput, binaram2JSONsetting currentSetting)
        {
            binaram2JSONdataFlow currentProcess = new binaram2JSONdataFlow();
            StringBuilder currentOutput = currentProcess.binaram2JSONMultithread(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder binaram2XMLMultithread(binaram currentInput, binaram2XMLsetting currentSetting)
        {
            binaram2XMLdataFlow currentProcess = new binaram2XMLdataFlow();
            StringBuilder currentOutput = currentProcess.binaram2XMLMultithread(currentInput, currentSetting);
            return currentOutput;
        }
        public StringBuilder binaram2HTMLMultithread(binaram currentInput, binaram2HTMLsetting currentSetting)
        {
            binaram2HTMLdataFlow currentProcess = new binaram2HTMLdataFlow();
            StringBuilder currentOutput = currentProcess.binaram2HTMLMultithread(currentInput, currentSetting);
            return currentOutput;
        }
        public DataTable binaram2DataTableMultithread(binaram currentInput, binaram2DataTablesetting currentSetting)
        {
            binaram2DataTabledataFlow currentProcess = new binaram2DataTabledataFlow();
            DataTable currentOutput = currentProcess.binaram2DataTableMultithread(currentInput, currentSetting);
            return currentOutput;
        }
        public binaram dataTable2BinaramMultithread(DataTable currentInput, dataTable2BinaramSetting currentSetting)
        {
            dataTable2BinaramdataFlow currentProcess = new dataTable2BinaramdataFlow();
            binaram currentOutput = currentProcess.dataTable2BinaramMultithread(currentInput, currentSetting);
            return currentOutput;
        }        
        public StringBuilder dataTable2CSV(DataTable currentInput, dataTable2CSVsetting setDataTable2CSV)
        {  
            // DataTable to Binaram
            dataTable2BinaramSetting setDataTable2Binaram = new dataTable2BinaramSetting();
            setDataTable2Binaram.columnThread = setDataTable2CSV.columnThread;
            dataTable2BinaramdataFlow process1 = new dataTable2BinaramdataFlow();
            binaram output1 = process1.dataTable2BinaramMultithread(currentInput, setDataTable2Binaram);

            // Binaram to CSV
            binaram2CSVsetting setBinaram2CSV = new binaram2CSVsetting();
            setBinaram2CSV.separator = setDataTable2CSV.separator;           
            setBinaram2CSV.rowThread = setDataTable2CSV.rowThread;            
            binaram2CSVdataFlow process2 = new binaram2CSVdataFlow();
            StringBuilder output2 = process2.binaram2CSVMultithread(output1, setBinaram2CSV);

            return output2;
        }
        public StringBuilder dataTable2HTML(DataTable currentInput, dataTable2HTMLsetting setDataTable2HTML)
        {
            // DataTable to Binaram
            dataTable2BinaramSetting setDataTable2Binaram = new dataTable2BinaramSetting();
            setDataTable2Binaram.columnThread = setDataTable2HTML.columnThread;
            dataTable2BinaramdataFlow process1 = new dataTable2BinaramdataFlow();
            binaram output1 = process1.dataTable2BinaramMultithread(currentInput, setDataTable2Binaram);

            // Binaram to HTML
            binaram2HTMLsetting setBinaram2HTML = new binaram2HTMLsetting();            
            setBinaram2HTML.rowThread = setDataTable2HTML.rowThread;
            binaram2HTMLdataFlow process2 = new binaram2HTMLdataFlow();
            StringBuilder output2 = process2.binaram2HTMLMultithread(output1, setBinaram2HTML);

            return output2;
        }
        public StringBuilder dataTable2JSON(DataTable currentInput, dataTable2JSONsetting setDataTable2JSON)
        {
            // DataTable to Binaram
            dataTable2BinaramSetting setDataTable2Binaram = new dataTable2BinaramSetting();
            setDataTable2Binaram.columnThread = setDataTable2JSON.columnThread;
            dataTable2BinaramdataFlow process1 = new dataTable2BinaramdataFlow();
            binaram output1 = process1.dataTable2BinaramMultithread(currentInput, setDataTable2Binaram);

            // Binaram to JSON
            binaram2JSONsetting setBinaram2JSON = new binaram2JSONsetting();
            setBinaram2JSON.rowThread = setDataTable2JSON.rowThread;            
            setBinaram2JSON.tableName = setDataTable2JSON.tableName;
            binaram2JSONdataFlow process2 = new binaram2JSONdataFlow();
            StringBuilder output2 = process2.binaram2JSONMultithread(output1, setBinaram2JSON);

            return output2;
        }
        public StringBuilder dataTable2XML(DataTable currentInput, dataTable2XMLsetting setDataTable2XML)
        {
            // DataTable to Binaram
            dataTable2BinaramSetting setDataTable2Binaram = new dataTable2BinaramSetting();
            setDataTable2Binaram.columnThread = setDataTable2XML.columnThread;
            dataTable2BinaramdataFlow process1 = new dataTable2BinaramdataFlow();
            binaram output1 = process1.dataTable2BinaramMultithread(currentInput, setDataTable2Binaram);

            // Binaram to XML
            binaram2XMLsetting setBinaram2XML = new binaram2XMLsetting();           
            setBinaram2XML.tableName = setDataTable2XML.tableName;
            binaram2XMLdataFlow process2 = new binaram2XMLdataFlow();
            StringBuilder output2 = process2.binaram2XMLMultithread(output1, setBinaram2XML);

            return output2;
        }

    }
}
