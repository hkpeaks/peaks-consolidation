# youFastCSV @ Company: YOUFAST.NET email: max@youfast.net

See demo video: https://youtu.be/i6PqhXLv4is

See sample log: https://github.com/YuWaiPang/youFastCSV/blob/main/samplelog.txt
(it is tested on a desktop windows PC having 8 cores and 32GB memory).

Dependency: Dotnet 4.72+ or Dotnet Core 3.1+. This solution and project folder is create using Visual Studo 2019 Community Edition.
If you run it on Linux, please use Visual Studio Code build the runtime.

Input Data: CSV, Binaram, DataTable 

Output Data: CSV, Binaram, DataTable, JSON, HTML, JSON

where DataTable is a standard dotnet in-memory data set
      Binaram is a customised dotnet in-memory data set

Example 1: How to retrieve value of the in-memory binaram (see Example1.zip and CashFlow.csv)
=============================================================================================

binaram currentProcess = new binaram(); // new one instance of the in-memory "Binaram" processing

csv2BinaramInput currentInput = new csv2BinaramInput();  // new one instance of csv2Binaram properties

currentInput.filePath = "Cashflow.csv"; // configure to process one data file "Cashflow.csv"

binaram csv2Binaram = currentProcess.csv2Binaram(currentInput); // use current csv2Binaram properties to run csv2Binaram method

for (int i = 0; i < csv2Binaram.dataType.Count; i++) // display the data schema of the convered binaram "csv2Binaram"

    Console.WriteLine("Column:" + i + " " + csv2Binaram.columnName[i] + ":" + csv2Binaram.dataType[i] + " " + csv2Binaram.factTable[i].Count);

int _column = 2;             

Console.WriteLine();
Console.WriteLine(csv2Binaram.columnName[_column]);
Console.WriteLine("----------------");

for (int _row = 1; _row < 5; _row++) // when row = 0, the value represents column ID 

    Console.WriteLine(readBinaram(_column, _row));

string readBinaram(int column, int row)

{
    
    string cell;
    if (csv2Binaram.dataType[column] == "Number")
        cell = csv2Binaram.factTable[column][row].ToString();
    else
        cell = csv2Binaram.key2Value[column][csv2Binaram.factTable[column][row]].ToString(); // csv2Binaram.key2Value is used to convert key to value for non number type column
        
    return cell;
    
}


Example 2: How to convert csv file to binaram and then convert to json file (see Example2.zip and CashFlow.csv)
===============================================================================================================
// CSV to Binaram

binaram currentProcess = new binaram(); // new one instance of the in-memory "Binaram" processing

csv2BinaramInput currentInput = new csv2BinaramInput();  // new one instance of csv2Binaram properties

currentInput.filePath = "Cashflow.csv"; // configure to process one data file "Cashflow.csv"

binaram csv2Binaram = currentProcess.csv2Binaram(currentInput); // use current csv2Binaram properties to run csv2Binaram method           

// Binaram to JSON

binaram2JSONsetting setBinaram2JSON = new binaram2JSONsetting();

setBinaram2JSON.tableName = "Cashflow";

StringBuilder binaram2JSON = currentProcess.binaram2JSONMultithread(csv2Binaram, setBinaram2JSON);

using (StreamWriter toDisk = new StreamWriter(setBinaram2JSON.tableName + ".json"))

{

    toDisk.Write(binaram2JSON);
    toDisk.Close();
    
}

var Filelength = new FileInfo("Cashflow.csv").Length;

Console.WriteLine("Cashflow.json has filelength of " + Filelength + " was generated.");

Console.ReadLine();



