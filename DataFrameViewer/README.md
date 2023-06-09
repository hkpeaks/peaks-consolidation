## Web Pivot Table for Peaks DataFrame

The author created a .NET Web Pivot Table (Previously known as youFast Desktop) before using Golang. He is considering whether to re-implement this visual into Peaks DataFrame. The last bug fix was made on August 3rd, 2020. On April 30th, 2023, the author published this project again. 

If you want to use a ready to use version of the app, please download it from the "Releases" section of this page. How to use, please visit https://www.youtube.com/@accountingetl5158/videos

If you want to built the app from source code, please download the above source code, in the directory ..\Downloads\PeaksDataFrameViewer-main>  type dotnet build, you will see a directory "\Downloads\youFast" is created and youFast.exe inside the directory.

Clicking the youFast.exe which will start a websocket server and open your browser with default data. The app supports csv file only with maximum of 50 Million Rows given that your device has installed 32GB RAM. The websocket runs on local host “ws://127.0.0.1:5000/”. The websocket is an open source and can be downloaded from https://github.com/statianzo/Fleck.

Depend on whether there are a real demand, the Peaks project is considering developing a new version that supports billions of rows and is 5X to 10X faster.

### How-to Run the App and Manage its Folders

➾ Once you have downloaded the app from the releases section, you can unzip it and read the readme file.

➾ After that, you can double click on the app and a folder named “uSpace” and a file named “uWeb” will be generated. Sample files are simulated automatically.

➾ Finally, double click on the “uWeb.html” file to open it in your browser. Or your browser may be opened automatically without clicking the file.

➾ To test your data, you can either drag and drop it from your desktop to the browser or copy it to the uSpace folder (which is an upload folder). Once you view it on the browser, it will be moved to uSpace\importedFile.

➾ uSpace\distinctDB contained serialized dataset of the in-memory dataset. 

➾ You may need to clean the files inside uSpace\distinctDB\Primary and uSpace\importedFile when it become sizable becuase uSpace\importedFile keep version control for every data import.

If it is not running properly, please check whether your machine has installed .net 4.72 or above. This is why the author moved to Go - no need to install .net to support an app.

<p align="center">
<img src="https://github.com/hkpeaks/peaks-framework/blob/main/PeaksDataFrameViewer/InstalledFolder.png" width=50% height=50%>
</p>

https://youtu.be/yfJnYQBJ5ZY

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/PeaksDataFrameViewer/WebPivotTable.png)](http://www.youtube.com/watch?v=yfJnYQBJ5ZY "Web Pivot Table")

### Features

➾ using C#, Vanilla JavaScript and W3.css for an integrated frontend to backend Web development, it do not implement any javascript framework or library 

➾  real-time web technology use Fleck websocket

➾  the project do not have dependency other than .net and fleck wedsocket, actually the web implementation did not use asp.net or asp.net core

➾  build-in a key-value NoSQL database for in-memory and disk versions

➾  building a set of algorithms to maximize parallel computing operating units which is extended to a big csv file and an in-memory table

➾  ultra-fast data import to web crosstab report with drag & drop and drill down capabilities 

➾  interactive pivot table that lets you move X and Y columns with real-time drilldown by simple mouse actions

➾  crosstab report supports multi-level of analysis account trial balance by period/currency/region

➾  1.3+ second youFast can process a million rows of a csv file to produce a web summary report and 0.13+ second youFast can filter data from 10 million rows to produce a web crosstab report (testing machine: Dell OptiPlex 7070 Micro Form Factor with Intel Core i9-9900 8 Cores 32G Ram Windows 10)

➾  developing pagination in X and Y direction, different levels of numeric precision

➾  clicking an app file with less than 500KB, user can enjoy zero installation and implementation 

➾  the app can run on a share drive and USB memory stick

➾  this project is maintained up to 100% C# source code

# About Peaks DataFrame

The performance of Peaks DataFrame is 5X ~ 10X of the viewer. It extend to support billions of rows using 32GB Memory. To learn about current development of Peaks DataFrame, please visit https://github.com/hkpeaks/peaks-framework


