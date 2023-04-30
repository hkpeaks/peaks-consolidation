## Web Pivot Table

The author created a .NET project called “WebPivotTable” before using Golang. He is considering whether to re-implement this visual into Peaks DataFrame. The original project’s source code can be found below. The last bug fix was made on August 3rd, 2020. On April 30th, 2023, the author published this project again. After downloading and building the runtime using Visual Studio 2022 Community Version, a folder called “youFast” was generated. Clicking a youFast which will start a websocket server and open your browser with default data. The app supports csv file only. The websocket runs on local host “ws://127.0.0.1:5000/”. The websocket is an open source and can be downloaded from https://github.com/statianzo/Fleck.

https://youtu.be/yfJnYQBJ5ZY

[![Web Pivot Table](https://github.com/hkpeaks/peaks-framework/blob/main/WebPivotTable/WebPivotTable.png)](http://www.youtube.com/watch?v=yfJnYQBJ5ZY "Web Pivot Table")

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
