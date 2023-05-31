@echo off
setlocal EnableDelayedExpansion

set start_time=!time!

:: SQL Statement
duckdb -c "copy (SELECT Ledger, Account, DC, Currency, SUM(Base_Amount) as Total_Base_Amount FROM read_csv_auto('input/300-MillionRows.csv') WHERE Ledger>='L30' AND Ledger <='L70' GROUP BY Ledger, Account, DC, Currency) to 'output/DuckFilterGroupBy.csv' (format csv, header true);"


set end_time=!time!

echo Start time: %start_time%
echo End time: %end_time%

:: Calculate elapsed time
set /a hours=%end_time:~0,2%-%start_time:~0,2%
set /a minutes=%end_time:~3,2%-%start_time:~3,2%
set /a seconds=%end_time:~6,2%-%start_time:~6,2%

:: Output elapsed time in seconds
set /a elapsed_seconds=hours*3600+minutes*60+seconds
echo Elapsed time in seconds: %elapsed_seconds%
