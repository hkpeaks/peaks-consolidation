@echo off
setlocal EnableDelayedExpansion

set start_time=!time!

:: SQL Statement
duckdb -c "copy (select * from read_csv_auto('input/300-MillionRows.csv') WHERE Ledger>='L30' AND Ledger <='L70' ) to 'output/DuckFilter.csv';"


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
