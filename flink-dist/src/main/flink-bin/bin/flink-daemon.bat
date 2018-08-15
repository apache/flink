@echo off

setlocal EnableDelayedExpansion

for %%X in (java.exe) do (set FOUND=%%~$PATH:X)
if not defined FOUND (
    echo java.exe was not found in PATH variable
    goto :eof
)

cscript /nologo %~dp0\flink-daemon.vbs %*

endlocal
