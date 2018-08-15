::###############################################################################
::  Licensed to the Apache Software Foundation (ASF) under one
::  or more contributor license agreements.  See the NOTICE file
::  distributed with this work for additional information
::  regarding copyright ownership.  The ASF licenses this file
::  to you under the Apache License, Version 2.0 (the
::  "License"); you may not use this file except in compliance
::  with the License.  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
::  Unless required by applicable law or agreed to in writing, software
::  distributed under the License is distributed on an "AS IS" BASIS,
::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::  See the License for the specific language governing permissions and
:: limitations under the License.
::###############################################################################

@echo off
rem Start/stop a Flink TaskManager.
setlocal EnableDelayedExpansion

for %%X in (java.exe) do (set FOUND=%%~$PATH:X)
if not defined FOUND (
    echo java.exe was not found in PATH variable
    goto :eof
)

rem Get first argument
SET STARTSTOP=%1

IF NOT "%STARTSTOP%"=="start" IF NOT "%STARTSTOP%"=="start-foreground" IF NOT "%STARTSTOP%"=="stop" IF NOT "%STARTSTOP%"=="stop-all" (
    ECHO Usage: taskmanager.bat ^(start^|start-foreground^|stop^|stop-all^)
    exit /b 1
)

rem Get remaining arguments
SET _all=%*
IF NOT "%~2"=="" (
    CALL SET ARGS=%%_all:*%1=%%
) ELSE (
    SET ARGS=
)

IF "%STARTSTOP%"=="start-foreground" (
    %~dp0\flink-console.bat taskmanager %ARGS%
) ELSE (
    %~dp0\flink-daemon.bat %STARTSTOP% taskmanager %ARGS%
)

endlocal
