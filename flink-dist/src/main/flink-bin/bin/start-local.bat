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
setlocal EnableDelayedExpansion

SET bin=%~dp0
SET FLINK_ROOT_DIR=%bin%..
SET FLINK_LIB_DIR=%FLINK_ROOT_DIR%\lib
SET FLINK_CONF_DIR=%FLINK_ROOT_DIR%\conf
SET FLINK_LOG_DIR=%FLINK_ROOT_DIR%\log

SET JVM_ARGS=-Xms768m -Xmx768m

SET FLINK_JM_CLASSPATH=%FLINK_LIB_DIR%\*

SET logname=flink-%username%-jobmanager-%computername%.log
SET log=%FLINK_LOG_DIR%\%logname%
SET outname=flink-%username%-jobmanager-%computername%.out
SET out=%FLINK_LOG_DIR%\%outname%
SET log_setting=-Dlog.file="%log%" -Dlogback.configurationFile=file:"%FLINK_CONF_DIR%/logback.xml" -Dlog4j.configuration=file:"%FLINK_CONF_DIR%/log4j.properties"


:: Log rotation (quick and dirty)
CD "%FLINK_LOG_DIR%"
for /l %%x in (5, -1, 1) do ( 
SET /A y = %%x+1 
RENAME "%logname%.%%x" "%logname%.!y!" 2> nul
RENAME "%outname%.%%x" "%outname%.!y!"  2> nul 
)
RENAME "%logname%" "%logname%.0"  2> nul
RENAME "%outname%" "%outname%.0"  2> nul
DEL "%logname%.6"  2> nul
DEL "%outname%.6"  2> nul

for %%X in (java.exe) do (set FOUND=%%~$PATH:X)
if not defined FOUND (
    echo java.exe was not found in PATH variable
    goto :eof
)

echo Starting Flink job manager. Webinterface by default on http://localhost:8081/.
echo Don't close this batch window. Stop job manager by pressing Ctrl+C.

java %JVM_ARGS% %log_setting% -cp "%FLINK_JM_CLASSPATH%"; org.apache.flink.runtime.jobmanager.JobManager --configDir "%FLINK_CONF_DIR%" --executionMode local > "%out%" 2>&1

endlocal
