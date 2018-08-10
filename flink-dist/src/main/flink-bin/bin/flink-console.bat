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
rem Start a Flink service as a console application.

setlocal

rem Get first argument
SET SERVICE=%1
rem Get remaining arguments
SET _all=%*
IF NOT "%~2"=="" (
    CALL SET ARGS=%%_all:*%1=%%
) ELSE (
    SET ARGS=
)

SET bin=%~dp0
SET FLINK_ROOT_DIR=%bin%..
SET FLINK_LIB_DIR=%FLINK_ROOT_DIR%\lib
SET FLINK_CLASSPATH=%FLINK_LIB_DIR%\*
SET FLINK_CONF_DIR=%FLINK_ROOT_DIR%\conf
SET FLINK_LOG_DIR=%FLINK_ROOT_DIR%\log
SET JVM_ARGS=-Xms1024m -Xmx1024m

SET logname_tm=flink-%username%-taskmanager.log
SET log_tm=%FLINK_LOG_DIR%\%logname_tm%
SET outname_tm=flink-%username%-taskmanager.out
SET out_tm=%FLINK_LOG_DIR%\%outname_tm%
SET log_setting_tm=-Dlog.file="%log_tm%" -Dlogback.configurationFile=file:"%FLINK_CONF_DIR%\logback.xml" -Dlog4j.configuration=file:"%FLINK_CONF_DIR%\log4j.properties"

:: Log rotation (quick and dirty)
CD "%FLINK_LOG_DIR%"
for /l %%x in (5, -1, 1) do (
SET /A y = %%x+1
RENAME "%logname_jm%.%%x" "%logname_jm%.!y!" 2> nul
RENAME "%logname_tm%.%%x" "%logname_tm%.!y!" 2> nul
RENAME "%outname_jm%.%%x" "%outname_jm%.!y!"  2> nul
RENAME "%outname_tm%.%%x" "%outname_tm%.!y!"  2> nul
)
RENAME "%logname_jm%" "%logname_jm%.0"  2> nul
RENAME "%logname_tm%" "%logname_tm%.0"  2> nul
RENAME "%outname_jm%" "%outname_jm%.0"  2> nul
RENAME "%outname_tm%" "%outname_tm%.0"  2> nul
DEL "%logname_jm%.6"  2> nul
DEL "%logname_tm%.6"  2> nul
DEL "%outname_jm%.6"  2> nul
DEL "%outname_tm%.6"  2> nul

IF "%SERVICE%"=="jobmanager" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.jobmanager.JobManager
) ELSE IF "%SERVICE%"=="taskmanager" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.taskmanager.TaskManager
) ELSE IF "%SERVICE%"=="taskexecutor" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
) ELSE IF "%SERVICE%"=="historyserver" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.webmonitor.history.HistoryServer
) ELSE IF "%SERVICE%"=="zookeeper" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer
) ELSE IF "%SERVICE%"=="standalonesession" (
    SET CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
) ELSE (
    ECHO Unknown service %SERVICE%. Usage: flink-console.bat ^(jobmanager^|taskmanager^|historyserver^|zookeeper^) ^[args^]
    exit /b 1
)

echo Starting %SERVICE% as a console application on host %computername%.
start java %JVM_ARGS% %log_setting_tm% -cp "%FLINK_CLASSPATH%"; %CLASS_TO_RUN% --configDir "%FLINK_CONF_DIR%" %ARGS% > "%out_tm%" 2>&1


endlocal
