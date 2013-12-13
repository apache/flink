::#######################################################################################################################
:: 
::  Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
:: 
::  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
::  the License. You may obtain a copy of the License at
:: 
::      http://www.apache.org/licenses/LICENSE-2.0
:: 
::  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
::  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
::  specific language governing permissions and limitations under the License.
:: 
::#######################################################################################################################
@echo off
setlocal EnableDelayedExpansion

SET bin=%~dp0
SET NEPHELE_ROOT_DIR=%bin%..
SET NEPHELE_LIB_DIR=%NEPHELE_ROOT_DIR%\lib
SET NEPHELE_CONF_DIR=%NEPHELE_ROOT_DIR%\conf
SET NEPHELE_LOG_DIR=%NEPHELE_ROOT_DIR%\log

SET JVM_ARGS=-Xms768m -Xmx768m

SET NEPHELE_JM_CLASSPATH=%NEPHELE_LIB_DIR%\*

SET logname=nephele-%username%-jobmanager-%computername%.log
SET log=%NEPHELE_LOG_DIR%\%logname%
SET outname=nephele-%username%-jobmanager-%computername%.out
SET out=%NEPHELE_LOG_DIR%\%outname%
SET log_setting=-Dlog.file=%log% -Dlog4j.configuration=file:%NEPHELE_CONF_DIR%/log4j.properties


:: Log rotation (quick and dirty)
CD %NEPHELE_LOG_DIR%
for /l %%x in (5, -1, 1) do ( 
SET /A y = %%x+1 
RENAME "%logname%.%%x" "%logname%.!y!" 2> nul
RENAME "%outname%.%%x" "%outname%.!y!"  2> nul 
)
RENAME "%logname%" "%logname%.0"  2> nul
RENAME "%outname%" "%outname%.0"  2> nul
DEL "%logname%.6"  2> nul
DEL "%outname%.6"  2> nul


echo Starting Stratosphere job manager. Webinterface by default on http://localhost:8081/.
echo Don't close this batch window. Stop job manager by pressing Ctrl+C.

java %JVM_ARGS% %log_setting% -cp %NEPHELE_JM_CLASSPATH% eu.stratosphere.nephele.jobmanager.JobManager -executionMode local -configDir %NEPHELE_CONF_DIR%  > "%out%"  2>&1

endlocal
