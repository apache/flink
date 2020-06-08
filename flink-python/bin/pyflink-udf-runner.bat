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
setlocal enabledelayedexpansion

if not defined python (
    set python=python.exe
)

if "%python%a"=="pythona" (
    set python=python.exe
)

if "%FLINK_TESTING%a"=="1a" (
    set CURRENT_DIR=!cd!
    cd /d %FLINK_HOME%
    set ACTUAL_FLINK_HOME=!cd!
    cd /d ../../../../
    set FLINK_SOURCE_ROOT_DIR=!cd!
    cd /d flink-python
    set FLINK_PYTHON=!cd!
    if exist !FLINK_PYTHON!/pyflink/fn_execution/boot.py (
        :: use pyflink source code to override the pyflink.zip in PYTHONPATH
        :: to ensure loading latest code
        set PYTHONPATH=!FLINK_PYTHON!;%PYTHONPATH%
    )
    cd /d !CURRENT_DIR!
)

if %_PYTHON_WORKING_DIR%a NEQ a (
    :: set current working directory to _PYTHON_WORKING_DIR
    cd /d %_PYTHON_WORKING_DIR%
)

set log=%BOOT_LOG_DIR%/flink-python-udf-boot.log
call %python% -m pyflink.fn_execution.boot %* 2>&1 > %log%

endlocal
