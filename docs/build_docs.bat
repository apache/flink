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
:start
call jekyll -version >nul 2>&1
if "%errorlevel%"=="0" goto check_redcarpet
echo ERROR: Could not find jekyll.
echo Please install with 'gem install jekyll' (see http://jekyllrb.com).
exit /b 1

:check_redcarpet
call redcarpet -version >nul 2>&1
if "%errorlevel%"=="0" goto check_pygments
echo WARN: Could not find redcarpet. 
echo Please install with 'gem install redcarpet' (see https://github.com/vmg/redcarpet).
echo Redcarpet is needed for Markdown parsing and table of contents generation.
goto check_pygments

:check_pygments
call python -c "import pygments" >nul 2>&1
if "%errorlevel%"=="0" goto execute
echo WARN: Could not find pygments.
echo Please install with 'sudo easy_install Pygments' (requires Python; see http://pygments.org). 
echo Pygments is needed for syntax highlighting of the code examples.
goto execute

:execute
SET "DOCS_SRC=%cd%"
SET "DOCS_DST=%DOCS_SRC%\target"

::default jekyll command is to just build site
::if flag p is set, start the webserver too.
IF "%1"=="" GOTO :build
IF "%1"=="-p" GOTO :serve 
GOTO :build

:build
jekyll build --source %DOCS_SRC% --destination %DOCS_DST%

:serve
jekyll serve --baseurl "" --watch --source %DOCS_SRC% --destination %DOCS_DST%
