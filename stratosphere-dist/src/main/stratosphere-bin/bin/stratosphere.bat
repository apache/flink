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
setlocal

SET bin=%~dp0
SET NEPHELE_ROOT_DIR=%bin%..
SET NEPHELE_LIB_DIR=%NEPHELE_ROOT_DIR%\lib

SET JVM_ARGS=-Xmx512m

SET NEPHELE_JM_CLASSPATH=%NEPHELE_LIB_DIR%\*

java %JVM_ARGS% -cp "%NEPHELE_JM_CLASSPATH%" eu.stratosphere.client.CliFrontend %*

endlocal
