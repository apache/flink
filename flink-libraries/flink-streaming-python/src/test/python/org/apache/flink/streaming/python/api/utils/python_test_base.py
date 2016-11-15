################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import sys
from org.apache.flink.api.java.utils import ParameterTool
from org.apache.flink.streaming.python.api.environment import PythonStreamExecutionEnvironment


class TestBase(object):
    _params = ParameterTool.fromArgs(sys.argv[1:]) if len(sys.argv[1:]) > 0 else None

    def __init__(self):
        pass

    def _get_execution_environment(self):
        if TestBase._params:
            print("Create local execution environment with provided configurations")
            return PythonStreamExecutionEnvironment.create_local_execution_environment(TestBase._params.getConfiguration())
        else:
            print("Get execution environment")
            return PythonStreamExecutionEnvironment.get_execution_environment()
