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
from pyflink.java_gateway import get_gateway


class SourceFunction(object):
    """
    Base class for all stream data source in Flink.
    """

    def __init__(self, j_source_function):
        """
        Constructor of SinkFunction.

        :param j_source_func: The java SourceFunction object.
        """
        self._j_source_function = j_source_function

    def get_java_source_function(self):
        """
        Return the java source function object.
        """
        return self._j_source_function


class CustomSourceFunction(SourceFunction):
    """
    A CustomSourceFunction enables users to use a custom source function implemented in Java.
    Important: The jar packaged with the custom source function must be added to the Flink
    CLASS_PATH.
    """

    def __init__(self, source_func_class: str, *args):
        """
        Constructor of CustomSourceFunction.

        :param source_func_class: the FQDN (my.project.MyCustomSourceFunction) for the source
                                  function class.
        :param args: arguments of the custom source function. Optional.
        """
        j_source_func_class = get_gateway().jvm.__getattr__(source_func_class)
        if len(args) > 0:
            j_source_func = j_source_func_class(*args)
        else:
            j_source_func = j_source_func_class()
        super(CustomSourceFunction, self).__init__(j_source_function=j_source_func)
