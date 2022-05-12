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
from typing import Union

from py4j.java_gateway import JavaObject

from pyflink.datastream.functions import JavaFunctionWrapper


class Source(JavaFunctionWrapper):
    """
    Base class for all unified data source in Flink.
    """

    def __init__(self, source: Union[str, JavaObject]):
        """
        Constructor of Source.

        :param source: The java Source object.
        """
        super(Source, self).__init__(source)


class Sink(JavaFunctionWrapper):
    """
    Base class for all unified data sink in Flink.
    """

    def __init__(self, sink: Union[str, JavaObject]):
        """
        Constructor of Sink.

        :param sink: The java Sink object.
        """
        super(Sink, self).__init__(sink)
