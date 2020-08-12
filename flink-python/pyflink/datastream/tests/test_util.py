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

import pickle

from pyflink.datastream.functions import SinkFunction
from pyflink.java_gateway import get_gateway


class DataStreamTestSinkFunction(SinkFunction):
    """
    A util class to collect test DataStream transformation results.
    """

    def __init__(self):
        self.j_data_stream_collect_sink = get_gateway().jvm \
            .org.apache.flink.python.util.DataStreamTestCollectSink()
        super(DataStreamTestSinkFunction, self).__init__(sink_func=self.j_data_stream_collect_sink)

    def get_results(self, is_python_object: bool = False):
        j_results = self.get_java_function().collectAndClear(is_python_object)
        results = list(j_results)
        if not is_python_object:
            return results
        else:
            str_results = []
            for result in results:
                pickled_result = pickle.loads(result)
                str_results.append(str(pickled_result))
            return str_results

    def clear(self):
        if self.j_data_stream_collect_sink is None:
            return
        self.j_data_stream_collect_sink.clear()
