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

from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import DataStream
from pyflink.java_gateway import get_gateway


class DataStreamCollectUtil(object):
    """
    A util class to collect test DataStream transformation results.
    """

    def __init__(self):
        self._is_python_objects = False
        self._j_data_stream_test_collect_sink = None

    def collect(self, data_stream: DataStream):
        gateway = get_gateway()
        self._is_python_objects = data_stream.get_type() == Types.PICKLED_BYTE_ARRAY()
        self._j_data_stream_test_collect_sink = gateway.jvm \
            .org.apache.flink.python.util.DataStreamTestCollectSink(self._is_python_objects)
        data_stream._j_data_stream.addSink(self._j_data_stream_test_collect_sink)

    def results(self):

        if self._j_data_stream_test_collect_sink is None:
            raise Exception("Must collect a Stream before getting results.")

        j_results = self._j_data_stream_test_collect_sink.collectAndClear()
        results = list(j_results)
        if not self._is_python_objects:
            return results
        else:
            str_results = []
            for result in results:
                pickled_result = pickle.loads(result)
                str_results.append(str(pickled_result))
            return str_results
