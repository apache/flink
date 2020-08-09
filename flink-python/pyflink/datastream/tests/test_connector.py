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
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import CustomSourceFunction
from pyflink.datastream.tests.test_util import DataStreamCollectUtil
from pyflink.testing.test_case_utils import PyFlinkTestCase


class PyFlinkConnectorTest(PyFlinkTestCase):
    """
    A test class for testing Python Flink Connectors.
    """

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def test_add_custom_source(self):
        record_count = 100
        custom_source = CustomSourceFunction("org.apache.flink.python.util.MyCustomSourceFunction",
                                             record_count)
        ds = self.env.add_source(custom_source, type_info=Types.ROW([Types.INT(), Types.STRING(),
                                                                     Types.DOUBLE()]))
        collect_util = DataStreamCollectUtil()
        collect_util.collect(ds)
        self.env.execute("test add custom source")
        results = collect_util.results()
        self.assertEqual(len(results), record_count)
