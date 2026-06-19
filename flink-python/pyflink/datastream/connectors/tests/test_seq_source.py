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
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import load_java_class


class SequenceSourceTests(PyFlinkStreamingTestCase):

    def test_seq_source(self):
        seq_source = NumberSequenceSource(1, 10)

        seq_source_clz = load_java_class(
            "org.apache.flink.api.connector.source.lib.NumberSequenceSource")
        from_field = seq_source_clz.getDeclaredField("from")
        from_field.setAccessible(True)
        self.assertEqual(1, from_field.get(seq_source.get_java_function()))

        to_field = seq_source_clz.getDeclaredField("to")
        to_field.setAccessible(True)
        self.assertEqual(10, to_field.get(seq_source.get_java_function()))
