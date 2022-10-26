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
from pyflink.common import Types
from pyflink.datastream.connectors.rabbitmq import RMQSink, RMQSource, RMQConnectionConfig
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field_value


class RMQTest(PyFlinkStreamingTestCase):

    def test_rabbitmq_connectors(self):
        connection_config = RMQConnectionConfig.Builder() \
            .set_host('localhost') \
            .set_port(5672) \
            .set_virtual_host('/') \
            .set_user_name('guest') \
            .set_password('guest') \
            .build()
        type_info = Types.ROW([Types.INT(), Types.STRING()])
        deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(type_info=type_info).build()

        rmq_source = RMQSource(
            connection_config, 'source_queue', True, deserialization_schema)
        self.assertEqual(
            get_field_value(rmq_source.get_java_function(), 'queueName'), 'source_queue')
        self.assertTrue(get_field_value(rmq_source.get_java_function(), 'usesCorrelationId'))

        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info) \
            .build()
        rmq_sink = RMQSink(connection_config, 'sink_queue', serialization_schema)
        self.assertEqual(
            get_field_value(rmq_sink.get_java_function(), 'queueName'), 'sink_queue')
