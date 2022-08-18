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

from pyflink.common import SimpleStringSchema, Types, Duration
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field_value, is_instance_of
from pyflink.datastream.connectors.pubsub import PubSubSource, PubSubSink, Credentials, \
    PubSubSubscriberFactory


class PubSubTest(PyFlinkStreamingTestCase):

    def test_pubsub_source(self):
        pubsub_source = PubSubSource.new_builder() \
            .with_deserialization_schema(SimpleStringSchema()) \
            .with_project_name("project") \
            .with_subscription_name("subscription") \
            .with_credentials(Credentials.emulator_credentials()) \
            .with_pubsub_subscriber_factory(
                PubSubSubscriberFactory.default(10, Duration.of_seconds(10), 10)) \
            .build()
        ds = self.env.add_source(source_func=pubsub_source, source_name="pubsub source")
        ds.print()
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Source: pubsub source', plan['nodes'][0]['type'])
        self.assertTrue(is_instance_of(
            get_field_value(pubsub_source.get_java_function(), 'credentials'),
            'org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials'))

    def test_pubsub_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        pubsub_sink = PubSubSink.new_builder() \
            .with_serialization_schema(SimpleStringSchema()) \
            .with_project_name("project") \
            .with_topic_name("topic") \
            .with_host_and_port_for_emulator("localhost:8080") \
            .build()

        ds.add_sink(pubsub_sink).name('pubsub sink')
        plan = eval(self.env.get_execution_plan())

        self.assertEqual('Sink: pubsub sink', plan['nodes'][1]['type'])
        self.assertEqual(get_field_value(pubsub_sink.get_java_function(), 'projectName'), 'project')
