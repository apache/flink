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
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Types, ConfigOptions, Duration
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.pulsar import TopicRoutingMode, MessageDelayer, PulsarSink, \
    PulsarSource, StartCursor, StopCursor, RangeGenerator
from pyflink.testing.test_case_utils import PyFlinkUTTestCase
from pyflink.util.java_utils import get_field_value, is_instance_of


class FlinkPulsarTest(PyFlinkUTTestCase):

    def test_pulsar_source(self):
        TEST_OPTION_NAME = 'pulsar.source.enableAutoAcknowledgeMessage'
        pulsar_source = PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics('ada') \
            .set_start_cursor(StartCursor.earliest()) \
            .set_unbounded_stop_cursor(StopCursor.never()) \
            .set_bounded_stop_cursor(StopCursor.at_publish_time(22)) \
            .set_subscription_name('ff') \
            .set_consumer_name('test_consumer') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_config(TEST_OPTION_NAME, True) \
            .set_properties({'pulsar.source.autoCommitCursorInterval': '1000'}) \
            .build()

        ds = self.env.from_source(source=pulsar_source,
                                  watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                                  source_name="pulsar source")
        ds.print()
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Source: pulsar source', plan['nodes'][0]['type'])

        configuration = get_field_value(pulsar_source.get_java_function(), "sourceConfiguration")
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.client.serviceUrl')
                .string_type()
                .no_default_value()._j_config_option), 'pulsar://localhost:6650')
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.admin.adminUrl')
                .string_type()
                .no_default_value()._j_config_option), 'http://localhost:8080')
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.consumer.subscriptionName')
                .string_type()
                .no_default_value()._j_config_option), 'ff')
        test_option = ConfigOptions.key(TEST_OPTION_NAME).boolean_type().no_default_value()
        self.assertEqual(
            configuration.getBoolean(
                test_option._j_config_option), True)
        self.assertEqual(
            configuration.getLong(
                ConfigOptions.key('pulsar.source.autoCommitCursorInterval')
                .long_type()
                .no_default_value()._j_config_option), 1000)

    def test_source_set_topics_with_list(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .build()

    def test_source_set_topics_pattern(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topic_pattern('ada.*') \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .build()

    def test_source_deprecated_method(self):
        test_option = ConfigOptions.key('pulsar.source.enableAutoAcknowledgeMessage') \
            .boolean_type().no_default_value()
        pulsar_source = PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topic_pattern('ada.*') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_unbounded_stop_cursor(StopCursor.at_publish_time(4444)) \
            .set_subscription_name('ff') \
            .set_config(test_option, True) \
            .set_properties({'pulsar.source.autoCommitCursorInterval': '1000'}) \
            .build()
        configuration = get_field_value(pulsar_source.get_java_function(), "sourceConfiguration")
        self.assertEqual(
            configuration.getBoolean(
                test_option._j_config_option), True)
        self.assertEqual(
            configuration.getLong(
                ConfigOptions.key('pulsar.source.autoCommitCursorInterval')
                .long_type()
                .no_default_value()._j_config_option), 1000)

    def test_stop_cursor_publish_time(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics('ada') \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_start_cursor(StartCursor.from_publish_time(2)) \
            .set_bounded_stop_cursor(StopCursor.at_publish_time(14)) \
            .set_bounded_stop_cursor(StopCursor.after_publish_time(24)) \
            .build()

    def test_stop_cursor_event_time(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics('ada') \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_bounded_stop_cursor(StopCursor.after_event_time(14)) \
            .set_bounded_stop_cursor(StopCursor.at_event_time(24)) \
            .build()

    def test_set_range_generator(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_range_generator(RangeGenerator.full()) \
            .build()

        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_range_generator(RangeGenerator.fixed_key(keys='k', key_bytes=bytearray(b'abc'))) \
            .build()

    def test_set_authentication(self):
        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_authentication('test.class', 'key1:val1,key2:val2') \
            .build()

        PulsarSource.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_subscription_name('ff') \
            .set_deserialization_schema(SimpleStringSchema()) \
            .set_authentication('test.class', {'k1': 'v1', 'k2': 'v2'}) \
            .build()

    def test_pulsar_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        TEST_OPTION_NAME = 'pulsar.producer.chunkingEnabled'
        pulsar_sink = PulsarSink.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_producer_name('fo') \
            .set_topics('ada') \
            .set_serialization_schema(SimpleStringSchema()) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN) \
            .delay_sending_message(MessageDelayer.fixed(Duration.of_seconds(12))) \
            .set_config(TEST_OPTION_NAME, True) \
            .set_properties({'pulsar.producer.batchingMaxMessages': '100'}) \
            .build()

        ds.sink_to(pulsar_sink).name('pulsar sink')

        plan = eval(self.env.get_execution_plan())
        self.assertEqual('pulsar sink: Writer', plan['nodes'][1]['type'])
        configuration = get_field_value(pulsar_sink.get_java_function(), "sinkConfiguration")
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.client.serviceUrl')
                .string_type()
                .no_default_value()._j_config_option), 'pulsar://localhost:6650')
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.admin.adminUrl')
                .string_type()
                .no_default_value()._j_config_option), 'http://localhost:8080')
        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.producer.producerName')
                .string_type()
                .no_default_value()._j_config_option), 'fo - %s')

        j_pulsar_serialization_schema = get_field_value(
            pulsar_sink.get_java_function(), 'serializationSchema')
        j_serialization_schema = get_field_value(
            j_pulsar_serialization_schema, 'serializationSchema')
        self.assertTrue(
            is_instance_of(
                j_serialization_schema,
                'org.apache.flink.api.common.serialization.SimpleStringSchema'))

        self.assertEqual(
            configuration.getString(
                ConfigOptions.key('pulsar.sink.deliveryGuarantee')
                .string_type()
                .no_default_value()._j_config_option), 'at-least-once')

        j_topic_router = get_field_value(pulsar_sink.get_java_function(), "topicRouter")
        self.assertTrue(
            is_instance_of(
                j_topic_router,
                'org.apache.flink.connector.pulsar.sink.writer.router.RoundRobinTopicRouter'))

        j_message_delayer = get_field_value(pulsar_sink.get_java_function(), 'messageDelayer')
        delay_duration = get_field_value(j_message_delayer, 'delayDuration')
        self.assertEqual(delay_duration, 12000)

        test_option = ConfigOptions.key(TEST_OPTION_NAME).boolean_type().no_default_value()
        self.assertEqual(
            configuration.getBoolean(
                test_option._j_config_option), True)
        self.assertEqual(
            configuration.getLong(
                ConfigOptions.key('pulsar.producer.batchingMaxMessages')
                .long_type()
                .no_default_value()._j_config_option), 100)

    def test_sink_set_topics_with_list(self):
        PulsarSink.builder() \
            .set_service_url('pulsar://localhost:6650') \
            .set_admin_url('http://localhost:8080') \
            .set_topics(['ada', 'beta']) \
            .set_serialization_schema(SimpleStringSchema()) \
            .build()
