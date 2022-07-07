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
from typing import Dict

from pyflink.common.configuration import Configuration
from pyflink.common.serialization import SimpleStringSchema, DeserializationSchema, \
    JsonRowDeserializationSchema, CsvRowDeserializationSchema, AvroRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaTopicPartition, \
    KafkaOffsetsInitializer, KafkaOffsetResetStrategy
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import to_jarray, is_instance_of


class KafkaSourceTest(PyFlinkStreamingTestCase):

    def test_compiling(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_topics('test_topic') \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        ds = self.env.from_source(source=source,
                                  watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                                  source_name='kafka source')
        ds.print()
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Source: kafka source', plan['nodes'][0]['type'])

    def test_set_properties(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_group_id('test_group_id') \
            .set_client_id_prefix('test_client_id_prefix') \
            .set_property('test_property', 'test_value') \
            .set_topics('test_topic') \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        conf = self._get_kafka_source_configuration(source)
        self.assertEqual(conf.get_string('bootstrap.servers', ''), 'localhost:9092')
        self.assertEqual(conf.get_string('group.id', ''), 'test_group_id')
        self.assertEqual(conf.get_string('client.id.prefix', ''), 'test_client_id_prefix')
        self.assertEqual(conf.get_string('test_property', ''), 'test_value')

    def test_set_topics(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_topics('test_topic1', 'test_topic2') \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        kafka_subscriber = self._get_java_field(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicListSubscriber'
        )
        topics = self._get_java_field(kafka_subscriber, 'topics')
        self.assertTrue(is_instance_of(topics, get_gateway().jvm.java.util.List))
        self.assertEqual(topics.size(), 2)
        self.assertEqual(topics[0], 'test_topic1')
        self.assertEqual(topics[1], 'test_topic2')

    def test_set_topic_pattern(self):
        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_topic_pattern('test_topic*') \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        kafka_subscriber = self._get_java_field(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicPatternSubscriber'
        )
        topic_pattern = self._get_java_field(kafka_subscriber, 'topicPattern')
        self.assertTrue(is_instance_of(topic_pattern, get_gateway().jvm.java.util.regex.Pattern))
        self.assertEqual(topic_pattern.toString(), 'test_topic*')

    def test_set_partitions(self):
        topic_partition_1 = KafkaTopicPartition('test_topic', 1)
        topic_partition_2 = KafkaTopicPartition('test_topic', 2)
        source = KafkaSource.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_partitions({topic_partition_1, topic_partition_2}) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        kafka_subscriber = self._get_java_field(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.PartitionSetSubscriber'
        )
        partitions = self._get_java_field(kafka_subscriber, 'subscribedPartitions')
        self.assertTrue(is_instance_of(partitions, get_gateway().jvm.java.util.Set))
        self.assertTrue(topic_partition_1._to_j_topic_partition() in partitions)
        self.assertTrue(topic_partition_2._to_j_topic_partition() in partitions)

    def test_set_starting_offsets(self):
        def _build_source(initializer: KafkaOffsetsInitializer):
            return KafkaSource.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_topics('test_topic') \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .set_group_id('test_group') \
                .set_starting_offsets(initializer) \
                .build()

        self._check_reader_handled_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.latest()), -1, KafkaOffsetResetStrategy.LATEST
        )
        self._check_reader_handled_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.earliest()), -2,
            KafkaOffsetResetStrategy.EARLIEST
        )
        self._check_reader_handled_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.committed_offsets()), -3,
            KafkaOffsetResetStrategy.NONE
        )
        self._check_reader_handled_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.LATEST
            )), -3, KafkaOffsetResetStrategy.LATEST
        )
        self._check_timestamp_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.timestamp(100)), 100
        )
        specified_offsets = {
            KafkaTopicPartition('test_topic1', 1): 1000,
            KafkaTopicPartition('test_topic2', 2): 2000
        }
        self._check_specified_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.offsets(specified_offsets)), specified_offsets,
            KafkaOffsetResetStrategy.EARLIEST
        )
        self._check_specified_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.offsets(
                specified_offsets,
                KafkaOffsetResetStrategy.LATEST
            )),
            specified_offsets,
            KafkaOffsetResetStrategy.LATEST
        )

    def test_bounded(self):
        def _build_source(initializer: KafkaOffsetsInitializer):
            return KafkaSource.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_topics('test_topic') \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .set_group_id('test_group') \
                .set_bounded(initializer) \
                .build()

        def _check_bounded(source: KafkaSource):
            self.assertEqual(
                self._get_java_field(source.get_java_function(), 'boundedness').toString(),
                'BOUNDED'
            )

        self._test_set_bounded_or_unbounded(_build_source, _check_bounded)

    def test_unbounded(self):
        def _build_source(initializer: KafkaOffsetsInitializer):
            return KafkaSource.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_topics('test_topic') \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .set_group_id('test_group') \
                .set_unbounded(initializer) \
                .build()

        def _check_bounded(source: KafkaSource):
            self.assertEqual(
                self._get_java_field(source.get_java_function(), 'boundedness').toString(),
                'CONTINUOUS_UNBOUNDED'
            )

        self._test_set_bounded_or_unbounded(_build_source, _check_bounded)

    def _test_set_bounded_or_unbounded(self, _build_source, _check_boundedness):
        source = _build_source(KafkaOffsetsInitializer.latest())
        _check_boundedness(source)
        self._check_reader_handled_offsets_initializer(
            source, -1, KafkaOffsetResetStrategy.LATEST, False
        )
        source = _build_source(KafkaOffsetsInitializer.earliest())
        _check_boundedness(source)
        self._check_reader_handled_offsets_initializer(
            source, -2, KafkaOffsetResetStrategy.EARLIEST, False
        )
        source = _build_source(KafkaOffsetsInitializer.committed_offsets())
        _check_boundedness(source)
        self._check_reader_handled_offsets_initializer(
            source, -3, KafkaOffsetResetStrategy.NONE, False
        )
        source = _build_source(KafkaOffsetsInitializer.committed_offsets(
            KafkaOffsetResetStrategy.LATEST
        ))
        _check_boundedness(source)
        self._check_reader_handled_offsets_initializer(
            source, -3, KafkaOffsetResetStrategy.LATEST, False
        )
        source = _build_source(KafkaOffsetsInitializer.timestamp(100))
        _check_boundedness(source)
        self._check_timestamp_offsets_initializer(source, 100, False)
        specified_offsets = {
            KafkaTopicPartition('test_topic1', 1): 1000,
            KafkaTopicPartition('test_topic2', 2): 2000
        }
        source = _build_source(KafkaOffsetsInitializer.offsets(specified_offsets))
        _check_boundedness(source)
        self._check_specified_offsets_initializer(
            source, specified_offsets, KafkaOffsetResetStrategy.EARLIEST, False
        )
        source = _build_source(KafkaOffsetsInitializer.offsets(
            specified_offsets,
            KafkaOffsetResetStrategy.LATEST)
        )
        _check_boundedness(source)
        self._check_specified_offsets_initializer(
            source, specified_offsets, KafkaOffsetResetStrategy.LATEST, False
        )

    def test_set_value_only_deserialization_schema(self):
        def _check(schema: DeserializationSchema, class_name: str):
            source = KafkaSource.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_topics('test_topic') \
                .set_value_only_deserializer(schema) \
                .build()
            deserialization_schema_wrapper = self._get_java_field(source.get_java_function(),
                                                                  'deserializationSchema')
            self.assertEqual(
                deserialization_schema_wrapper.getClass().getCanonicalName(),
                'org.apache.flink.connector.kafka.source.reader.deserializer'
                '.KafkaValueOnlyDeserializationSchemaWrapper'
            )
            deserialization_schema = self._get_java_field(deserialization_schema_wrapper,
                                                          'deserializationSchema')
            self.assertEqual(deserialization_schema.getClass().getCanonicalName(),
                             class_name)

        _check(SimpleStringSchema(), 'org.apache.flink.api.common.serialization.SimpleStringSchema')
        _check(
            JsonRowDeserializationSchema.builder().type_info(Types.ROW([Types.STRING()])).build(),
            'org.apache.flink.formats.json.JsonRowDeserializationSchema'
        )
        _check(
            CsvRowDeserializationSchema.Builder(Types.ROW([Types.STRING()])).build(),
            'org.apache.flink.formats.csv.CsvRowDeserializationSchema'
        )
        avro_schema_string = """
        {
            "type": "record",
            "name": "test_record",
            "fields": []
        }
        """
        _check(
            AvroRowDeserializationSchema(avro_schema_string=avro_schema_string),
            'org.apache.flink.formats.avro.AvroRowDeserializationSchema'
        )

    def _check_reader_handled_offsets_initializer(self,
                                                  source: KafkaSource,
                                                  offset: int,
                                                  reset_strategy: KafkaOffsetResetStrategy,
                                                  is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = self._get_java_field(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.ReaderHandledOffsetsInitializer'
        )

        starting_offset = self._get_java_field(offsets_initializer, 'startingOffset')
        self.assertEqual(starting_offset, offset)

        offset_reset_strategy = self._get_java_field(offsets_initializer, 'offsetResetStrategy')
        self.assertTrue(
            offset_reset_strategy.equals(reset_strategy._to_j_offset_reset_strategy())
        )

    def _check_timestamp_offsets_initializer(self,
                                             source: KafkaSource,
                                             timestamp: int,
                                             is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = self._get_java_field(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.TimestampOffsetsInitializer'
        )

        starting_timestamp = self._get_java_field(offsets_initializer, 'startingTimestamp')
        self.assertEqual(starting_timestamp, timestamp)

    def _check_specified_offsets_initializer(self,
                                             source: KafkaSource,
                                             offsets: Dict[KafkaTopicPartition, int],
                                             reset_strategy: KafkaOffsetResetStrategy,
                                             is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = self._get_java_field(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.SpecifiedOffsetsInitializer'
        )

        initial_offsets = self._get_java_field(offsets_initializer, 'initialOffsets')
        self.assertTrue(is_instance_of(initial_offsets, get_gateway().jvm.java.util.Map))
        self.assertEqual(initial_offsets.size(), len(offsets))
        for j_topic_partition in initial_offsets:
            topic_partition = KafkaTopicPartition(j_topic_partition.topic(),
                                                  j_topic_partition.partition())
            self.assertIsNotNone(offsets.get(topic_partition))
            self.assertEqual(initial_offsets[j_topic_partition], offsets[topic_partition])

        offset_reset_strategy = self._get_java_field(offsets_initializer, 'offsetResetStrategy')
        self.assertTrue(
            offset_reset_strategy.equals(reset_strategy._to_j_offset_reset_strategy())
        )

    @staticmethod
    def _get_kafka_source_configuration(source: KafkaSource):
        jvm = get_gateway().jvm
        j_source = source.get_java_function()
        j_to_configuration = j_source.getClass().getDeclaredMethod(
            'getConfiguration', to_jarray(jvm.java.lang.Class, [])
        )
        j_to_configuration.setAccessible(True)
        j_configuration = j_to_configuration.invoke(j_source, to_jarray(jvm.java.lang.Object, []))
        return Configuration(j_configuration=j_configuration)

    @staticmethod
    def _get_java_field(java_object, field_name: str):
        j_field = java_object.getClass().getDeclaredField(field_name)
        j_field.setAccessible(True)
        return j_field.get(java_object)
