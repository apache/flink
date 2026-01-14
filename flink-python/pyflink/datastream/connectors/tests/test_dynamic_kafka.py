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
import json
from typing import Dict
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.dynamic_kafka import DynamicKafkaSource, \
    KafkaRecordDeserializationSchema, KafkaStreamSetSubscriber, StreamPatternSubscriber, \
    SingleClusterTopicMetadataService
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaOffsetResetStrategy, \
    KafkaTopicPartition
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field, get_field_value, is_instance_of


class DynamicKafkaSourceTests(PyFlinkStreamingTestCase):

    def test_compiling(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'test-stream'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        ds = self.env.from_source(source=source,
                                  watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                                  source_name='dynamic kafka source')
        ds.print()
        plan = json.loads(self.env.get_execution_plan())
        self.assertEqual('Source: dynamic kafka source', plan['nodes'][0]['type'])

    def test_set_stream_ids(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a', 'stream-b'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.KafkaStreamSetSubscriber'
        )
        stream_ids = get_field_value(subscriber, 'streamIds')
        self.assertTrue(is_instance_of(stream_ids, get_gateway().jvm.java.util.Set))
        self.assertEqual(stream_ids.size(), 2)
        self.assertTrue('stream-a' in stream_ids)
        self.assertTrue('stream-b' in stream_ids)

    def test_set_stream_pattern(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_pattern('stream-*') \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.StreamPatternSubscriber'
        )
        stream_pattern = get_field_value(subscriber, 'streamPattern')
        self.assertTrue(is_instance_of(stream_pattern, get_gateway().jvm.java.util.regex.Pattern))
        self.assertEqual(stream_pattern.toString(), 'stream-*')

    def test_set_stream_set_subscriber(self):
        subscriber = KafkaStreamSetSubscriber({'stream-a', 'stream-b'})
        source = DynamicKafkaSource.builder() \
            .set_kafka_stream_subscriber(subscriber) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        j_subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            j_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.KafkaStreamSetSubscriber'
        )
        stream_ids = get_field_value(j_subscriber, 'streamIds')
        self.assertTrue(is_instance_of(stream_ids, get_gateway().jvm.java.util.Set))
        self.assertEqual(stream_ids.size(), 2)
        self.assertTrue('stream-a' in stream_ids)
        self.assertTrue('stream-b' in stream_ids)

    def test_set_stream_pattern_subscriber(self):
        subscriber = StreamPatternSubscriber('stream-*')
        source = DynamicKafkaSource.builder() \
            .set_kafka_stream_subscriber(subscriber) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        j_subscriber = get_field_value(source.get_java_function(), 'kafkaStreamSubscriber')
        self.assertEqual(
            j_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.dynamic.source.enumerator.subscriber'
            '.StreamPatternSubscriber'
        )
        stream_pattern = get_field_value(j_subscriber, 'streamPattern')
        self.assertTrue(is_instance_of(stream_pattern, get_gateway().jvm.java.util.regex.Pattern))
        self.assertEqual(stream_pattern.toString(), 'stream-*')

    def test_set_properties(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_group_id('test-group') \
            .set_client_id_prefix('test-client-id') \
            .set_property('test-property', 'test-value') \
            .build()
        properties = get_field_value(source.get_java_function(), 'properties')
        self.assertEqual(properties.getProperty('group.id'), 'test-group')
        self.assertEqual(properties.getProperty('client.id.prefix'), 'test-client-id')
        self.assertEqual(properties.getProperty('test-property'), 'test-value')

    def test_set_starting_offsets(self):
        def _build_source(initializer: KafkaOffsetsInitializer):
            return DynamicKafkaSource.builder() \
                .set_stream_ids({'stream-a'}) \
                .set_kafka_metadata_service(self._build_metadata_service()) \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .set_starting_offsets(initializer) \
                .build()

        self._check_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.latest()),
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'LatestOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.LATEST,
            offset=-1
        )
        self._check_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.earliest()),
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'EarliestOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.EARLIEST,
            offset=-2
        )
        self._check_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.committed_offsets()),
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'CommittedOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.NONE,
            offset=-3
        )
        self._check_offsets_initializer(
            _build_source(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.LATEST
            )),
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'CommittedOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.LATEST,
            offset=-3
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
            return DynamicKafkaSource.builder() \
                .set_stream_ids({'stream-a'}) \
                .set_kafka_metadata_service(self._build_metadata_service()) \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .set_bounded(initializer) \
                .build()

        def _check_bounded(source: DynamicKafkaSource):
            self.assertEqual(
                get_field_value(source.get_java_function(), 'boundedness').toString(), 'BOUNDED'
            )

        source = _build_source(KafkaOffsetsInitializer.latest())
        _check_bounded(source)
        self._check_offsets_initializer(
            source,
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'LatestOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.LATEST,
            offset=-1,
            is_start=False
        )
        source = _build_source(KafkaOffsetsInitializer.earliest())
        _check_bounded(source)
        self._check_offsets_initializer(
            source,
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'EarliestOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.EARLIEST,
            offset=-2,
            is_start=False
        )
        source = _build_source(KafkaOffsetsInitializer.committed_offsets())
        _check_bounded(source)
        self._check_offsets_initializer(
            source,
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'CommittedOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.NONE,
            offset=-3,
            is_start=False
        )
        source = _build_source(KafkaOffsetsInitializer.committed_offsets(
            KafkaOffsetResetStrategy.LATEST
        ))
        _check_bounded(source)
        self._check_offsets_initializer(
            source,
            {
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'CommittedOffsetsInitializer',
                'org.apache.flink.connector.kafka.source.enumerator.initializer.'
                'ReaderHandledOffsetsInitializer',
            },
            reset_strategy=KafkaOffsetResetStrategy.LATEST,
            offset=-3,
            is_start=False
        )
        source = _build_source(KafkaOffsetsInitializer.timestamp(100))
        _check_bounded(source)
        self._check_timestamp_offsets_initializer(source, 100, False)
        specified_offsets = {
            KafkaTopicPartition('test_topic1', 1): 1000,
            KafkaTopicPartition('test_topic2', 2): 2000
        }
        source = _build_source(KafkaOffsetsInitializer.offsets(specified_offsets))
        _check_bounded(source)
        self._check_specified_offsets_initializer(
            source, specified_offsets, KafkaOffsetResetStrategy.EARLIEST, False
        )
        source = _build_source(KafkaOffsetsInitializer.offsets(
            specified_offsets,
            KafkaOffsetResetStrategy.LATEST)
        )
        _check_bounded(source)
        self._check_specified_offsets_initializer(
            source, specified_offsets, KafkaOffsetResetStrategy.LATEST, False
        )

    def test_set_value_only_deserializer(self):
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        deserialization_schema_wrapper = get_field_value(source.get_java_function(),
                                                         'deserializationSchema')
        self.assertEqual(
            deserialization_schema_wrapper.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.reader.deserializer'
            '.KafkaValueOnlyDeserializationSchemaWrapper'
        )
        deserialization_schema = get_field_value(deserialization_schema_wrapper,
                                                 'deserializationSchema')
        self.assertEqual(deserialization_schema.getClass().getCanonicalName(),
                         'org.apache.flink.api.common.serialization.SimpleStringSchema')

    def test_set_deserializer(self):
        record_deserializer = KafkaRecordDeserializationSchema.value_only(SimpleStringSchema())
        source = DynamicKafkaSource.builder() \
            .set_stream_ids({'stream-a'}) \
            .set_kafka_metadata_service(self._build_metadata_service()) \
            .set_deserializer(record_deserializer) \
            .build()
        deserialization_schema_wrapper = get_field_value(source.get_java_function(),
                                                         'deserializationSchema')
        self.assertEqual(
            deserialization_schema_wrapper.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.reader.deserializer'
            '.KafkaValueOnlyDeserializationSchemaWrapper'
        )

    @staticmethod
    def _build_metadata_service() -> SingleClusterTopicMetadataService:
        return SingleClusterTopicMetadataService(
            'test-cluster', {'bootstrap.servers': 'localhost:9092'})

    def _check_offsets_initializer(self,
                                   source: DynamicKafkaSource,
                                   expected_class_names,
                                   reset_strategy: KafkaOffsetResetStrategy = None,
                                   offset: int = None,
                                   is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = get_field_value(source.get_java_function(), field_name)
        class_name = offsets_initializer.getClass().getCanonicalName()
        self.assertIn(class_name, expected_class_names)

        if offset is not None:
            starting_offset_field = get_field(offsets_initializer.getClass(), 'startingOffset')
            if starting_offset_field is not None:
                starting_offset = starting_offset_field.get(offsets_initializer)
                self.assertEqual(starting_offset, offset)

        if reset_strategy is not None:
            offset_reset_strategy_field = get_field(offsets_initializer.getClass(),
                                                    'offsetResetStrategy')
            if offset_reset_strategy_field is not None:
                offset_reset_strategy = offset_reset_strategy_field.get(offsets_initializer)
                self.assertTrue(
                    offset_reset_strategy.equals(reset_strategy._to_j_offset_reset_strategy())
                )

    def _check_timestamp_offsets_initializer(self,
                                             source: DynamicKafkaSource,
                                             timestamp: int,
                                             is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = get_field_value(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.TimestampOffsetsInitializer'
        )

        starting_timestamp = get_field_value(offsets_initializer, 'startingTimestamp')
        self.assertEqual(starting_timestamp, timestamp)

    def _check_specified_offsets_initializer(self,
                                             source: DynamicKafkaSource,
                                             offsets: Dict[KafkaTopicPartition, int],
                                             reset_strategy: KafkaOffsetResetStrategy,
                                             is_start: bool = True):
        if is_start:
            field_name = 'startingOffsetsInitializer'
        else:
            field_name = 'stoppingOffsetsInitializer'
        offsets_initializer = get_field_value(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.SpecifiedOffsetsInitializer'
        )

        initial_offsets = get_field_value(offsets_initializer, 'initialOffsets')
        self.assertTrue(is_instance_of(initial_offsets, get_gateway().jvm.java.util.Map))
        self.assertEqual(initial_offsets.size(), len(offsets))
        for j_topic_partition in initial_offsets:
            topic_partition = KafkaTopicPartition(j_topic_partition.topic(),
                                                  j_topic_partition.partition())
            self.assertIsNotNone(offsets.get(topic_partition))
            self.assertEqual(initial_offsets[j_topic_partition], offsets[topic_partition])

        offset_reset_strategy = get_field_value(offsets_initializer, 'offsetResetStrategy')
        self.assertTrue(
            offset_reset_strategy.equals(reset_strategy._to_j_offset_reset_strategy())
        )
