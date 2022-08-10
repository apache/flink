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

import pyflink.datastream.data_stream as data_stream
from pyflink.common import typeinfo

from pyflink.common.configuration import Configuration
from pyflink.common.serialization import SimpleStringSchema, DeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.types import Row
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaTopicPartition, \
    KafkaOffsetsInitializer, KafkaOffsetResetStrategy, KafkaRecordSerializationSchema, KafkaSink, \
    FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.avro import AvroRowDeserializationSchema, AvroRowSerializationSchema
from pyflink.datastream.formats.csv import CsvRowDeserializationSchema, CsvRowSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import (
    PyFlinkStreamingTestCase,
    PyFlinkTestCase,
    invoke_java_object_method,
    to_java_data_structure,
)
from pyflink.util.java_utils import to_jarray, is_instance_of, get_field_value


class KafkaSourceTests(PyFlinkStreamingTestCase):

    def test_legacy_kafka_connector(self):
        source_topic = 'test_source_topic'
        sink_topic = 'test_sink_topic'
        props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
        type_info = Types.ROW([Types.INT(), Types.STRING()])

        # Test for kafka consumer
        deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(type_info=type_info).build()

        flink_kafka_consumer = FlinkKafkaConsumer(source_topic, deserialization_schema, props)
        flink_kafka_consumer.set_start_from_earliest()
        flink_kafka_consumer.set_commit_offsets_on_checkpoints(True)

        j_properties = get_field_value(flink_kafka_consumer.get_java_function(), 'properties')
        self.assertEqual('localhost:9092', j_properties.getProperty('bootstrap.servers'))
        self.assertEqual('test_group', j_properties.getProperty('group.id'))
        self.assertTrue(get_field_value(flink_kafka_consumer.get_java_function(),
                                        'enableCommitOnCheckpoints'))
        j_start_up_mode = get_field_value(flink_kafka_consumer.get_java_function(), 'startupMode')

        j_deserializer = get_field_value(flink_kafka_consumer.get_java_function(), 'deserializer')
        j_deserialize_type_info = invoke_java_object_method(j_deserializer, "getProducedType")
        deserialize_type_info = typeinfo._from_java_type(j_deserialize_type_info)
        self.assertTrue(deserialize_type_info == type_info)
        self.assertTrue(j_start_up_mode.equals(get_gateway().jvm
                                               .org.apache.flink.streaming.connectors
                                               .kafka.config.StartupMode.EARLIEST))
        j_topic_desc = get_field_value(flink_kafka_consumer.get_java_function(),
                                       'topicsDescriptor')
        j_topics = invoke_java_object_method(j_topic_desc, 'getFixedTopics')
        self.assertEqual(['test_source_topic'], list(j_topics))

        # Test for kafka producer
        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info) \
            .build()
        flink_kafka_producer = FlinkKafkaProducer(sink_topic, serialization_schema, props)
        flink_kafka_producer.set_write_timestamp_to_kafka(False)

        j_producer_config = get_field_value(flink_kafka_producer.get_java_function(),
                                            'producerConfig')
        self.assertEqual('localhost:9092', j_producer_config.getProperty('bootstrap.servers'))
        self.assertEqual('test_group', j_producer_config.getProperty('group.id'))
        self.assertFalse(get_field_value(flink_kafka_producer.get_java_function(),
                                         'writeTimestampToKafka'))

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
        plan = json.loads(self.env.get_execution_plan())
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
        kafka_subscriber = get_field_value(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicListSubscriber'
        )
        topics = get_field_value(kafka_subscriber, 'topics')
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
        kafka_subscriber = get_field_value(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicPatternSubscriber'
        )
        topic_pattern = get_field_value(kafka_subscriber, 'topicPattern')
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
        kafka_subscriber = get_field_value(source.get_java_function(), 'subscriber')
        self.assertEqual(
            kafka_subscriber.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.subscriber.PartitionSetSubscriber'
        )
        partitions = get_field_value(kafka_subscriber, 'subscribedPartitions')
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
                get_field_value(source.get_java_function(), 'boundedness').toString(), 'BOUNDED'
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
                get_field_value(source.get_java_function(), 'boundedness').toString(),
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

    def test_set_value_only_deserializer(self):
        def _check(schema: DeserializationSchema, class_name: str):
            source = KafkaSource.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_topics('test_topic') \
                .set_value_only_deserializer(schema) \
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
        offsets_initializer = get_field_value(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.ReaderHandledOffsetsInitializer'
        )

        starting_offset = get_field_value(offsets_initializer, 'startingOffset')
        self.assertEqual(starting_offset, offset)

        offset_reset_strategy = get_field_value(offsets_initializer, 'offsetResetStrategy')
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
        offsets_initializer = get_field_value(source.get_java_function(), field_name)
        self.assertEqual(
            offsets_initializer.getClass().getCanonicalName(),
            'org.apache.flink.connector.kafka.source.enumerator.initializer'
            '.TimestampOffsetsInitializer'
        )

        starting_timestamp = get_field_value(offsets_initializer, 'startingTimestamp')
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


class KafkaSinkTests(PyFlinkStreamingTestCase):

    def test_compile(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()

        ds = self.env.from_collection([], type_info=Types.STRING())
        ds.sink_to(sink)

        plan = json.loads(self.env.get_execution_plan())
        self.assertEqual(plan['nodes'][1]['type'], 'Sink: Writer')
        self.assertEqual(plan['nodes'][2]['type'], 'Sink: Committer')

    def test_set_bootstrap_severs(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092,localhost:9093') \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        config = get_field_value(sink.get_java_function(), 'kafkaProducerConfig')
        self.assertEqual(config.get('bootstrap.servers'), 'localhost:9092,localhost:9093')

    def test_set_delivery_guarantee(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        guarantee = get_field_value(sink.get_java_function(), 'deliveryGuarantee')
        self.assertEqual(guarantee.toString(), 'none')

        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        guarantee = get_field_value(sink.get_java_function(), 'deliveryGuarantee')
        self.assertEqual(guarantee.toString(), 'at-least-once')

        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE) \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        guarantee = get_field_value(sink.get_java_function(), 'deliveryGuarantee')
        self.assertEqual(guarantee.toString(), 'exactly-once')

    def test_set_transactional_id_prefix(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_transactional_id_prefix('test-prefix') \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        prefix = get_field_value(sink.get_java_function(), 'transactionalIdPrefix')
        self.assertEqual(prefix, 'test-prefix')

    def test_set_property(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_record_serializer(self._build_serialization_schema()) \
            .set_property('test-key', 'test-value') \
            .build()
        config = get_field_value(sink.get_java_function(), 'kafkaProducerConfig')
        self.assertEqual(config.get('test-key'), 'test-value')

    def test_set_record_serializer(self):
        sink = KafkaSink.builder() \
            .set_bootstrap_servers('localhost:9092') \
            .set_record_serializer(self._build_serialization_schema()) \
            .build()
        serializer = get_field_value(sink.get_java_function(), 'recordSerializer')
        self.assertEqual(serializer.getClass().getCanonicalName(),
                         'org.apache.flink.connector.kafka.sink.'
                         'KafkaRecordSerializationSchemaBuilder.'
                         'KafkaRecordSerializationSchemaWrapper')
        topic_selector = get_field_value(serializer, 'topicSelector')
        self.assertEqual(topic_selector.apply(None), 'test-topic')
        value_serializer = get_field_value(serializer, 'valueSerializationSchema')
        self.assertEqual(value_serializer.getClass().getCanonicalName(),
                         'org.apache.flink.api.common.serialization.SimpleStringSchema')

    @staticmethod
    def _build_serialization_schema() -> KafkaRecordSerializationSchema:
        return KafkaRecordSerializationSchema.builder() \
            .set_topic('test-topic') \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()


class KafkaRecordSerializationSchemaTests(PyFlinkTestCase):

    def test_set_topic(self):
        input_type = Types.ROW([Types.STRING()])

        serialization_schema = KafkaRecordSerializationSchema.builder() \
            .set_topic('test-topic') \
            .set_value_serialization_schema(
                JsonRowSerializationSchema.builder().with_type_info(input_type).build()) \
            .build()
        jvm = get_gateway().jvm
        serialization_schema._j_serialization_schema.open(
            jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext(),
            jvm.org.apache.flink.connector.kafka.sink.DefaultKafkaSinkContext(
                0, 1, jvm.java.util.Properties()))

        j_record = serialization_schema._j_serialization_schema.serialize(
            to_java_data_structure(Row('test')), None, None
        )
        self.assertEqual(j_record.topic(), 'test-topic')
        self.assertIsNone(j_record.key())
        self.assertEqual(j_record.value(), b'{"f0":"test"}')

    def test_set_topic_selector(self):
        def _select(data):
            data = data[0]
            if data == 'a':
                return 'topic-a'
            elif data == 'b':
                return 'topic-b'
            else:
                return 'topic-dead-letter'

        def _check_record(data, topic, serialized_data):
            input_type = Types.ROW([Types.STRING()])

            serialization_schema = KafkaRecordSerializationSchema.builder() \
                .set_topic_selector(_select) \
                .set_value_serialization_schema(
                    JsonRowSerializationSchema.builder().with_type_info(input_type).build()) \
                .build()
            jvm = get_gateway().jvm
            serialization_schema._j_serialization_schema.open(
                jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext(),
                jvm.org.apache.flink.connector.kafka.sink.DefaultKafkaSinkContext(
                    0, 1, jvm.java.util.Properties()))
            sink = KafkaSink.builder() \
                .set_bootstrap_servers('localhost:9092') \
                .set_record_serializer(serialization_schema) \
                .build()

            ds = MockDataStream(Types.ROW([Types.STRING()]))
            ds.sink_to(sink)
            row = Row(data)
            topic_row = ds.feed(row)  # type: Row
            j_record = serialization_schema._j_serialization_schema.serialize(
                to_java_data_structure(topic_row), None, None
            )
            self.assertEqual(j_record.topic(), topic)
            self.assertIsNone(j_record.key())
            self.assertEqual(j_record.value(), serialized_data)

        _check_record('a', 'topic-a', b'{"f0":"a"}')
        _check_record('b', 'topic-b', b'{"f0":"b"}')
        _check_record('c', 'topic-dead-letter', b'{"f0":"c"}')
        _check_record('d', 'topic-dead-letter', b'{"f0":"d"}')

    def test_set_key_serialization_schema(self):
        def _check_key_serialization_schema(key_serialization_schema, expected_class):
            serialization_schema = KafkaRecordSerializationSchema.builder() \
                .set_topic('test-topic') \
                .set_key_serialization_schema(key_serialization_schema) \
                .set_value_serialization_schema(SimpleStringSchema()) \
                .build()
            schema_field = get_field_value(serialization_schema._j_serialization_schema,
                                           'keySerializationSchema')
            self.assertIsNotNone(schema_field)
            self.assertEqual(schema_field.getClass().getCanonicalName(), expected_class)

        self._check_serialization_schema_implementations(_check_key_serialization_schema)

    def test_set_value_serialization_schema(self):
        def _check_value_serialization_schema(value_serialization_schema, expected_class):
            serialization_schema = KafkaRecordSerializationSchema.builder() \
                .set_topic('test-topic') \
                .set_value_serialization_schema(value_serialization_schema) \
                .build()
            schema_field = get_field_value(serialization_schema._j_serialization_schema,
                                           'valueSerializationSchema')
            self.assertIsNotNone(schema_field)
            self.assertEqual(schema_field.getClass().getCanonicalName(), expected_class)

        self._check_serialization_schema_implementations(_check_value_serialization_schema)

    @staticmethod
    def _check_serialization_schema_implementations(check_function):
        input_type = Types.ROW([Types.STRING()])

        check_function(
            JsonRowSerializationSchema.builder().with_type_info(input_type).build(),
            'org.apache.flink.formats.json.JsonRowSerializationSchema'
        )
        check_function(
            CsvRowSerializationSchema.Builder(input_type).build(),
            'org.apache.flink.formats.csv.CsvRowSerializationSchema'
        )
        avro_schema_string = """
        {
            "type": "record",
            "name": "test_record",
            "fields": []
        }
        """
        check_function(
            AvroRowSerializationSchema(avro_schema_string=avro_schema_string),
            'org.apache.flink.formats.avro.AvroRowSerializationSchema'
        )
        check_function(
            SimpleStringSchema(),
            'org.apache.flink.api.common.serialization.SimpleStringSchema'
        )


class MockDataStream(data_stream.DataStream):

    def __init__(self, original_type=None):
        super().__init__(None)
        self._operators = []
        self._type = original_type

    def feed(self, data):
        for op in self._operators:
            data = op(data)
        return data

    def get_type(self):
        return self._type

    def map(self, f, output_type=None):
        self._operators.append(f)
        self._type = output_type

    def sink_to(self, sink):
        ds = self
        from pyflink.datastream.connectors.base import SupportsPreprocessing
        if isinstance(sink, SupportsPreprocessing) and sink.get_transformer() is not None:
            ds = sink.get_transformer().apply(self)
        return ds
