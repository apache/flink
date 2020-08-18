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

from pyflink.common import typeinfo
from pyflink.common.serialization import JsonRowDeserializationSchema, \
    JsonRowSerializationSchema, SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer010, FlinkKafkaProducer010, \
    FlinkKafkaConsumer011, FlinkKafkaProducer011, FlinkKafkaConsumer, FlinkKafkaProducer, JdbcSink,\
    JdbcConnectionOptions, JdbcExecutionOptions, StreamingFileSink, DefaultRollingPolicy, \
    OutputFileConfig
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase, _load_specific_flink_module_jars, \
    get_private_field, invoke_java_object_method


class FlinkKafkaTest(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        # Cache current ContextClassLoader, we will replace it with a temporary URLClassLoader to
        # load specific connector jars with given module path to do dependency isolation. And We
        # will change the ClassLoader back to the cached ContextClassLoader after the test case
        # finished.
        self._cxt_clz_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()

    def test_kafka_connector_010(self):
        _load_specific_flink_module_jars('/flink-connectors/flink-sql-connector-kafka-0.10')
        self.kafka_connector_assertion(FlinkKafkaConsumer010, FlinkKafkaProducer010)

    def test_kafka_connector_011(self):
        _load_specific_flink_module_jars('/flink-connectors/flink-sql-connector-kafka-0.11')
        self.kafka_connector_assertion(FlinkKafkaConsumer011, FlinkKafkaProducer011)

    def test_kafka_connector_universal(self):
        _load_specific_flink_module_jars('/flink-connectors/flink-sql-connector-kafka')
        self.kafka_connector_assertion(FlinkKafkaConsumer, FlinkKafkaProducer)

    def kafka_connector_assertion(self, flink_kafka_consumer_clz, flink_kafka_producer_clz):
        source_topic = 'test_source_topic'
        sink_topic = 'test_sink_topic'
        props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
        type_info = Types.ROW([Types.INT(), Types.STRING()])

        # Test for kafka consumer
        deserialization_schema = JsonRowDeserializationSchema.builder() \
            .type_info(type_info=type_info).build()

        flink_kafka_consumer = flink_kafka_consumer_clz(source_topic, deserialization_schema, props)
        flink_kafka_consumer.set_start_from_earliest()
        flink_kafka_consumer.set_commit_offsets_on_checkpoints(True)

        j_properties = get_private_field(flink_kafka_consumer.get_java_function(), 'properties')
        self.assertEqual('localhost:9092', j_properties.getProperty('bootstrap.servers'))
        self.assertEqual('test_group', j_properties.getProperty('group.id'))
        self.assertTrue(get_private_field(flink_kafka_consumer.get_java_function(),
                                          'enableCommitOnCheckpoints'))
        j_start_up_mode = get_private_field(flink_kafka_consumer.get_java_function(), 'startupMode')

        j_deserializer = get_private_field(flink_kafka_consumer.get_java_function(), 'deserializer')
        j_deserialize_type_info = invoke_java_object_method(j_deserializer, "getProducedType")
        deserialize_type_info = typeinfo._from_java_type(j_deserialize_type_info)
        self.assertTrue(deserialize_type_info == type_info)
        self.assertTrue(j_start_up_mode.equals(get_gateway().jvm
                                               .org.apache.flink.streaming.connectors
                                               .kafka.config.StartupMode.EARLIEST))
        j_topic_desc = get_private_field(flink_kafka_consumer.get_java_function(),
                                         'topicsDescriptor')
        j_topics = invoke_java_object_method(j_topic_desc, 'getFixedTopics')
        self.assertEqual(['test_source_topic'], list(j_topics))

        # Test for kafka producer
        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(type_info) \
            .build()
        flink_kafka_producer = flink_kafka_producer_clz(sink_topic, serialization_schema, props)
        flink_kafka_producer.set_write_timestamp_to_kafka(False)

        j_producer_config = get_private_field(flink_kafka_producer.get_java_function(),
                                              'producerConfig')
        self.assertEqual('localhost:9092', j_producer_config.getProperty('bootstrap.servers'))
        self.assertEqual('test_group', j_producer_config.getProperty('group.id'))
        self.assertFalse(get_private_field(flink_kafka_producer.get_java_function(),
                                           'writeTimestampToKafka'))

    def tearDown(self):
        # Change the ClassLoader back to the cached ContextClassLoader after the test case finished.
        if self._cxt_clz_loader is not None:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(self._cxt_clz_loader)


class FlinkJdbcSinkTest(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self._cxt_clz_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()
        _load_specific_flink_module_jars('/flink-connectors/flink-connector-jdbc')

    def test_jdbc_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder()\
            .with_driver_name('com.mysql.jdbc.Driver')\
            .with_user_name('root')\
            .with_password('password')\
            .with_url('jdbc:mysql://server-name:server-port/database-name').build()

        jdbc_execution_options = JdbcExecutionOptions.builder().with_batch_interval_ms(2000)\
            .with_batch_size(100).with_max_retries(5).build()
        jdbc_sink = JdbcSink.sink("insert into test table", ds.get_type(), jdbc_connection_options,
                                  jdbc_execution_options)

        ds.add_sink(jdbc_sink).name('jdbc sink')
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Sink: jdbc sink', plan['nodes'][1]['type'])
        j_output_format = get_private_field(jdbc_sink.get_java_function(), 'outputFormat')

        connection_options = JdbcConnectionOptions(
            get_private_field(get_private_field(j_output_format, 'connectionProvider'),
                              'jdbcOptions'))
        self.assertEqual(jdbc_connection_options.get_db_url(), connection_options.get_db_url())
        self.assertEqual(jdbc_connection_options.get_driver_name(),
                         connection_options.get_driver_name())
        self.assertEqual(jdbc_connection_options.get_password(), connection_options.get_password())
        self.assertEqual(jdbc_connection_options.get_user_name(),
                         connection_options.get_user_name())

        exec_options = JdbcExecutionOptions(get_private_field(j_output_format, 'executionOptions'))
        self.assertEqual(jdbc_execution_options.get_batch_interval_ms(),
                         exec_options.get_batch_interval_ms())
        self.assertEqual(jdbc_execution_options.get_batch_size(),
                         exec_options.get_batch_size())
        self.assertEqual(jdbc_execution_options.get_max_retries(),
                         exec_options.get_max_retries())

    def tearDown(self):
        if self._cxt_clz_loader is not None:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(self._cxt_clz_loader)


class ConnectorTests(PyFlinkTestCase):

    def setUp(self) -> None:
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.test_sink = DataStreamTestSinkFunction()

    def test_stream_file_sink(self):
        self.env.set_parallelism(2)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.map(
            lambda a: a[0],
            Types.STRING()).add_sink(
            StreamingFileSink.for_row_format(self.tempdir, SimpleStringEncoder())
                .with_rolling_policy(
                    DefaultRollingPolicy.builder().with_rollover_interval(15 * 60 * 1000)
                .with_inactivity_interval(5 * 60 * 1000)
                .with_max_part_size(1024 * 1024 * 1024).build())
                .with_output_file_config(
                    OutputFileConfig.OutputFileConfigBuilder()
                    .with_part_prefix("prefix")
                    .with_part_suffix("suffix").build()).build())

        self.env.execute("test_streaming_file_sink")

        results = []
        import os
        for root, dirs, files in os.walk(self.tempdir, topdown=True):
            for file in files:
                self.assertTrue(file.startswith('.prefix'))
                self.assertTrue('suffix' in file)
                path = root + "/" + file
                with open(path) as infile:
                    for line in infile:
                        results.append(line)

        expected = ['deeefg\n', 'bdc\n', 'ab\n', 'cfgs\n']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def tearDown(self) -> None:
        self.test_sink.clear()
