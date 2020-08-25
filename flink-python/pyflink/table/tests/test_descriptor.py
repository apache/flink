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
import collections
import os
import sys

from pyflink.java_gateway import get_gateway
from pyflink.table.descriptors import (FileSystem, OldCsv, Rowtime, Schema, Kafka,
                                       Elasticsearch, Csv, Avro, Json, CustomConnectorDescriptor,
                                       CustomFormatDescriptor, HBase)
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import (PyFlinkTestCase, PyFlinkStreamTableTestCase,
                                             PyFlinkBatchTableTestCase,
                                             _load_specific_flink_module_jars)


class FileSystemDescriptorTests(PyFlinkTestCase):

    def test_path(self):
        file_system = FileSystem()

        file_system = file_system.path("/test.csv")

        properties = file_system.to_properties()
        expected = {'connector.property-version': '1',
                    'connector.type': 'filesystem',
                    'connector.path': '/test.csv'}
        self.assertEqual(expected, properties)


class KafkaDescriptorTests(PyFlinkTestCase):

    @classmethod
    def setUpClass(cls):
        super(KafkaDescriptorTests, cls).setUpClass()
        cls._cxt_clz_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()
        _load_specific_flink_module_jars('/flink-connectors/flink-connector-kafka-base')

    def test_version(self):
        kafka = Kafka().version("0.11")

        properties = kafka.to_properties()
        expected = {'connector.version': '0.11',
                    'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_topic(self):
        kafka = Kafka().topic("topic1")

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.topic': 'topic1',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_properties(self):
        kafka = Kafka().properties({"bootstrap.servers": "localhost:9092"})

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.properties.bootstrap.servers': 'localhost:9092',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_property(self):
        kafka = Kafka().property("group.id", "testGroup")

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.properties.group.id': 'testGroup',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_start_from_earliest(self):
        kafka = Kafka().start_from_earliest()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'earliest-offset',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_start_from_latest(self):
        kafka = Kafka().start_from_latest()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'latest-offset',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_start_from_group_offsets(self):
        kafka = Kafka().start_from_group_offsets()

        properties = kafka.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_start_from_specific_offsets(self):
        kafka = Kafka().start_from_specific_offsets({1: 220, 3: 400})

        properties = kafka.to_properties()
        expected = {'connector.startup-mode': 'specific-offsets',
                    'connector.specific-offsets': 'partition:1,offset:220;partition:3,offset:400',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_start_from_specific_offset(self):
        kafka = Kafka().start_from_specific_offset(3, 300)

        properties = kafka.to_properties()
        expected = {'connector.startup-mode': 'specific-offsets',
                    'connector.specific-offsets': 'partition:3,offset:300',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_sink_partitioner_fixed(self):
        kafka = Kafka().sink_partitioner_fixed()

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'fixed',
                    'connector.startup-mode': 'group-offsets',
                    'connector.type': 'kafka',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_sink_partitioner_custom(self):
        kafka = Kafka().sink_partitioner_custom(
            "org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner")

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'custom',
                    'connector.sink-partitioner-class':
                        'org.apache.flink.streaming.connectors.kafka.partitioner.'
                        'FlinkFixedPartitioner',
                    'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_sink_partitioner_round_robin(self):
        kafka = Kafka().sink_partitioner_round_robin()

        properties = kafka.to_properties()
        expected = {'connector.sink-partitioner': 'round-robin',
                    'connector.type': 'kafka',
                    'connector.startup-mode': 'group-offsets',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    @classmethod
    def tearDownClass(cls):
        if cls._cxt_clz_loader is not None:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(cls._cxt_clz_loader)


class ElasticsearchDescriptorTest(PyFlinkTestCase):

    @classmethod
    def setUpClass(cls):
        super(ElasticsearchDescriptorTest, cls).setUpClass()
        cls._cxt_clz_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()
        _load_specific_flink_module_jars('/flink-connectors/flink-connector-elasticsearch-base')

    def test_version(self):
        elasticsearch = Elasticsearch().version("6")

        properties = elasticsearch.to_properties()
        expected = {'connector.type': 'elasticsearch',
                    'connector.version': '6',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_host(self):
        elasticsearch = Elasticsearch().host("localhost", 9200, "http")

        properties = elasticsearch.to_properties()
        expected = {'connector.hosts': 'http://localhost:9200',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_index(self):
        elasticsearch = Elasticsearch().index("MyUsers")

        properties = elasticsearch.to_properties()
        expected = {'connector.index': 'MyUsers',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_document_type(self):
        elasticsearch = Elasticsearch().document_type("user")

        properties = elasticsearch.to_properties()
        expected = {'connector.document-type': 'user',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_key_delimiter(self):
        elasticsearch = Elasticsearch().key_delimiter("$")

        properties = elasticsearch.to_properties()
        expected = {'connector.key-delimiter': '$',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_key_null_literal(self):
        elasticsearch = Elasticsearch().key_null_literal("n/a")

        properties = elasticsearch.to_properties()
        expected = {'connector.key-null-literal': 'n/a',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_failure_handler_fail(self):
        elasticsearch = Elasticsearch().failure_handler_fail()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'fail',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_failure_handler_ignore(self):
        elasticsearch = Elasticsearch().failure_handler_ignore()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'ignore',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_failure_handler_retry_rejected(self):
        elasticsearch = Elasticsearch().failure_handler_retry_rejected()

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'retry-rejected',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_failure_handler_custom(self):
        elasticsearch = Elasticsearch().failure_handler_custom(
            "org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler")

        properties = elasticsearch.to_properties()
        expected = {'connector.failure-handler': 'custom',
                    'connector.failure-handler-class':
                        'org.apache.flink.streaming.connectors.elasticsearch.util.'
                        'IgnoringFailureHandler',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_disable_flush_on_checkpoint(self):
        elasticsearch = Elasticsearch().disable_flush_on_checkpoint()

        properties = elasticsearch.to_properties()
        expected = {'connector.flush-on-checkpoint': 'false',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_max_actions(self):
        elasticsearch = Elasticsearch().bulk_flush_max_actions(42)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.max-actions': '42',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_max_size(self):
        elasticsearch = Elasticsearch().bulk_flush_max_size("42 mb")

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.max-size': '42 mb',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_interval(self):
        elasticsearch = Elasticsearch().bulk_flush_interval(2000)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.interval': '2000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_backoff_exponential(self):
        elasticsearch = Elasticsearch().bulk_flush_backoff_exponential()

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.type': 'exponential',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_backoff_constant(self):
        elasticsearch = Elasticsearch().bulk_flush_backoff_constant()

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.type': 'constant',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_backoff_max_retries(self):
        elasticsearch = Elasticsearch().bulk_flush_backoff_max_retries(3)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.max-retries': '3',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_bulk_flush_backoff_delay(self):
        elasticsearch = Elasticsearch().bulk_flush_backoff_delay(30000)

        properties = elasticsearch.to_properties()
        expected = {'connector.bulk-flush.backoff.delay': '30000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_connection_max_retry_timeout(self):
        elasticsearch = Elasticsearch().connection_max_retry_timeout(3000)

        properties = elasticsearch.to_properties()
        expected = {'connector.connection-max-retry-timeout': '3000',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_connection_path_prefix(self):
        elasticsearch = Elasticsearch().connection_path_prefix("/v1")

        properties = elasticsearch.to_properties()
        expected = {'connector.connection-path-prefix': '/v1',
                    'connector.type': 'elasticsearch',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    @classmethod
    def tearDownClass(cls):
        if cls._cxt_clz_loader is not None:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(cls._cxt_clz_loader)


class CustomConnectorDescriptorTests(PyFlinkTestCase):

    def test_custom_connector(self):
        custom_connector = CustomConnectorDescriptor('kafka', 1, True) \
            .property('connector.topic', 'topic1')\
            .properties({'connector.version': '0.11', 'connector.startup-mode': 'earliest-offset'})

        properties = custom_connector.to_properties()
        expected = {'connector.type': 'kafka',
                    'connector.property-version': '1',
                    'connector.topic': 'topic1',
                    'connector.version': '0.11',
                    'connector.startup-mode': 'earliest-offset'}
        self.assertEqual(expected, properties)


class HBaseDescriptorTests(PyFlinkTestCase):

    @classmethod
    def setUpClass(cls):
        super(HBaseDescriptorTests, cls).setUpClass()
        cls._cxt_clz_loader = get_gateway().jvm.Thread.currentThread().getContextClassLoader()
        _load_specific_flink_module_jars('/flink-connectors/flink-connector-hbase-base')

    def test_version(self):
        hbase = HBase().version("1.4.3")

        properties = hbase.to_properties()
        expected = {'connector.version': '1.4.3',
                    'connector.type': 'hbase',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

        hbase = HBase().version(1.1)
        properties = hbase.to_properties()
        expected = {'connector.version': '1.1',
                    'connector.type': 'hbase',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_table_name(self):
        hbase = HBase().table_name('tableName1')

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.table-name': 'tableName1',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_zookeeper_quorum(self):
        hbase = HBase().zookeeper_quorum("localhost:2181,localhost:2182")

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.zookeeper.quorum': 'localhost:2181,localhost:2182',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_zookeeper_node_parent(self):
        hbase = HBase().zookeeper_node_parent('/hbase/example-root-znode')

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.zookeeper.znode.parent': '/hbase/example-root-znode',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_write_buffer_flush_max_size(self):
        hbase = HBase().write_buffer_flush_max_size('1000')

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.write.buffer-flush.max-size': '1000 bytes',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

        hbase = HBase().write_buffer_flush_max_size(1000)
        properties = hbase.to_properties()
        self.assertEqual(expected, properties)

        hbase = HBase().write_buffer_flush_max_size('10mb')
        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.write.buffer-flush.max-size': '10 mb',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_write_buffer_flush_max_rows(self):
        hbase = HBase().write_buffer_flush_max_rows(10)

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.write.buffer-flush.max-rows': '10',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_write_buffer_flush_interval(self):
        hbase = HBase().write_buffer_flush_interval('123')

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.write.buffer-flush.interval': '123',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

        hbase = HBase().write_buffer_flush_interval(123)

        properties = hbase.to_properties()
        self.assertEqual(expected, properties)

        hbase = HBase().write_buffer_flush_interval('123ms')

        properties = hbase.to_properties()
        expected = {'connector.type': 'hbase',
                    'connector.write.buffer-flush.interval': '123ms',
                    'connector.property-version': '1'}
        self.assertEqual(expected, properties)

    @classmethod
    def tearDownClass(cls):
        if cls._cxt_clz_loader is not None:
            get_gateway().jvm.Thread.currentThread().setContextClassLoader(cls._cxt_clz_loader)


class OldCsvDescriptorTests(PyFlinkTestCase):

    def test_field_delimiter(self):
        csv = OldCsv().field_delimiter("|")

        properties = csv.to_properties()
        expected = {'format.field-delimiter': '|',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_line_delimiter(self):
        csv = OldCsv().line_delimiter(";")

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.line-delimiter': ';'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_ignore_parse_errors(self):
        csv = OldCsv().ignore_parse_errors()

        properties = csv.to_properties()
        expected = {'format.ignore-parse-errors': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_quote_character(self):
        csv = OldCsv().quote_character("*")

        properties = csv.to_properties()
        expected = {'format.quote-character': '*',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_comment_prefix(self):
        csv = OldCsv().comment_prefix("#")

        properties = csv.to_properties()
        expected = {'format.comment-prefix': '#',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_ignore_first_line(self):
        csv = OldCsv().ignore_first_line()

        properties = csv.to_properties()
        expected = {'format.ignore-first-line': 'true',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_field(self):
        csv = OldCsv()

        csv.field("a", DataTypes.BIGINT())
        csv.field("b", DataTypes.STRING())
        csv.field("c", "SQL_TIMESTAMP")

        properties = csv.to_properties()
        expected = {'format.fields.0.name': 'a',
                    'format.fields.0.data-type': 'BIGINT',
                    'format.fields.1.name': 'b',
                    'format.fields.1.data-type': 'VARCHAR(2147483647)',
                    'format.fields.2.name': 'c',
                    'format.fields.2.data-type': 'TIMESTAMP(3)',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_schema(self):
        csv = OldCsv()
        schema = TableSchema(["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

        csv = csv.schema(schema)

        properties = csv.to_properties()
        expected = {'format.fields.0.name': 'a',
                    'format.fields.0.data-type': 'INT',
                    'format.fields.1.name': 'b',
                    'format.fields.1.data-type': 'VARCHAR(2147483647)',
                    'format.type': 'csv',
                    'format.property-version': '1'}

        self.assertEqual(expected, properties)


class CsvDescriptorTests(PyFlinkTestCase):

    def test_field_delimiter(self):
        csv = Csv().field_delimiter("|")

        properties = csv.to_properties()
        expected = {'format.field-delimiter': '|',
                    'format.type': 'csv',
                    'format.property-version': '1'}
        self.assertEqual(expected, properties)

    def test_line_delimiter(self):
        csv = Csv().line_delimiter(";")

        expected = {'format.line-delimiter': ';',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_quote_character(self):
        csv = Csv().quote_character("'")

        expected = {'format.quote-character': "'",
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_allow_comments(self):
        csv = Csv().allow_comments()

        expected = {'format.allow-comments': 'true',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_ignore_parse_errors(self):
        csv = Csv().ignore_parse_errors()

        expected = {'format.ignore-parse-errors': 'true',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_array_element_delimiter(self):
        csv = Csv().array_element_delimiter("/")

        expected = {'format.array-element-delimiter': '/',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_escape_character(self):
        csv = Csv().escape_character("\\")

        expected = {'format.escape-character': '\\',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_null_literal(self):
        csv = Csv().null_literal("null")

        expected = {'format.null-literal': 'null',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_schema(self):
        csv = Csv().schema(DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                          DataTypes.FIELD("b", DataTypes.STRING())]))

        expected = {'format.schema': 'ROW<a INT, b VARCHAR>',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)

    def test_derive_schema(self):
        csv = Csv().derive_schema()

        expected = {'format.derive-schema': 'true',
                    'format.property-version': '1',
                    'format.type': 'csv'}

        properties = csv.to_properties()
        self.assertEqual(expected, properties)


class AvroDescriptorTest(PyFlinkTestCase):

    def test_record_class(self):
        avro = Avro().record_class("org.apache.flink.formats.avro.generated.Address")

        expected = {'format.record-class': 'org.apache.flink.formats.avro.generated.Address',
                    'format.property-version': '1',
                    'format.type': 'avro'}

        properties = avro.to_properties()
        self.assertEqual(expected, properties)

    def test_avro_schema(self):
        avro = Avro().avro_schema(
            '{"type":"record",'
            '"name":"Address",'
            '"namespace":"org.apache.flink.formats.avro.generated",'
            '"fields":['
            '{"name":"num","type":"int"},'
            '{"name":"street","type":"string"},'
            '{"name":"city","type":"string"},'
            '{"name":"state","type":"string"},'
            '{"name":"zip","type":"string"}'
            ']}')

        expected = {'format.avro-schema': '{"type":"record",'
                                          '"name":"Address",'
                                          '"namespace":"org.apache.flink.formats.avro.generated",'
                                          '"fields":['
                                          '{"name":"num","type":"int"},'
                                          '{"name":"street","type":"string"},'
                                          '{"name":"city","type":"string"},'
                                          '{"name":"state","type":"string"},'
                                          '{"name":"zip","type":"string"}'
                                          ']}',
                    'format.property-version': '1',
                    'format.type': 'avro'}

        properties = avro.to_properties()
        self.assertEqual(expected, properties)


class JsonDescriptorTests(PyFlinkTestCase):

    def test_fail_on_missing_field_true(self):
        json = Json().fail_on_missing_field(True)

        expected = {'format.fail-on-missing-field': 'true',
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = json.to_properties()
        self.assertEqual(expected, properties)

    def test_ignore_parse_errors(self):
        json = Json().ignore_parse_errors(True)

        expected = {'format.ignore-parse-errors': 'true',
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = json.to_properties()
        self.assertEqual(expected, properties)

    def test_json_schema(self):
        json = Json().json_schema(
            "{"
            "'title': 'Fruit',"
            "'type': 'object',"
            "'properties': "
            "{"
            "'name': {'type': 'string'},"
            "'count': {'type': 'integer'},"
            "'time': "
            "{"
            "'description': 'row time',"
            "'type': 'string',"
            "'format': 'date-time'"
            "}"
            "},"
            "'required': ['name', 'count', 'time']"
            "}")

        expected = {'format.json-schema':
                    "{"
                    "'title': 'Fruit',"
                    "'type': 'object',"
                    "'properties': {"
                    "'name': {'type': 'string'},"
                    "'count': {'type': 'integer'},"
                    "'time': {"
                    "'description': 'row time',"
                    "'type': 'string',"
                    "'format': 'date-time'}"
                    "},"
                    "'required': ['name', 'count', 'time']}",
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = json.to_properties()
        self.assertEqual(expected, properties)

    def test_schema(self):
        json = Json().schema(DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                            DataTypes.FIELD("b", DataTypes.STRING())]))

        expected = {'format.schema': 'ROW<a INT, b VARCHAR>',
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = json.to_properties()
        self.assertEqual(expected, properties)

    def test_derive_schema(self):
        json = Json().derive_schema()

        expected = {'format.derive-schema': 'true',
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = json.to_properties()
        self.assertEqual(expected, properties)


class CustomFormatDescriptorTests(PyFlinkTestCase):

    def test_custom_format_descriptor(self):
        custom_format = CustomFormatDescriptor('json', 1) \
            .property('format.schema', 'ROW<a INT, b VARCHAR>') \
            .properties({'format.fail-on-missing-field': 'true'})

        expected = {'format.fail-on-missing-field': 'true',
                    'format.schema': 'ROW<a INT, b VARCHAR>',
                    'format.property-version': '1',
                    'format.type': 'json'}

        properties = custom_format.to_properties()
        self.assertEqual(expected, properties)


class RowTimeDescriptorTests(PyFlinkTestCase):

    def test_timestamps_from_field(self):
        rowtime = Rowtime().timestamps_from_field("rtime")

        properties = rowtime.to_properties()
        expected = {'rowtime.timestamps.type': 'from-field', 'rowtime.timestamps.from': 'rtime'}
        self.assertEqual(expected, properties)

    def test_timestamps_from_source(self):
        rowtime = Rowtime().timestamps_from_source()

        properties = rowtime.to_properties()
        expected = {'rowtime.timestamps.type': 'from-source'}
        self.assertEqual(expected, properties)

    def test_timestamps_from_extractor(self):
        rowtime = Rowtime().timestamps_from_extractor(
            "org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor")

        properties = rowtime.to_properties()
        expected = {
            'rowtime.timestamps.type': 'custom',
            'rowtime.timestamps.class':
                'org.apache.flink.table.descriptors.RowtimeTest$CustomExtractor',
            'rowtime.timestamps.serialized':
                'rO0ABXNyAD5vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3JzLlJvd3RpbWVUZXN0JEN1c3R'
                'vbUV4dHJhY3RvcoaChjMg55xwAgABTAAFZmllbGR0ABJMamF2YS9sYW5nL1N0cmluZzt4cgA-b3JnLm'
                'FwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLnRzZXh0cmFjdG9ycy5UaW1lc3RhbXBFeHRyYWN0b3Jf1'
                'Y6piFNsGAIAAHhwdAACdHM'}
        self.assertEqual(expected, properties)

    def test_watermarks_periodic_ascending(self):
        rowtime = Rowtime().watermarks_periodic_ascending()

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'periodic-ascending'}
        self.assertEqual(expected, properties)

    def test_watermarks_periodic_bounded(self):
        rowtime = Rowtime().watermarks_periodic_bounded(1000)

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'periodic-bounded',
                    'rowtime.watermarks.delay': '1000'}
        self.assertEqual(expected, properties)

    def test_watermarks_from_source(self):
        rowtime = Rowtime().watermarks_from_source()

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'from-source'}
        self.assertEqual(expected, properties)

    def test_watermarks_from_strategy(self):
        rowtime = Rowtime().watermarks_from_strategy(
            "org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner")

        properties = rowtime.to_properties()
        expected = {
            'rowtime.watermarks.type': 'custom',
            'rowtime.watermarks.class':
                'org.apache.flink.table.descriptors.RowtimeTest$CustomAssigner',
            'rowtime.watermarks.serialized':
                'rO0ABXNyAD1vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLmRlc2NyaXB0b3JzLlJvd3RpbWVUZXN0JEN1c3R'
                'vbUFzc2lnbmVyeDcuDvfbu0kCAAB4cgBHb3JnLmFwYWNoZS5mbGluay50YWJsZS5zb3VyY2VzLndtc3'
                'RyYXRlZ2llcy5QdW5jdHVhdGVkV2F0ZXJtYXJrQXNzaWduZXKBUc57oaWu9AIAAHhyAD1vcmcuYXBhY'
                '2hlLmZsaW5rLnRhYmxlLnNvdXJjZXMud21zdHJhdGVnaWVzLldhdGVybWFya1N0cmF0ZWd53nt-g2OW'
                'aT4CAAB4cA'}
        self.assertEqual(expected, properties)


class SchemaDescriptorTests(PyFlinkTestCase):

    def test_field(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("string_field", DataTypes.STRING())\
            .field("timestamp_field", DataTypes.TIMESTAMP(3))\
            .field("time_field", DataTypes.TIME())\
            .field("date_field", DataTypes.DATE())\
            .field("double_field", DataTypes.DOUBLE())\
            .field("float_field", DataTypes.FLOAT())\
            .field("byte_field", DataTypes.TINYINT())\
            .field("short_field", DataTypes.SMALLINT())\
            .field("boolean_field", DataTypes.BOOLEAN())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

    def test_fields(self):
        fields = collections.OrderedDict([
            ("int_field", DataTypes.INT()),
            ("long_field", DataTypes.BIGINT()),
            ("string_field", DataTypes.STRING()),
            ("timestamp_field", DataTypes.TIMESTAMP(3)),
            ("time_field", DataTypes.TIME()),
            ("date_field", DataTypes.DATE()),
            ("double_field", DataTypes.DOUBLE()),
            ("float_field", DataTypes.FLOAT()),
            ("byte_field", DataTypes.TINYINT()),
            ("short_field", DataTypes.SMALLINT()),
            ("boolean_field", DataTypes.BOOLEAN())
        ])

        schema = Schema().fields(fields)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

        if sys.version_info[:2] <= (3, 5):
            fields = {
                "int_field": DataTypes.INT(),
                "long_field": DataTypes.BIGINT(),
                "string_field": DataTypes.STRING(),
                "timestamp_field": DataTypes.TIMESTAMP(3),
                "time_field": DataTypes.TIME(),
                "date_field": DataTypes.DATE(),
                "double_field": DataTypes.DOUBLE(),
                "float_field": DataTypes.FLOAT(),
                "byte_field": DataTypes.TINYINT(),
                "short_field": DataTypes.SMALLINT(),
                "boolean_field": DataTypes.BOOLEAN()
            }
            self.assertRaises(TypeError, Schema().fields, fields)

    def test_field_in_string(self):
        schema = Schema()\
            .field("int_field", 'INT')\
            .field("long_field", 'BIGINT')\
            .field("string_field", 'VARCHAR')\
            .field("timestamp_field", 'SQL_TIMESTAMP')\
            .field("time_field", 'SQL_TIME')\
            .field("date_field", 'SQL_DATE')\
            .field("double_field", 'DOUBLE')\
            .field("float_field", 'FLOAT')\
            .field("byte_field", 'TINYINT')\
            .field("short_field", 'SMALLINT')\
            .field("boolean_field", 'BOOLEAN')

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

    def test_from_origin_field(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT()).from_origin_field("origin_field_a")\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.1.from': 'origin_field_a',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_proctime(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("ptime", DataTypes.BIGINT()).proctime()\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'ptime',
                    'schema.1.data-type': 'BIGINT',
                    'schema.1.proctime': 'true',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_rowtime(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("rtime", DataTypes.BIGINT())\
            .rowtime(
                Rowtime().timestamps_from_field("long_field").watermarks_periodic_bounded(5000))\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        print(properties)
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'rtime',
                    'schema.2.data-type': 'BIGINT',
                    'schema.2.rowtime.timestamps.type': 'from-field',
                    'schema.2.rowtime.timestamps.from': 'long_field',
                    'schema.2.rowtime.watermarks.type': 'periodic-bounded',
                    'schema.2.rowtime.watermarks.delay': '5000',
                    'schema.3.name': 'string_field',
                    'schema.3.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_schema(self):
        table_schema = TableSchema(["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

        schema = Schema().schema(table_schema)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'b',
                    'schema.1.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)


class AbstractTableDescriptorTests(object):

    def test_with_format(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor.with_format(OldCsv().field("a", "INT"))

        properties = descriptor.to_properties()

        expected = {'format.type': 'csv',
                    'format.property-version': '1',
                    'format.fields.0.name': 'a',
                    'format.fields.0.data-type': 'INT',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_with_schema(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor.with_format(OldCsv()).with_schema(Schema().field("a", "INT"))

        properties = descriptor.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.data-type': 'INT',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.type': 'filesystem',
                    'connector.property-version': '1'}
        assert properties == expected

    def test_register_temporary_table(self):
        self.env.set_parallelism(1)
        source_path = os.path.join(self.tempdir + '/streaming.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]
        data = [(1, "Hi", "Hello"), (2, "Hello", "Hello")]
        self.prepare_csv_source(source_path, data, field_types, field_names)
        sink_path = os.path.join(self.tempdir + '/streaming2.csv')
        if os.path.isfile(sink_path):
            os.remove(sink_path)
        t_env = self.t_env

        t_env.connect(FileSystem().path(source_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .create_temporary_table("source")
        t_env.connect(FileSystem().path(sink_path))\
             .with_format(OldCsv()
                          .field_delimiter(',')
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .with_schema(Schema()
                          .field("a", DataTypes.INT())
                          .field("b", DataTypes.STRING())
                          .field("c", DataTypes.STRING()))\
             .create_temporary_table("sink")
        t_env.from_path("source").select("a + 1, b, c").execute_insert("sink").wait()

        with open(sink_path, 'r') as f:
            lines = f.read()
            assert lines == '2,Hi,Hello\n' + "3,Hello,Hello\n"


class StreamTableDescriptorTests(PyFlinkStreamTableTestCase, AbstractTableDescriptorTests):

    def test_in_append_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor\
            .with_format(OldCsv())\
            .in_append_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'append',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_retract_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor \
            .with_format(OldCsv()) \
            .in_retract_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'retract',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected

    def test_in_upsert_mode(self):
        descriptor = self.t_env.connect(FileSystem())

        descriptor = descriptor \
            .with_format(OldCsv()) \
            .in_upsert_mode()

        properties = descriptor.to_properties()
        expected = {'update-mode': 'upsert',
                    'format.type': 'csv',
                    'format.property-version': '1',
                    'connector.property-version': '1',
                    'connector.type': 'filesystem'}
        assert properties == expected


class BatchTableDescriptorTests(PyFlinkBatchTableTestCase, AbstractTableDescriptorTests):
    pass


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
