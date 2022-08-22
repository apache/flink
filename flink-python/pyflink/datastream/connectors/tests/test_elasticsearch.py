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
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, \
    FlushBackoffType, ElasticsearchEmitter
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase
from pyflink.util.java_utils import get_field_value, is_instance_of


class FlinkElasticsearch7Test(PyFlinkStreamingTestCase):

    def test_es_sink(self):
        ds = self.env.from_collection(
            [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
            type_info=Types.MAP(Types.STRING(), Types.STRING()))

        es_sink = Elasticsearch7SinkBuilder() \
            .set_emitter(ElasticsearchEmitter.static_index('foo', 'id')) \
            .set_hosts(['localhost:9200']) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .set_bulk_flush_max_actions(1) \
            .set_bulk_flush_max_size_mb(2) \
            .set_bulk_flush_interval(1000) \
            .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000) \
            .set_connection_username('foo') \
            .set_connection_password('bar') \
            .set_connection_path_prefix('foo-bar') \
            .set_connection_request_timeout(30000) \
            .set_connection_timeout(31000) \
            .set_socket_timeout(32000) \
            .build()

        j_emitter = get_field_value(es_sink.get_java_function(), 'emitter')
        self.assertTrue(
            is_instance_of(
                j_emitter,
                'org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter'))
        self.assertEqual(
            get_field_value(
                es_sink.get_java_function(), 'hosts')[0].toString(), 'http://localhost:9200')
        self.assertEqual(
            get_field_value(
                es_sink.get_java_function(), 'deliveryGuarantee').toString(), 'at-least-once')

        j_build_bulk_processor_config = get_field_value(
            es_sink.get_java_function(), 'buildBulkProcessorConfig')
        self.assertEqual(j_build_bulk_processor_config.getBulkFlushMaxActions(), 1)
        self.assertEqual(j_build_bulk_processor_config.getBulkFlushMaxMb(), 2)
        self.assertEqual(j_build_bulk_processor_config.getBulkFlushInterval(), 1000)
        self.assertEqual(j_build_bulk_processor_config.getFlushBackoffType().toString(), 'CONSTANT')
        self.assertEqual(j_build_bulk_processor_config.getBulkFlushBackoffRetries(), 3)
        self.assertEqual(j_build_bulk_processor_config.getBulkFlushBackOffDelay(), 3000)

        j_network_client_config = get_field_value(
            es_sink.get_java_function(), 'networkClientConfig')
        self.assertEqual(j_network_client_config.getUsername(), 'foo')
        self.assertEqual(j_network_client_config.getPassword(), 'bar')
        self.assertEqual(j_network_client_config.getConnectionRequestTimeout(), 30000)
        self.assertEqual(j_network_client_config.getConnectionTimeout(), 31000)
        self.assertEqual(j_network_client_config.getSocketTimeout(), 32000)
        self.assertEqual(j_network_client_config.getConnectionPathPrefix(), 'foo-bar')

        ds.sink_to(es_sink).name('es sink')

    def test_es_sink_dynamic(self):
        ds = self.env.from_collection(
            [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
            type_info=Types.MAP(Types.STRING(), Types.STRING()))

        es_dynamic_index_sink = Elasticsearch7SinkBuilder() \
            .set_emitter(ElasticsearchEmitter.dynamic_index('name', 'id')) \
            .set_hosts(['localhost:9200']) \
            .build()

        j_emitter = get_field_value(es_dynamic_index_sink.get_java_function(), 'emitter')
        self.assertTrue(
            is_instance_of(
                j_emitter,
                'org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter'))

        ds.sink_to(es_dynamic_index_sink).name('es dynamic index sink')

    def test_es_sink_key_none(self):
        ds = self.env.from_collection(
            [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
            type_info=Types.MAP(Types.STRING(), Types.STRING()))

        es_sink = Elasticsearch7SinkBuilder() \
            .set_emitter(ElasticsearchEmitter.static_index('foo')) \
            .set_hosts(['localhost:9200']) \
            .build()

        j_emitter = get_field_value(es_sink.get_java_function(), 'emitter')
        self.assertTrue(
            is_instance_of(
                j_emitter,
                'org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter'))

        ds.sink_to(es_sink).name('es sink')

    def test_es_sink_dynamic_key_none(self):
        ds = self.env.from_collection(
            [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
            type_info=Types.MAP(Types.STRING(), Types.STRING()))

        es_dynamic_index_sink = Elasticsearch7SinkBuilder() \
            .set_emitter(ElasticsearchEmitter.dynamic_index('name')) \
            .set_hosts(['localhost:9200']) \
            .build()

        j_emitter = get_field_value(es_dynamic_index_sink.get_java_function(), 'emitter')
        self.assertTrue(
            is_instance_of(
                j_emitter,
                'org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter'))

        ds.sink_to(es_dynamic_index_sink).name('es dynamic index sink')
