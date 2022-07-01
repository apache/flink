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
from pyflink.datastream.connectors import CassandraSink
from pyflink.datastream.connectors.cassandra import MapperOptions, ConsistencyLevel, \
    ClusterBuilder, LoadBalancingPolicy, ReconnectionPolicy, RetryPolicy, SpeculativeExecutionPolicy
from pyflink.datastream.connectors.tests.test_connectors import ConnectorTestBase


class CassandraSinkTest(ConnectorTestBase):
    @classmethod
    def _get_jars_relative_path(cls):
        return '/flink-connectors/flink-connector-cassandra'

    def test_cassandra_sink(self):
        type_info = Types.ROW([Types.STRING(), Types.INT()])
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deefg', 4)],
                                      type_info=type_info)
        cassandra_sink_builder = CassandraSink.add_sink(ds)

        cassandra_sink = cassandra_sink_builder \
            .set_host('localhost', 9876) \
            .set_query('query') \
            .enable_ignore_null_fields() \
            .set_mapper_options(MapperOptions()
                                .ttl(1)
                                .timestamp(100)
                                .tracing(True)
                                .if_not_exists(False)
                                .consistency_level(ConsistencyLevel.ANY)
                                .save_null_fields(True)) \
            .set_max_concurrent_requests(1000) \
            .build()

        cassandra_sink.name('cassandra_sink').set_parallelism(3)

        plan = eval(self.env.get_execution_plan())
        self.assertEqual("Sink: cassandra_sink", plan['nodes'][1]['type'])
        self.assertEqual(3, plan['nodes'][1]['parallelism'])

    def test_cassandra_sink_with_cluster(self):
        type_info = Types.ROW([Types.STRING(), Types.STRING()])
        ds = self.env.from_collection([('a', "1"), ('b', "2"), ('c', "3"), ('d', "4")],
                                      type_info=type_info)
        cassandra_sink_builder = CassandraSink.add_sink(ds)

        cluster_builder = ClusterBuilder() \
            .with_cluster_name("cluster name") \
            .with_port(9042) \
            .with_max_schema_agreement_wait_seconds(3600) \
            .add_contact_points("127.0.0.1", "127.0.0.2") \
            .with_load_balancing_policy(LoadBalancingPolicy.dc_aware_round_robin_policy()) \
            .with_reconnection_policy(ReconnectionPolicy.exponential_reconnection_policy()) \
            .with_retry_policy(RetryPolicy.consistency_retry_policy()) \
            .with_speculative_execution_policy(SpeculativeExecutionPolicy.
                                               constant_speculative_execution_policy(1, 2)) \
            .with_credentials("user", "pwd") \
            .without_metrics() \
            .without_jmx_reporting() \
            .with_no_compact()

        cassandra_sink = cassandra_sink_builder \
            .set_cluster_builder(cluster_builder) \
            .set_query('query') \
            .build()

        cassandra_sink.name('cassandra_sink').set_parallelism(3)

        plan = eval(self.env.get_execution_plan())
        self.assertEqual("Sink: cassandra_sink", plan['nodes'][1]['type'])
        self.assertEqual(3, plan['nodes'][1]['parallelism'])
