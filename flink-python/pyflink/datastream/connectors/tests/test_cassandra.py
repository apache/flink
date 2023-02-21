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
from pyflink.datastream.connectors.cassandra import CassandraSink, MapperOptions, ConsistencyLevel
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


class CassandraSinkTest(PyFlinkStreamingTestCase):

    def test_cassandra_sink(self):
        type_info = Types.ROW([Types.STRING(), Types.INT()])
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=type_info)
        cassandra_sink_builder = CassandraSink.add_sink(ds)

        cassandra_sink = cassandra_sink_builder\
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
