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
from pyflink.common import SimpleStringSchema, Types
from pyflink.datastream.connectors.kinesis import PartitionKeyGenerator, FlinkKinesisConsumer, \
    KinesisStreamsSink, KinesisFirehoseSink
from pyflink.testing.test_case_utils import PyFlinkUTTestCase
from pyflink.util.java_utils import get_field_value


class FlinkKinesisTest(PyFlinkUTTestCase):

    def test_kinesis_source(self):
        consumer_config = {
            'aws.region': 'us-east-1',
            'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
            'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
            'flink.stream.initpos': 'LATEST'
        }

        kinesis_source = FlinkKinesisConsumer("stream-1", SimpleStringSchema(), consumer_config)

        ds = self.env.add_source(source_func=kinesis_source, source_name="kinesis source")
        ds.print()
        plan = eval(self.env.get_execution_plan())
        self.assertEqual('Source: kinesis source', plan['nodes'][0]['type'])
        self.assertEqual(
            get_field_value(kinesis_source.get_java_function(), 'streams')[0], 'stream-1')

    def test_kinesis_streams_sink(self):
        sink_properties = {
            'aws.region': 'us-east-1',
            'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key'
        }

        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        kinesis_streams_sink = KinesisStreamsSink.builder() \
            .set_kinesis_client_properties(sink_properties) \
            .set_serialization_schema(SimpleStringSchema()) \
            .set_partition_key_generator(PartitionKeyGenerator.fixed()) \
            .set_stream_name("stream-1") \
            .set_fail_on_error(False) \
            .set_max_batch_size(500) \
            .set_max_in_flight_requests(50) \
            .set_max_buffered_requests(10000) \
            .set_max_batch_size_in_bytes(5 * 1024 * 1024) \
            .set_max_time_in_buffer_ms(5000) \
            .set_max_record_size_in_bytes(1 * 1024 * 1024) \
            .build()

        ds.sink_to(kinesis_streams_sink).name('kinesis streams sink')
        plan = eval(self.env.get_execution_plan())

        self.assertEqual('kinesis streams sink: Writer', plan['nodes'][1]['type'])
        self.assertEqual(get_field_value(kinesis_streams_sink.get_java_function(), 'failOnError'),
                         False)
        self.assertEqual(
            get_field_value(kinesis_streams_sink.get_java_function(), 'streamName'), 'stream-1')

    def test_kinesis_firehose_sink(self):

        sink_properties = {
            'aws.region': 'eu-west-1',
            'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
            'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key'
        }

        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        kinesis_firehose_sink = KinesisFirehoseSink.builder() \
            .set_firehose_client_properties(sink_properties) \
            .set_serialization_schema(SimpleStringSchema()) \
            .set_delivery_stream_name('stream-1') \
            .set_fail_on_error(False) \
            .set_max_batch_size(500) \
            .set_max_in_flight_requests(50) \
            .set_max_buffered_requests(10000) \
            .set_max_batch_size_in_bytes(5 * 1024 * 1024) \
            .set_max_time_in_buffer_ms(5000) \
            .set_max_record_size_in_bytes(1 * 1024 * 1024) \
            .build()

        ds.sink_to(kinesis_firehose_sink).name('kinesis firehose sink')
        plan = eval(self.env.get_execution_plan())

        self.assertEqual('kinesis firehose sink: Writer', plan['nodes'][1]['type'])
        self.assertEqual(get_field_value(kinesis_firehose_sink.get_java_function(), 'failOnError'),
                         False)
        self.assertEqual(
            get_field_value(kinesis_firehose_sink.get_java_function(), 'deliveryStreamName'),
            'stream-1')
