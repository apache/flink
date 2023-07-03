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

import logging
import sys

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.pulsar import PulsarSource, PulsarSink, StartCursor, \
    StopCursor, DeliveryGuarantee, TopicRoutingMode

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    PULSAR_SQL_CONNECTOR_PATH = 'file:///path/to/flink-sql-connector-pulsar-1.16.0.jar'
    SERVICE_URL = 'pulsar://localhost:6650'
    ADMIN_URL = 'http://localhost:8080'

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(PULSAR_SQL_CONNECTOR_PATH)

    pulsar_source = PulsarSource.builder() \
        .set_service_url(SERVICE_URL) \
        .set_admin_url(ADMIN_URL) \
        .set_topics('ada') \
        .set_start_cursor(StartCursor.latest()) \
        .set_unbounded_stop_cursor(StopCursor.never()) \
        .set_subscription_name('pyflink_subscription') \
        .set_deserialization_schema(SimpleStringSchema()) \
        .set_config('pulsar.source.enableAutoAcknowledgeMessage', True) \
        .set_properties({'pulsar.source.autoCommitCursorInterval': '1000'}) \
        .build()

    ds = env.from_source(source=pulsar_source,
                         watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                         source_name="pulsar source")

    pulsar_sink = PulsarSink.builder() \
        .set_service_url(SERVICE_URL) \
        .set_admin_url(ADMIN_URL) \
        .set_producer_name('pyflink_producer') \
        .set_topics('beta') \
        .set_serialization_schema(SimpleStringSchema()) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_topic_routing_mode(TopicRoutingMode.ROUND_ROBIN) \
        .set_config('pulsar.producer.maxPendingMessages', 1000) \
        .set_properties({'pulsar.producer.batchingMaxMessages': '100'}) \
        .build()

    ds.sink_to(pulsar_sink).name('pulsar sink')

    env.execute()
