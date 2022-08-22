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
from pyflink.common import SerializationSchema, DeserializationSchema
from pyflink.datastream.functions import SinkFunction, SourceFunction
from pyflink.java_gateway import get_gateway


__all__ = [
    'RMQConnectionConfig',
    'RMQSource',
    'RMQSink'
]


class RMQConnectionConfig(object):
    """
    Connection Configuration for RMQ.
    """

    def __init__(self, j_rmq_connection_config):
        self._j_rmq_connection_config = j_rmq_connection_config

    def get_host(self) -> str:
        return self._j_rmq_connection_config.getHost()

    def get_port(self) -> int:
        return self._j_rmq_connection_config.getPort()

    def get_virtual_host(self) -> str:
        return self._j_rmq_connection_config.getVirtualHost()

    def get_user_name(self) -> str:
        return self._j_rmq_connection_config.getUsername()

    def get_password(self) -> str:
        return self._j_rmq_connection_config.getPassword()

    def get_uri(self) -> str:
        return self._j_rmq_connection_config.getUri()

    def get_network_recovery_interval(self) -> int:
        return self._j_rmq_connection_config.getNetworkRecoveryInterval()

    def is_automatic_recovery(self) -> bool:
        return self._j_rmq_connection_config.isAutomaticRecovery()

    def is_topology_recovery(self) -> bool:
        return self._j_rmq_connection_config.isTopologyRecovery()

    def get_connection_timeout(self) -> int:
        return self._j_rmq_connection_config.getConnectionTimeout()

    def get_requested_channel_max(self) -> int:
        return self._j_rmq_connection_config.getRequestedChannelMax()

    def get_requested_frame_max(self) -> int:
        return self._j_rmq_connection_config.getRequestedFrameMax()

    def get_requested_heartbeat(self) -> int:
        return self._j_rmq_connection_config.getRequestedHeartbeat()

    class Builder(object):
        """
        Builder for RMQConnectionConfig.
        """

        def __init__(self):
            self._j_options_builder = get_gateway().jvm.org.apache.flink.streaming.connectors\
                .rabbitmq.common.RMQConnectionConfig.Builder()

        def set_port(self, port: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setPort(port)
            return self

        def set_host(self, host: str) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setHost(host)
            return self

        def set_virtual_host(self, vhost: str) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setVirtualHost(vhost)
            return self

        def set_user_name(self, user_name: str) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setUserName(user_name)
            return self

        def set_password(self, password: str) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setPassword(password)
            return self

        def set_uri(self, uri: str) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setUri(uri)
            return self

        def set_topology_recovery_enabled(
                self, topology_recovery_enabled: bool) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setTopologyRecoveryEnabled(topology_recovery_enabled)
            return self

        def set_requested_heartbeat(
                self, requested_heartbeat: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setRequestedHeartbeat(requested_heartbeat)
            return self

        def set_requested_frame_max(
                self, requested_frame_max: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setRequestedFrameMax(requested_frame_max)
            return self

        def set_requested_channel_max(
                self, requested_channel_max: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setRequestedChannelMax(requested_channel_max)
            return self

        def set_network_recovery_interval(
                self, network_recovery_interval: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setNetworkRecoveryInterval(network_recovery_interval)
            return self

        def set_connection_timeout(self, connection_timeout: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setConnectionTimeout(connection_timeout)
            return self

        def set_automatic_recovery(self, automatic_recovery: bool) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setAutomaticRecovery(automatic_recovery)
            return self

        def set_prefetch_count(self, prefetch_count: int) -> 'RMQConnectionConfig.Builder':
            self._j_options_builder.setPrefetchCount(prefetch_count)
            return self

        def build(self) -> 'RMQConnectionConfig':
            return RMQConnectionConfig(self._j_options_builder.build())


class RMQSource(SourceFunction):
    def __init__(self,
                 connection_config: 'RMQConnectionConfig',
                 queue_name: str,
                 use_correlation_id: bool,
                 deserialization_schema: DeserializationSchema
                 ):
        """
        Creates a new RabbitMQ source.

        For exactly-once, you must set the correlation ids of messages at the producer.
        The correlation id must be unique. Otherwise the behavior of the source is undefined.

        If in doubt, set use_correlation_id to False.

        When correlation ids are not used, this source has at-least-once processing semantics
        when checkpointing is enabled.

        :param connection_config: The RabbiMQ connection configuration.
        :param queue_name: The queue to receive messages from.
        :param use_correlation_id: Whether the messages received are supplied with a unique id
                                   to deduplicate messages (in case of failed acknowledgments).
                                   Only used when checkpointing is enabled.
        :param deserialization_schema: A deserializer used to convert between RabbitMQ's
                                       messages and Flink's objects.
        """
        JRMQSource = get_gateway().jvm.org.apache.flink.streaming.connectors.rabbitmq.RMQSource
        j_rmq_source = JRMQSource(
            connection_config._j_rmq_connection_config,
            queue_name,
            use_correlation_id,
            deserialization_schema._j_deserialization_schema
        )
        super(RMQSource, self).__init__(source_func=j_rmq_source)


class RMQSink(SinkFunction):
    def __init__(self, connection_config: 'RMQConnectionConfig',
                 queue_name: str, serialization_schema: SerializationSchema):
        """
        Creates a new RabbitMQ sink.

        :param connection_config: The RabbiMQ connection configuration.
        :param queue_name: The queue to publish messages to.
        :param serialization_schema: A serializer used to convert Flink objects to bytes.
        """
        JRMQSink = get_gateway().jvm.org.apache.flink.streaming.connectors.rabbitmq.RMQSink
        j_rmq_sink = JRMQSink(
            connection_config._j_rmq_connection_config,
            queue_name,
            serialization_schema._j_serialization_schema,
        )
        super(RMQSink, self).__init__(sink_func=j_rmq_sink)
