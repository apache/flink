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
import warnings
from enum import Enum
from typing import Dict, Union, List

from pyflink.common import DeserializationSchema, TypeInformation, ExecutionConfig, \
    ConfigOptions, Duration, SerializationSchema, ConfigOption
from pyflink.datastream.connectors import Source, Sink, DeliveryGuarantee
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import load_java_class


__all__ = [
    'PulsarSource',
    'PulsarSourceBuilder',
    'PulsarDeserializationSchema',
    'SubscriptionType',
    'StartCursor',
    'StopCursor',
    'PulsarSink',
    'PulsarSinkBuilder',
    'PulsarSerializationSchema',
    'MessageDelayer',
    'TopicRoutingMode'
]


# ---- PulsarSource ----


class PulsarDeserializationSchema(object):
    """
    A schema bridge for deserializing the pulsar's Message into a flink managed instance. We
    support both the pulsar's self managed schema and flink managed schema.
    """

    def __init__(self, _j_pulsar_deserialization_schema):
        self._j_pulsar_deserialization_schema = _j_pulsar_deserialization_schema

    @staticmethod
    def flink_schema(deserialization_schema: DeserializationSchema) \
            -> 'PulsarDeserializationSchema':
        """
        Create a PulsarDeserializationSchema by using the flink's DeserializationSchema. It would
        consume the pulsar message as byte array and decode the message by using flink's logic.
        """
        JPulsarDeserializationSchema = get_gateway().jvm.org.apache.flink \
            .connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema
        _j_pulsar_deserialization_schema = JPulsarDeserializationSchema.flinkSchema(
            deserialization_schema._j_deserialization_schema)
        return PulsarDeserializationSchema(_j_pulsar_deserialization_schema)

    @staticmethod
    def flink_type_info(type_information: TypeInformation,
                        execution_config: ExecutionConfig = None) -> 'PulsarDeserializationSchema':
        """
        Create a PulsarDeserializationSchema by using the given TypeInformation. This method is
        only used for treating message that was written into pulsar by TypeInformation.
        """
        JPulsarDeserializationSchema = get_gateway().jvm.org.apache.flink \
            .connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema
        JExecutionConfig = get_gateway().jvm.org.apache.flink.api.common.ExecutionConfig
        _j_execution_config = execution_config._j_execution_config \
            if execution_config is not None else JExecutionConfig()
        _j_pulsar_deserialization_schema = JPulsarDeserializationSchema.flinkTypeInfo(
            type_information.get_java_type_info(), _j_execution_config)
        return PulsarDeserializationSchema(_j_pulsar_deserialization_schema)


class SubscriptionType(Enum):
    """
    Types of subscription supported by Pulsar.

    :data: `Exclusive`:

    There can be only 1 consumer on the same topic with the same subscription name.

    :data: `Shared`:

    Multiple consumer will be able to use the same subscription name and the messages will be
    dispatched according to a round-robin rotation between the connected consumers. In this mode,
    the consumption order is not guaranteed.

    :data: `Failover`:

    Multiple consumer will be able to use the same subscription name but only 1 consumer will
    receive the messages. If that consumer disconnects, one of the other connected consumers will
    start receiving messages. In failover mode, the consumption ordering is guaranteed. In case of
    partitioned topics, the ordering is guaranteed on a per-partition basis. The partitions
    assignments will be split across the available consumers. On each partition, at most one
    consumer will be active at a given point in time.

    :data: `Key_Shared`:

    Multiple consumer will be able to use the same subscription and all messages with the same key
    will be dispatched to only one consumer. Use ordering_key to overwrite the message key for
    message ordering.
    """

    Exclusive = 0,
    Shared = 1,
    Failover = 2,
    Key_Shared = 3

    def _to_j_subscription_type(self):
        JSubscriptionType = get_gateway().jvm.org.apache.pulsar.client.api.SubscriptionType
        return getattr(JSubscriptionType, self.name)


class StartCursor(object):
    """
    A factory class for users to specify the start position of a pulsar subscription.
    Since it would be serialized into split.
    The implementation for this interface should be well considered.
    I don't recommend adding extra internal state for this implementation.

    This class would be used only for SubscriptionType.Exclusive and SubscriptionType.Failover.
    """

    def __init__(self, _j_start_cursor):
        self._j_start_cursor = _j_start_cursor

    @staticmethod
    def default_start_cursor() -> 'StartCursor':
        return StartCursor.earliest()

    @staticmethod
    def earliest() -> 'StartCursor':
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        return StartCursor(JStartCursor.earliest())

    @staticmethod
    def latest() -> 'StartCursor':
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        return StartCursor(JStartCursor.latest())

    @staticmethod
    def from_message_time(timestamp: int) -> 'StartCursor':
        """
        This method is designed for seeking message from event time. But Pulsar didn't support
        seeking from message time, instead, it would seek the position from publish time. We only
        keep this method for backward compatible.
        """
        warnings.warn("Deprecated in 1.16, use from_publish_time() instead.", DeprecationWarning)
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        return StartCursor(JStartCursor.fromMessageTime(timestamp))

    @staticmethod
    def from_message_id(message_id: bytes, inclusive: bool = True) -> 'StartCursor':
        """
        Find the available message id and start consuming from it. User could call pulsar Python
        library serialize method to cover messageId bytes.

        Example:
        ::

            >>> from pulsar import MessageId
            >>> message_id_bytes = MessageId().serialize()
            >>> start_cursor = StartCursor.from_message_id(message_id_bytes)
        """
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        j_message_id = get_gateway().jvm.org.apache.pulsar.client.api.MessageId \
            .fromByteArray(message_id)
        return StartCursor(JStartCursor.fromMessageId(j_message_id, inclusive))

    @staticmethod
    def from_publish_time(timestamp: int) -> 'StartCursor':
        """
        Seek the start position by using message publish time.
        """
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        return StartCursor(JStartCursor.fromPublishTime(timestamp))


class StopCursor(object):
    """
    A factory class for users to specify the stop position of a pulsar subscription. Since it would
    be serialized into split. The implementation for this interface should be well considered. I
    don't recommend adding extra internal state for this implementation.
    """

    def __init__(self, _j_stop_cursor):
        self._j_stop_cursor = _j_stop_cursor

    @staticmethod
    def default_stop_cursor() -> 'StopCursor':
        return StopCursor.never()

    @staticmethod
    def never() -> 'StopCursor':
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.never())

    @staticmethod
    def latest() -> 'StopCursor':
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.latest())

    @staticmethod
    def at_event_time(timestamp: int) -> 'StopCursor':
        """
        Stop consuming when message eventTime is greater than or equals the specified timestamp.
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.atEventTime(timestamp))

    @staticmethod
    def after_event_time(timestamp: int) -> 'StopCursor':
        """
        Stop consuming when message eventTime is greater than the specified timestamp.
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.afterEventTime(timestamp))

    @staticmethod
    def at_publish_time(timestamp: int) -> 'StopCursor':
        """
        Stop consuming when message publishTime is greater than or equals the specified timestamp.
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.atPublishTime(timestamp))

    @staticmethod
    def after_publish_time(timestamp: int) -> 'StopCursor':
        """
        Stop consuming when message publishTime is greater than the specified timestamp.
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.afterPublishTime(timestamp))

    @staticmethod
    def at_message_id(message_id: bytes) -> 'StopCursor':
        """
        Stop consuming when the messageId is equal or greater than the specified messageId.
        Message that is equal to the specified messageId will not be consumed. User could call
        pulsar Python library serialize method to cover messageId bytes.

        Example:
        ::

            >>> from pulsar import MessageId
            >>> message_id_bytes = MessageId().serialize()
            >>> stop_cursor = StopCursor.at_message_id(message_id_bytes)
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        j_message_id = get_gateway().jvm.org.apache.pulsar.client.api.MessageId \
            .fromByteArray(message_id)
        return StopCursor(JStopCursor.atMessageId(j_message_id))

    @staticmethod
    def after_message_id(message_id: bytes) -> 'StopCursor':
        """
        Stop consuming when the messageId is greater than the specified messageId. Message that is
        equal to the specified messageId will be consumed. User could call pulsar Python library
        serialize method to cover messageId bytes.

        Example:
        ::

            >>> from pulsar import MessageId
            >>> message_id_bytes = MessageId().serialize()
            >>> stop_cursor = StopCursor.after_message_id(message_id_bytes)
        """
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        j_message_id = get_gateway().jvm.org.apache.pulsar.client.api.MessageId \
            .fromByteArray(message_id)
        return StopCursor(JStopCursor.afterMessageId(j_message_id))


class PulsarSource(Source):
    """
    The Source implementation of Pulsar. Please use a PulsarSourceBuilder to construct a
    PulsarSource. The following example shows how to create a PulsarSource emitting records of
    String type.

    Example:
    ::

        >>> source = PulsarSource() \\
        ...     .builder() \\
        ...     .set_topics([TOPIC1, TOPIC2]) \\
        ...     .set_service_url(get_service_url()) \\
        ...     .set_admin_url(get_admin_url()) \\
        ...     .set_subscription_name("test") \\
        ...     .set_deserialization_schema(
        ...         PulsarDeserializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .set_bounded_stop_cursor(StopCursor.default_stop_cursor()) \\
        ...     .build()

    See PulsarSourceBuilder for more details.
    """

    def __init__(self, j_pulsar_source):
        super(PulsarSource, self).__init__(source=j_pulsar_source)

    @staticmethod
    def builder() -> 'PulsarSourceBuilder':
        """
        Get a PulsarSourceBuilder to builder a PulsarSource.
        """
        return PulsarSourceBuilder()


class PulsarSourceBuilder(object):
    """
    The builder class for PulsarSource to make it easier for the users to construct a PulsarSource.

    The following example shows the minimum setup to create a PulsarSource that reads the String
    values from a Pulsar topic.

    Example:
    ::

        >>> source = PulsarSource() \\
        ...     .builder() \\
        ...     .set_service_url(PULSAR_BROKER_URL) \\
        ...     .set_admin_url(PULSAR_BROKER_HTTP_URL) \\
        ...     .set_subscription_name("flink-source-1") \\
        ...     .set_topics([TOPIC1, TOPIC2]) \\
        ...     .set_deserialization_schema(
        ...         PulsarDeserializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .build()

    The service url, admin url, subscription name, topics to consume, and the record deserializer
    are required fields that must be set.

    To specify the starting position of PulsarSource, one can call set_start_cursor(StartCursor).

    By default the PulsarSource runs in an Boundedness.CONTINUOUS_UNBOUNDED mode and never stop
    until the Flink job is canceled or fails. To let the PulsarSource run in
    Boundedness.CONTINUOUS_UNBOUNDED but stops at some given offsets, one can call
    set_unbounded_stop_cursor(StopCursor).

    For example the following PulsarSource stops after it consumes up to a event time when the
    Flink started.

    Example:
    ::

        >>> source = PulsarSource() \\
        ...     .builder() \\
        ...     .set_service_url(PULSAR_BROKER_URL) \\
        ...     .set_admin_url(PULSAR_BROKER_HTTP_URL) \\
        ...     .set_subscription_name("flink-source-1") \\
        ...     .set_topics([TOPIC1, TOPIC2]) \\
        ...     .set_deserialization_schema(
        ...         PulsarDeserializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .set_bounded_stop_cursor(StopCursor.at_publish_time(int(time.time() * 1000)))
        ...     .build()
    """

    def __init__(self):
        JPulsarSource = \
            get_gateway().jvm.org.apache.flink.connector.pulsar.source.PulsarSource
        self._j_pulsar_source_builder = JPulsarSource.builder()

    def set_admin_url(self, admin_url: str) -> 'PulsarSourceBuilder':
        """
        Sets the admin endpoint for the PulsarAdmin of the PulsarSource.
        """
        self._j_pulsar_source_builder.setAdminUrl(admin_url)
        return self

    def set_service_url(self, service_url: str) -> 'PulsarSourceBuilder':
        """
        Sets the server's link for the PulsarConsumer of the PulsarSource.
        """
        self._j_pulsar_source_builder.setServiceUrl(service_url)
        return self

    def set_subscription_name(self, subscription_name: str) -> 'PulsarSourceBuilder':
        """
        Sets the name for this pulsar subscription.
        """
        self._j_pulsar_source_builder.setSubscriptionName(subscription_name)
        return self

    def set_subscription_type(self, subscription_type: SubscriptionType) -> 'PulsarSourceBuilder':
        """
        SubscriptionType is the consuming behavior for pulsar, we would generator different split
        by the given subscription type. Please take some time to consider which subscription type
        matches your application best. Default is SubscriptionType.Shared.
        """
        self._j_pulsar_source_builder.setSubscriptionType(
            subscription_type._to_j_subscription_type())
        return self

    def set_topics(self, topics: Union[str, List[str]]) -> 'PulsarSourceBuilder':
        """
        Set a pulsar topic list for flink source. Some topic may not exist currently, consuming this
        non-existed topic wouldn't throw any exception. But the best solution is just consuming by
        using a topic regex. You can set topics once either with setTopics or setTopicPattern in
        this builder.
        """
        if not isinstance(topics, list):
            topics = [topics]
        self._j_pulsar_source_builder.setTopics(topics)
        return self

    def set_topics_pattern(self, topics_pattern: str) -> 'PulsarSourceBuilder':
        """
        Set a topic pattern to consume from the java regex str. You can set topics once either with
        set_topics or set_topic_pattern in this builder.
        """
        warnings.warn("set_topics_pattern is deprecated. Use set_topic_pattern instead.",
                      DeprecationWarning, stacklevel=2)
        self._j_pulsar_source_builder.setTopicPattern(topics_pattern)
        return self

    def set_topic_pattern(self, topic_pattern: str) -> 'PulsarSourceBuilder':
        """
        Set a topic pattern to consume from the java regex str. You can set topics once either with
        set_topics or set_topic_pattern in this builder.
        """
        self._j_pulsar_source_builder.setTopicPattern(topic_pattern)
        return self

    def set_start_cursor(self, start_cursor: StartCursor) -> 'PulsarSourceBuilder':
        """
        Specify from which offsets the PulsarSource should start consume from by providing an
        StartCursor.
        """
        self._j_pulsar_source_builder.setStartCursor(start_cursor._j_start_cursor)
        return self

    def set_unbounded_stop_cursor(self, stop_cursor: StopCursor) -> 'PulsarSourceBuilder':
        """
        By default the PulsarSource is set to run in Boundedness.CONTINUOUS_UNBOUNDED manner and
        thus never stops until the Flink job fails or is canceled. To let the PulsarSource run as a
        streaming source but still stops at some point, one can set an StopCursor to specify the
        stopping offsets for each partition. When all the partitions have reached their stopping
        offsets, the PulsarSource will then exit.

        This method is different from set_bounded_stop_cursor(StopCursor) that after setting the
        stopping offsets with this method, PulsarSource.getBoundedness() will still return
        Boundedness.CONTINUOUS_UNBOUNDED even though it will stop at the stopping offsets specified
        by the stopping offsets StopCursor.
        """
        self._j_pulsar_source_builder.setUnboundedStopCursor(stop_cursor._j_stop_cursor)
        return self

    def set_bounded_stop_cursor(self, stop_cursor: StopCursor) -> 'PulsarSourceBuilder':
        """
        By default the PulsarSource is set to run in Boundedness.CONTINUOUS_UNBOUNDED manner and
        thus never stops until the Flink job fails or is canceled. To let the PulsarSource run in
        Boundedness.BOUNDED manner and stops at some point, one can set an StopCursor to specify
        the stopping offsets for each partition. When all the partitions have reached their stopping
        offsets, the PulsarSource will then exit.

        This method is different from set_unbounded_stop_cursor(StopCursor) that after setting the
        stopping offsets with this method, PulsarSource.getBoundedness() will return
        Boundedness.BOUNDED instead of Boundedness.CONTINUOUS_UNBOUNDED.
        """
        self._j_pulsar_source_builder.setBoundedStopCursor(stop_cursor._j_stop_cursor)
        return self

    def set_deserialization_schema(self,
                                   pulsar_deserialization_schema: PulsarDeserializationSchema) \
            -> 'PulsarSourceBuilder':
        """
        DeserializationSchema is required for getting the Schema for deserialize message from
        pulsar and getting the TypeInformation for message serialization in flink.

        We have defined a set of implementations, using PulsarDeserializationSchema#flink_type_info
        or PulsarDeserializationSchema#flink_schema for creating the desired schema.
        """
        self._j_pulsar_source_builder.setDeserializationSchema(
            pulsar_deserialization_schema._j_pulsar_deserialization_schema)
        return self

    def set_config(self, key: Union[str, ConfigOption], value) -> 'PulsarSourceBuilder':
        """
        Set arbitrary properties for the PulsarSource and PulsarConsumer. The valid keys can be
        found in PulsarSourceOptions and PulsarOptions.

        Make sure the option could be set only once or with same value.
        """
        if isinstance(key, ConfigOption):
            warnings.warn("set_config(key: ConfigOption, value) is deprecated. "
                          "Use set_config(key: str, value) instead.",
                          DeprecationWarning, stacklevel=2)
            j_config_option = key._j_config_option
        else:
            j_config_option = \
                ConfigOptions.key(key).string_type().no_default_value()._j_config_option
        self._j_pulsar_source_builder.setConfig(j_config_option, value)
        return self

    def set_config_with_dict(self, config: Dict) -> 'PulsarSourceBuilder':
        """
        Set arbitrary properties for the PulsarSource and PulsarConsumer. The valid keys can be
        found in PulsarSourceOptions and PulsarOptions.
        """
        warnings.warn("set_config_with_dict is deprecated. Use set_properties instead.",
                      DeprecationWarning, stacklevel=2)
        self.set_properties(config)
        return self

    def set_properties(self, config: Dict) -> 'PulsarSourceBuilder':
        """
        Set arbitrary properties for the PulsarSource and PulsarConsumer. The valid keys can be
        found in PulsarSourceOptions and PulsarOptions.
        """
        JConfiguration = get_gateway().jvm.org.apache.flink.configuration.Configuration
        self._j_pulsar_source_builder.setConfig(JConfiguration.fromMap(config))
        return self

    def build(self) -> 'PulsarSource':
        """
        Build the PulsarSource.
        """
        return PulsarSource(self._j_pulsar_source_builder.build())


# ---- PulsarSink ----


class PulsarSerializationSchema(object):
    """
    The serialization schema for how to serialize records into Pulsar.
    """

    def __init__(self, _j_pulsar_serialization_schema):
        self._j_pulsar_serialization_schema = _j_pulsar_serialization_schema

    @staticmethod
    def flink_schema(serialization_schema: SerializationSchema) \
            -> 'PulsarSerializationSchema':
        """
        Create a PulsarSerializationSchema by using the flink's SerializationSchema. It would
        serialize the message into byte array and send it to Pulsar with Schema#BYTES.
        """
        JPulsarSerializationSchema = get_gateway().jvm.org.apache.flink \
            .connector.pulsar.sink.writer.serializer.PulsarSerializationSchema
        _j_pulsar_serialization_schema = JPulsarSerializationSchema.flinkSchema(
            serialization_schema._j_serialization_schema)
        return PulsarSerializationSchema(_j_pulsar_serialization_schema)


class TopicRoutingMode(Enum):
    """
    The routing policy for choosing the desired topic by the given message.

    :data: `ROUND_ROBIN`:

    The producer will publish messages across all partitions in a round-robin fashion to achieve
    maximum throughput. Please note that round-robin is not done per individual message but
    rather it's set to the same boundary of batching delay, to ensure batching is effective.

    :data: `MESSAGE_KEY_HASH`:

    If no key is provided, The partitioned producer will randomly pick one single topic partition
    and publish all the messages into that partition. If a key is provided on the message, the
    partitioned producer will hash the key and assign the message to a particular partition.

    :data: `CUSTOM`:

    Use custom topic router implementation that will be called to determine the partition for a
    particular message.
    """

    ROUND_ROBIN = 0
    MESSAGE_KEY_HASH = 1
    CUSTOM = 2

    def _to_j_topic_routing_mode(self):
        JTopicRoutingMode = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode
        return getattr(JTopicRoutingMode, self.name)


class MessageDelayer(object):
    """
    A delayer for Pulsar broker passing the sent message to the downstream consumer. This is only
    works in :data:`SubscriptionType.Shared` subscription.

    Read delayed message delivery
    https://pulsar.apache.org/docs/en/next/concepts-messaging/#delayed-message-delivery for better
    understanding this feature.
    """
    def __init__(self, _j_message_delayer):
        self._j_message_delayer = _j_message_delayer

    @staticmethod
    def never() -> 'MessageDelayer':
        """
        All the messages should be consumed immediately.
        """
        JMessageDelayer = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer
        return MessageDelayer(JMessageDelayer.never())

    @staticmethod
    def fixed(duration: Duration) -> 'MessageDelayer':
        """
        All the messages should be consumed in a fixed duration.
        """
        JMessageDelayer = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer
        return MessageDelayer(JMessageDelayer.fixed(duration._j_duration))


class PulsarSink(Sink):
    """
    The Sink implementation of Pulsar. Please use a PulsarSinkBuilder to construct a
    PulsarSink. The following example shows how to create a PulsarSink receiving records of
    String type.

    Example:
    ::

        >>> sink = PulsarSink.builder() \\
        ...     .set_service_url(PULSAR_BROKER_URL) \\
        ...     .set_admin_url(PULSAR_BROKER_HTTP_URL) \\
        ...     .set_topics(topic) \\
        ...     .set_serialization_schema(
        ...         PulsarSerializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .build()

    The sink supports all delivery guarantees described by DeliveryGuarantee.

    DeliveryGuarantee#NONE does not provide any guarantees: messages may be lost in
    case of issues on the Pulsar broker and messages may be duplicated in case of a Flink
    failure.

    DeliveryGuarantee#AT_LEAST_ONCE the sink will wait for all outstanding records in
    the Pulsar buffers to be acknowledged by the Pulsar producer on a checkpoint. No messages
    will be lost in case of any issue with the Pulsar brokers but messages may be duplicated
    when Flink restarts.

    DeliveryGuarantee#EXACTLY_ONCE: In this mode the PulsarSink will write all messages
    in a Pulsar transaction that will be committed to Pulsar on a checkpoint. Thus, no
    duplicates will be seen in case of a Flink restart. However, this delays record writing
    effectively until a checkpoint is written, so adjust the checkpoint duration accordingly.
    Additionally, it is highly recommended to tweak Pulsar transaction timeout (link) >>
    maximum checkpoint duration + maximum restart duration or data loss may happen when Pulsar
    expires an uncommitted transaction.

    See PulsarSinkBuilder for more details.
    """

    def __init__(self, j_pulsar_sink):
        super(PulsarSink, self).__init__(sink=j_pulsar_sink)

    @staticmethod
    def builder() -> 'PulsarSinkBuilder':
        """
        Get a PulsarSinkBuilder to builder a PulsarSink.
        """
        return PulsarSinkBuilder()


class PulsarSinkBuilder(object):
    """
    The builder class for PulsarSink to make it easier for the users to construct a PulsarSink.

    The following example shows the minimum setup to create a PulsarSink that reads the String
    values from a Pulsar topic.

    Example:
    ::

        >>> sink = PulsarSink.builder() \\
        ...     .set_service_url(PULSAR_BROKER_URL) \\
        ...     .set_admin_url(PULSAR_BROKER_HTTP_URL) \\
        ...     .set_topics([TOPIC1, TOPIC2]) \\
        ...     .set_serialization_schema(
        ...         PulsarSerializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .build()

    The service url, admin url, and the record serializer are required fields that must be set. If
    you don't set the topics, make sure you have provided a custom TopicRouter. Otherwise,
    you must provide the topics to produce.

    To specify the delivery guarantees of PulsarSink, one can call
    #setDeliveryGuarantee(DeliveryGuarantee). The default value of the delivery guarantee is
    DeliveryGuarantee#NONE, and it wouldn't promise the consistence when write the message into
    Pulsar.

    Example:
    ::

        >>> sink = PulsarSink.builder() \\
        ...     .set_service_url(PULSAR_BROKER_URL) \\
        ...     .set_admin_url(PULSAR_BROKER_HTTP_URL) \\
        ...     .set_topics([TOPIC1, TOPIC2]) \\
        ...     .set_serialization_schema(
        ...         PulsarSerializationSchema.flink_schema(SimpleStringSchema())) \\
        ...     .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        ...     .build()
    """

    def __init__(self):
        JPulsarSink = get_gateway().jvm.org.apache.flink.connector.pulsar.sink.PulsarSink
        self._j_pulsar_sink_builder = JPulsarSink.builder()

    def set_admin_url(self, admin_url: str) -> 'PulsarSinkBuilder':
        """
        Sets the admin endpoint for the PulsarAdmin of the PulsarSink.
        """
        self._j_pulsar_sink_builder.setAdminUrl(admin_url)
        return self

    def set_service_url(self, service_url: str) -> 'PulsarSinkBuilder':
        """
        Sets the server's link for the PulsarProducer of the PulsarSink.
        """
        self._j_pulsar_sink_builder.setServiceUrl(service_url)
        return self

    def set_producer_name(self, producer_name: str) -> 'PulsarSinkBuilder':
        """
        The producer name is informative, and it can be used to identify a particular producer
        instance from the topic stats.
        """
        self._j_pulsar_sink_builder.setProducerName(producer_name)
        return self

    def set_topics(self, topics: Union[str, List[str]]) -> 'PulsarSinkBuilder':
        """
        Set a pulsar topic list for flink sink. Some topic may not exist currently, write to this
        non-existed topic wouldn't throw any exception.
        """
        if not isinstance(topics, list):
            topics = [topics]
        self._j_pulsar_sink_builder.setTopics(topics)
        return self

    def set_delivery_guarantee(self, delivery_guarantee: DeliveryGuarantee) -> 'PulsarSinkBuilder':
        """
        Sets the wanted the DeliveryGuarantee. The default delivery guarantee is
        DeliveryGuarantee#NONE.
        """
        self._j_pulsar_sink_builder.setDeliveryGuarantee(
            delivery_guarantee._to_j_delivery_guarantee())
        return self

    def set_topic_routing_mode(self, topic_routing_mode: TopicRoutingMode) -> 'PulsarSinkBuilder':
        """
        Set a routing mode for choosing right topic partition to send messages.
        """
        self._j_pulsar_sink_builder.setTopicRoutingMode(
            topic_routing_mode._to_j_topic_routing_mode())
        return self

    def set_topic_router(self, topic_router_class_name: str) -> 'PulsarSinkBuilder':
        """
        Use a custom topic router instead predefine topic routing.
        """
        j_topic_router = load_java_class(topic_router_class_name).newInstance()
        self._j_pulsar_sink_builder.setTopicRouter(j_topic_router)
        return self

    def set_serialization_schema(self, pulsar_serialization_schema: PulsarSerializationSchema) \
            -> 'PulsarSinkBuilder':
        """
        Sets the PulsarSerializationSchema that transforms incoming records to bytes.
        """
        self._j_pulsar_sink_builder.setSerializationSchema(
            pulsar_serialization_schema._j_pulsar_serialization_schema)
        return self

    def delay_sending_message(self, message_delayer: MessageDelayer) -> 'PulsarSinkBuilder':
        """
        Set a message delayer for enable Pulsar message delay delivery.
        """
        self._j_pulsar_sink_builder.delaySendingMessage(message_delayer._j_message_delayer)
        return self

    def set_config(self, key: str, value) -> 'PulsarSinkBuilder':
        """
        Set an arbitrary property for the PulsarSink and Pulsar Producer. The valid keys can be
        found in PulsarSinkOptions and PulsarOptions.

        Make sure the option could be set only once or with same value.
        """
        j_config_option = ConfigOptions.key(key).string_type().no_default_value()._j_config_option
        self._j_pulsar_sink_builder.setConfig(j_config_option, value)
        return self

    def set_properties(self, config: Dict) -> 'PulsarSinkBuilder':
        """
        Set an arbitrary property for the PulsarSink and Pulsar Producer. The valid keys can be
        found in PulsarSinkOptions and PulsarOptions.
        """
        JConfiguration = get_gateway().jvm.org.apache.flink.configuration.Configuration
        self._j_pulsar_sink_builder.setConfig(JConfiguration.fromMap(config))
        return self

    def build(self) -> 'PulsarSink':
        """
        Build the PulsarSink.
        """
        return PulsarSink(self._j_pulsar_sink_builder.build())
