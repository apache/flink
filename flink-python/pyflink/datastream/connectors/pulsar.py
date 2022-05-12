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
from enum import Enum
from typing import Dict, Union, List

from pyflink.common import DeserializationSchema, TypeInformation, ExecutionConfig, ConfigOption
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway


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
    def flink_type_info(type_information: TypeInformation, execution_config: ExecutionConfig) \
            -> 'PulsarDeserializationSchema':
        """
        Create a PulsarDeserializationSchema by using the given TypeInformation. This method is
        only used for treating message that was written into pulsar by TypeInformation.
        """
        JPulsarDeserializationSchema = get_gateway().jvm.org.apache.flink \
            .connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema
        _j_execution_config = execution_config._j_execution_config \
            if execution_config is not None else None
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
        JStartCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor
        return StartCursor(JStartCursor.fromMessageTime(timestamp))


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
        JStopCursor = get_gateway().jvm \
            .org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor
        return StopCursor(JStopCursor.atEventTime(timestamp))


class PulsarSource(Source):
    """
    The Source implementation of Pulsar. Please use a PulsarSourceBuilder to construct a
    PulsarSource. The following example shows how to create a PulsarSource emitting records of
    String type.

    Example:
    ::

        >>> source = PulsarSource() \\
        ...     .builder() \\
        ...     .set_topics(TOPIC1, TOPIC2) \\
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
        ...     .set_bounded_stop_cursor(StopCursor.at_event_time(int(time.time() * 1000)))
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
        setTopics or setTopicPattern in this builder.
        """
        self._j_pulsar_source_builder.setTopicPattern(topics_pattern)
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

    def set_config(self, key: ConfigOption, value) -> 'PulsarSourceBuilder':
        """
        Set arbitrary properties for the PulsarSource and PulsarConsumer. The valid keys can be
        found in PulsarSourceOptions and PulsarOptions.

        Make sure the option could be set only once or with same value.
        """
        self._j_pulsar_source_builder.setConfig(key._j_config_option, value)
        return self

    def set_config_with_dict(self, config: Dict) -> 'PulsarSourceBuilder':
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
