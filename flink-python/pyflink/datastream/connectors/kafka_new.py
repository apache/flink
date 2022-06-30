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
from typing import Set, Dict, overload, Optional

from py4j.java_gateway import JavaObject, get_java_class
from pyflink.common.serialization import DeserializationSchema

from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray

__all__ = [
    'KafkaOffsetResetStrategy',
    'KafkaOffsetsInitializer',
    'KafkaSource',
    'KafkaSourceBuilder',
    'KafkaTopicPartition',
]


class KafkaSource(Source):
    """
    The Source implementation of Kafka. Please use a :class:`KafkaSourceBuilder` to construct a
    :class:`KafkaSource`. The following example shows how to create a KafkaSource emitting records
    of String type.

    ::

        >>> source = KafkaSource \\
        ...     .builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_group_id('MY_GROUP') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \\
        ...     .build()

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_kafka_source: JavaObject):
        super().__init__(j_kafka_source)

    @staticmethod
    def builder() -> 'KafkaSourceBuilder':
        """
        Get a kafkaSourceBuilder to build a :class:`KafkaSource`.

        :return: a Kafka source builder.
        """
        return KafkaSourceBuilder()


class KafkaSourceBuilder(object):
    """
    The builder class for :class:`KafkaSource` to make it easier for the users to construct a
    :class:`KafkaSource`.

    The following example shows the minimum setup to create a KafkaSource that reads the String
    values from a Kafka topic.

    ::

        >>> source = KafkaSource.builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .build()

    The bootstrap servers, topics/partitions to consume, and the record deserializer are required
    fields that must be set.

    To specify the starting offsets of the KafkaSource, one can call :meth:`set_starting_offsets`.

    By default, the KafkaSource runs in an CONTINUOUS_UNBOUNDED mode and never stops until the Flink
    job is canceled or fails. To let the KafkaSource run in CONTINUOUS_UNBOUNDED but stops at some
    given offsets, one can call :meth:`set_stopping_offsets`. For example the following KafkaSource
    stops after it consumes up to the latest partition offsets at the point when the Flink started.

    ::

        >>> source = KafkaSource.builder() \\
        ...     .set_bootstrap_servers('MY_BOOTSTRAP_SERVERS') \\
        ...     .set_topics('TOPIC1', 'TOPIC2') \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .set_unbounded(KafkaOffsetsInitializer.latest()) \\
        ...     .build()

    .. versionadded:: 1.16.0
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm.org.apache.flink.connector.kafka.source \
            .KafkaSource.builder()

    def build(self) -> 'KafkaSource':
        return KafkaSource(self._j_builder.build())

    def set_bootstrap_servers(self, bootstrap_servers: str) -> 'KafkaSourceBuilder':
        """
        Sets the bootstrap servers for the KafkaConsumer of the KafkaSource.

        :param bootstrap_servers: the bootstrap servers of the Kafka cluster.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setBootstrapServers(bootstrap_servers)
        return self

    def set_group_id(self, group_id: str) -> 'KafkaSourceBuilder':
        """
        Sets the consumer group id of the KafkaSource.

        :param group_id: the group id of the KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setGroupId(group_id)
        return self

    def set_topics(self, *topics: str) -> 'KafkaSourceBuilder':
        """
        Set a list of topics the KafkaSource should consume from. All the topics in the list should
        have existed in the Kafka cluster. Otherwise, an exception will be thrown. To allow some
        topics to be created lazily, please use :meth:`set_topic_pattern` instead.

        :param topics: the list of topics to consume from.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setTopics(to_jarray(get_gateway().jvm.java.lang.String, topics))
        return self

    def set_topic_pattern(self, topic_pattern: str) -> 'KafkaSourceBuilder':
        """
        Set a topic pattern to consume from use the java Pattern. For grammar, check out
        `JavaDoc <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_ .

        :param topic_pattern: the pattern of the topic name to consume from.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setTopicPattern(get_gateway().jvm.java.util.regex
                                        .Pattern.compile(topic_pattern))
        return self

    def set_partitions(self, partitions: Set['KafkaTopicPartition']) -> 'KafkaSourceBuilder':
        """
        Set a set of partitions to consume from.

        Example:
        ::

            >>> KafkaSource.builder().set_partitions({
            ...     KafkaTopicPartition('TOPIC1', 0),
            ...     KafkaTopicPartition('TOPIC1', 1),
            ... })

        :param partitions: the set of partitions to consume from.
        :return: this KafkaSourceBuilder.
        """
        j_set = get_gateway().jvm.java.util.HashSet()
        for tp in partitions:
            j_set.add(tp._to_j_topic_partition())
        self._j_builder.setPartitions(j_set)
        return self

    def set_starting_offsets(self, starting_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        Specify from which offsets the KafkaSource should start consume from by providing an
        :class:`KafkaOffsetsInitializer`.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.earliest` - starting from the earliest offsets. This is
          also the default offset initializer of the KafkaSource for starting offsets.
        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param starting_offsets_initializer: the :class:`KafkaOffsetsInitializer` setting the
            starting offsets for the Source.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setStartingOffsets(starting_offsets_initializer._j_initializer)
        return self

    def set_unbounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        By default, the KafkaSource is set to run in CONTINUOUS_UNBOUNDED manner and thus never
        stops until the Flink job fails or is canceled. To let the KafkaSource run as a streaming
        source but still stops at some point, one can set an :class:`KafkaOffsetsInitializer`
        to specify the stopping offsets for each partition. When all the partitions have reached
        their stopping offsets, the KafkaSource will then exit.

        This method is different from :meth:`set_bounded` that after setting the stopping offsets
        with this method, KafkaSource will still be CONTINUOUS_UNBOUNDED even though it will stop at
        the stopping offsets specified by the stopping offset initializer.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param stopping_offsets_initializer: the :class:`KafkaOffsetsInitializer` to specify the
            stopping offsets.
        :return: this KafkaSourceBuilder
        """
        self._j_builder.setUnbounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_bounded(self, stopping_offsets_initializer: 'KafkaOffsetsInitializer') \
            -> 'KafkaSourceBuilder':
        """
        By default, the KafkaSource is set to run in CONTINUOUS_UNBOUNDED manner and thus never
        stops until the Flink job fails or is canceled. To let the KafkaSource run in BOUNDED manner
        and stop at some point, one can set an :class:`KafkaOffsetsInitializer` to specify the
        stopping offsets for each partition. When all the partitions have reached their stopping
        offsets, the KafkaSource will then exit.

        This method is different from :meth:`set_unbounded` that after setting the stopping offsets
        with this method, :meth:`KafkaSource.get_boundedness` will return BOUNDED instead of
        CONTINUOUS_UNBOUNDED.

        The following :class:`KafkaOffsetsInitializer` s are commonly used and provided out of the
        box. Currently, customized offset initializer is not supported in PyFlink.

        * :meth:`KafkaOffsetsInitializer.latest` - starting from the latest offsets.
        * :meth:`KafkaOffsetsInitializer.committedOffsets` - starting from the committed offsets of
          the consumer group. If there is no committed offsets, starting from the offsets
          specified by the :class:`KafkaOffsetResetStrategy`.
        * :meth:`KafkaOffsetsInitializer.offsets` - starting from the specified offsets for each
          partition.
        * :meth:`KafkaOffsetsInitializer.timestamp` - starting from the specified timestamp for each
          partition. Note that the guarantee here is that all the records in Kafka whose timestamp
          is greater than the given starting timestamp will be consumed. However, it is possible
          that some consumer records whose timestamp is smaller than the given starting timestamp
          are also consumed.

        :param stopping_offsets_initializer: the :class:`KafkaOffsetsInitializer` to specify the
            stopping offsets.
        :return: this KafkaSourceBuilder
        """
        self._j_builder.setBounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_value_only_deserializer(self, deserialization_schema: DeserializationSchema) \
            -> 'KafkaSourceBuilder':
        """
        Sets the :class:`~pyflink.common.serialization.DeserializationSchema` for deserializing the
        value of Kafka's ConsumerRecord. The other information (e.g. key) in a ConsumerRecord will
        be ignored.

        :param deserialization_schema: the :class:`DeserializationSchema` to use for
            deserialization.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setValueOnlyDeserializer(deserialization_schema._j_deserialization_schema)
        return self

    def set_client_id_prefix(self, prefix: str) -> 'KafkaSourceBuilder':
        """
        Sets the client id prefix of this KafkaSource.

        :param prefix: the client id prefix to use for this KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setClientIdPrefix(prefix)
        return self

    def set_property(self, key: str, value: str) -> 'KafkaSourceBuilder':
        """
        Set an arbitrary property for the KafkaSource and KafkaConsumer. The valid keys can be found
        in ConsumerConfig and KafkaSourceOptions.

        Note that the following keys will be overridden by the builder when the KafkaSource is
        created.

        * ``key.deserializer`` is always set to ByteArrayDeserializer.
        * ``value.deserializer`` is always set to ByteArrayDeserializer.
        * ``auto.offset.reset.strategy`` is overridden by AutoOffsetResetStrategy returned by
          :class:`KafkaOffsetsInitializer` for the starting offsets, which is by default
          :meth:`KafkaOffsetsInitializer.earliest`.
        * ``partition.discovery.interval.ms`` is overridden to -1 when :meth:`set_bounded` has been
          invoked.

        :param key: the key of the property.
        :param value: the value of the property.
        :return: this KafkaSourceBuilder.
        """
        self._j_builder.setProperty(key, value)
        return self


class KafkaTopicPartition(object):
    """
    Corresponding to Java ``org.apache.kafka.common.TopicPartition`` class.

    Example:
    ::

        >>> topic_partition = KafkaTopicPartition('TOPIC1', 0)

    .. versionadded:: 1.16.0
    """

    def __init__(self, topic: str, partition: int):
        self._topic = topic
        self._partition = partition

    def _to_j_topic_partition(self):
        jvm = get_gateway().jvm
        return jvm.org.apache.flink.kafka.shaded.org.apache.kafka.common.TopicPartition(
            self._topic, self._partition)

    def __eq__(self, other):
        if not isinstance(other, KafkaTopicPartition):
            return False
        return self._topic == other._topic and self._partition == other._partition

    def __hash__(self):
        return 31 * (31 + self._partition) + hash(self._topic)


class KafkaOffsetResetStrategy(Enum):
    """
    Corresponding to Java ``org.apache.kafka.client.consumer.OffsetResetStrategy`` class.

    .. versionadded:: 1.16.0
    """

    LATEST = 0
    EARLIEST = 1
    NONE = 2

    def _to_j_offset_reset_strategy(self):
        jvm = get_gateway().jvm
        if self.value == 0:
            return jvm.org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer \
                .OffsetResetStrategy.LATEST
        if self.value == 1:
            return jvm.org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer \
                .OffsetResetStrategy.EARLIEST
        if self.value == 2:
            return jvm.org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer \
                .OffsetResetStrategy.NONE


class KafkaOffsetsInitializer(object):
    """
    An interface for users to specify the starting / stopping offset of a KafkaPartitionSplit.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_initializer: JavaObject):
        self._j_initializer = j_initializer

    @staticmethod
    @overload
    def committed_offsets() -> 'KafkaOffsetsInitializer':
        pass

    @staticmethod
    @overload
    def committed_offsets(offset_reset_strategy: 'KafkaOffsetResetStrategy') \
            -> 'KafkaOffsetsInitializer':
        pass

    @staticmethod
    def committed_offsets(offset_reset_strategy: Optional['KafkaOffsetResetStrategy'] = None) \
            -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the committed
        offsets. An exception will be thrown at runtime if there is no committed offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets if
        the committed offsets does not exist.

        :param offset_reset_strategy: the offset reset strategy to use when the committed offsets do
            not exist.
        :return: an offset initializer which initialize the offsets to the committed offsets.
        """
        jvm = get_gateway().jvm
        if offset_reset_strategy is not None:
            return KafkaOffsetsInitializer(
                jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
                .OffsetsInitializer
                .committedOffsets(offset_reset_strategy._to_j_offset_reset_strategy()))
        else:
            return KafkaOffsetsInitializer(
                jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
                .OffsetsInitializer.committedOffsets())

    @staticmethod
    def timestamp(timestamp: int) -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets in each partition so
        that the initialized offset is the offset of the first record whose record timestamp is
        greater than or equals the give timestamp.

        :param timestamp: the timestamp to start the consumption.
        :return: an :class:`OffsetsInitializer` which initializes the offsets based on the given
            timestamp.
        """
        return KafkaOffsetsInitializer(get_gateway().jvm.org.apache.flink.connector.kafka.source
                                       .enumerator.initializer
                                       .OffsetsInitializer.timestamp(timestamp))

    @staticmethod
    def earliest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
        available offsets of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
            available offsets.
        """
        return KafkaOffsetsInitializer(get_gateway().jvm.org.apache.flink.connector.kafka.source
                                       .enumerator.initializer
                                       .OffsetsInitializer.earliest())

    @staticmethod
    def latest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest offsets
        of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest
            offsets.
        """
        return KafkaOffsetsInitializer(get_gateway().jvm.org.apache.flink.connector.kafka.source
                                       .enumerator.initializer.OffsetsInitializer.latest())

    @staticmethod
    @overload
    def offsets(offsets: Dict['KafkaTopicPartition', int]) -> 'KafkaOffsetsInitializer':
        pass

    @staticmethod
    @overload
    def offsets(offsets: Dict['KafkaTopicPartition', int],
                offset_reset_strategy: 'KafkaOffsetResetStrategy') -> 'KafkaOffsetsInitializer':
        pass

    @staticmethod
    def offsets(offsets: Dict['KafkaTopicPartition', int],
                offset_reset_strategy: Optional['KafkaOffsetResetStrategy'] = None) \
            -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
        offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets in
        case the specified offset is out of range.

        Example:
        ::

            >>> KafkaOffsetsInitializer.offsets({
            ...     KafkaTopicPartition('TOPIC1', 0): 0,
            ...     KafkaTopicPartition('TOPIC1', 1): 10000
            ... }, KafkaOffsetResetStrategy.EARLIEST)

        :param offsets: the specified offsets for each partition.
        :param offset_reset_strategy: the :class:`KafkaOffsetResetStrategy` to use when the
            specified offset is out of range.
        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
            offsets.
        """
        jvm = get_gateway().jvm
        j_primitive_long_class = get_java_class(jvm.long)
        j_long_class = get_java_class(jvm.Long)
        j_map = jvm.org.apache.flink.python.util.PythonConfigUtil.HashMapHelper(
            j_long_class.getDeclaredConstructor(to_jarray(jvm.Class, [j_primitive_long_class])),
            False
        )
        for tp, offset in offsets.items():
            j_map.put(tp._to_j_topic_partition(), to_jarray(jvm.Object, [offset]))

        if offset_reset_strategy is not None:
            return KafkaOffsetsInitializer(
                jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
                .OffsetsInitializer
                .offsets(j_map.getMap(), offset_reset_strategy._to_j_offset_reset_strategy()))
        else:
            return KafkaOffsetsInitializer(
                jvm.org.apache.flink.connector.kafka.source.enumerator.initializer
                .OffsetsInitializer.offsets(j_map.getMap()))
