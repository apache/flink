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
import abc
import warnings
from enum import Enum
from typing import Dict, Union, List, Set

from py4j.java_gateway import JavaObject, get_java_class

from pyflink.common import DeserializationSchema, TypeInformation, typeinfo, SerializationSchema
from pyflink.datastream.connectors import Source
from pyflink.datastream.functions import SinkFunction, SourceFunction
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


__all__ = [
    'KafkaOffsetResetStrategy',
    'KafkaOffsetsInitializer',
    'KafkaSource',
    'KafkaSourceBuilder',
    'KafkaTopicPartition',
]


# ---- FlinkKafkaConsumer ----

class FlinkKafkaConsumerBase(SourceFunction, abc.ABC):
    """
    Base class of all Flink Kafka Consumer data sources. This implements the common behavior across
    all kafka versions.

    The Kafka version specific behavior is defined mainly in the specific subclasses.
    """

    def __init__(self, j_flink_kafka_consumer):
        super(FlinkKafkaConsumerBase, self).__init__(source_func=j_flink_kafka_consumer)

    def set_commit_offsets_on_checkpoints(self,
                                          commit_on_checkpoints: bool) -> 'FlinkKafkaConsumerBase':
        """
        Specifies whether or not the consumer should commit offsets back to kafka on checkpoints.
        This setting will only have effect if checkpointing is enabled for the job. If checkpointing
        isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
        property settings will be used.
        """
        self._j_function = self._j_function \
            .setCommitOffsetsOnCheckpoints(commit_on_checkpoints)
        return self

    def set_start_from_earliest(self) -> 'FlinkKafkaConsumerBase':
        """
        Specifies the consumer to start reading from the earliest offset for all partitions. This
        lets the consumer ignore any committed group offsets in Zookeeper/ Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function.setStartFromEarliest()
        return self

    def set_start_from_latest(self) -> 'FlinkKafkaConsumerBase':
        """
        Specifies the consuer to start reading from the latest offset for all partitions. This lets
        the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function.setStartFromLatest()
        return self

    def set_start_from_timestamp(self, startup_offsets_timestamp: int) -> 'FlinkKafkaConsumerBase':
        """
        Specifies the consumer to start reading partitions from a specified timestamp. The specified
        timestamp must be before the current timestamp. This lets the consumer ignore any committed
        group offsets in Zookeeper / Kafka brokers.

        The consumer will look up the earliest offset whose timestamp is greater than or equal to
        the specific timestamp from Kafka. If there's no such offset, the consumer will use the
        latest offset to read data from Kafka.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.

        :param startup_offsets_timestamp: timestamp for the startup offsets, as milliseconds for
                                          epoch.
        """
        self._j_function = self._j_function.setStartFromTimestamp(
            startup_offsets_timestamp)
        return self

    def set_start_from_group_offsets(self) -> 'FlinkKafkaConsumerBase':
        """
        Specifies the consumer to start reading from any committed group offsets found in Zookeeper/
        Kafka brokers. The 'group.id' property must be set in the configuration properties. If no
        offset can be found for a partition, the behaviour in 'auto.offset.reset' set in the
        configuration properties will be used for the partition.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function.setStartFromGroupOffsets()
        return self

    def disable_filter_restored_partitions_with_subscribed_topics(self) -> 'FlinkKafkaConsumerBase':
        """
        By default, when restoring from a checkpoint / savepoint, the consumer always ignores
        restored partitions that are no longer associated with the current specified topics or topic
        pattern to subscribe to.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function \
            .disableFilterRestoredPartitionsWithSubscribedTopics()
        return self

    def get_produced_type(self) -> TypeInformation:
        return typeinfo._from_java_type(self._j_function.getProducedType())


def _get_kafka_consumer(topics, properties, deserialization_schema, j_consumer_clz):
    if not isinstance(topics, list):
        topics = [topics]
    gateway = get_gateway()
    j_properties = gateway.jvm.java.util.Properties()
    for key, value in properties.items():
        j_properties.setProperty(key, value)

    j_flink_kafka_consumer = j_consumer_clz(topics,
                                            deserialization_schema._j_deserialization_schema,
                                            j_properties)
    return j_flink_kafka_consumer


class FlinkKafkaConsumer(FlinkKafkaConsumerBase):
    """
    The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
    Apache Kafka. The consumer can run in multiple parallel instances, each of which will
    pull data from one or more Kafka partitions.

    The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
    during a failure, and that the computation processes elements 'exactly once. (These guarantees
    naturally assume that Kafka itself does not lose any data.)

    Please note that Flink snapshots the offsets internally as part of its distributed checkpoints.
    The offsets committed to Kafka / Zookeeper are only to bring the outside view of progress in
    sync with Flink's view of the progress. That way, monitoring and other jobs can get a view of
    how far the Flink Kafka consumer has consumed a topic.

    Please refer to Kafka's documentation for the available configuration properties:
    http://kafka.apache.org/documentation.html#newconsumerconfigs
    """

    def __init__(self, topics: Union[str, List[str]], deserialization_schema: DeserializationSchema,
                 properties: Dict):
        """
        Creates a new Kafka streaming source consumer for Kafka 0.10.x.

        This constructor allows passing multiple topics to the consumer.

        :param topics: The Kafka topics to read from.
        :param deserialization_schema: The de-/serializer used to convert between Kafka's byte
                                       messages and Flink's objects.
        :param properties: The properties that are used to configure both the fetcher and the offset
                           handler.
        """

        warnings.warn("Deprecated in 1.16. Use KafkaSource instead.", DeprecationWarning)
        JFlinkKafkaConsumer = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
        j_flink_kafka_consumer = _get_kafka_consumer(topics, properties, deserialization_schema,
                                                     JFlinkKafkaConsumer)
        super(FlinkKafkaConsumer, self).__init__(j_flink_kafka_consumer=j_flink_kafka_consumer)


# ---- FlinkKafkaProducer ----


class Semantic(Enum):
    """
    Semantics that can be chosen.

    :data: `EXACTLY_ONCE`:

    The Flink producer will write all messages in a Kafka transaction that will be committed to
    the Kafka on a checkpoint. In this mode FlinkKafkaProducer sets up a pool of
    FlinkKafkaProducer. Between each checkpoint there is created new Kafka transaction, which is
    being committed on FlinkKafkaProducer.notifyCheckpointComplete(long). If checkpoint
    complete notifications are running late, FlinkKafkaProducer can run out of
    FlinkKafkaProducers in the pool. In that case any subsequent FlinkKafkaProducer.snapshot-
    State() requests will fail and the FlinkKafkaProducer will keep using the
    FlinkKafkaProducer from previous checkpoint. To decrease chances of failing checkpoints
    there are four options:

        1. decrease number of max concurrent checkpoints
        2. make checkpoints mre reliable (so that they complete faster)
        3. increase delay between checkpoints
        4. increase size of FlinkKafkaProducers pool

    :data: `AT_LEAST_ONCE`:

    The Flink producer will wait for all outstanding messages in the Kafka buffers to be
    acknowledged by the Kafka producer on a checkpoint.

    :data: `NONE`:

    Means that nothing will be guaranteed. Messages can be lost and/or duplicated in case of
    failure.

    """

    EXACTLY_ONCE = 0,
    AT_LEAST_ONCE = 1,
    NONE = 2

    def _to_j_semantic(self):
        JSemantic = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
        return getattr(JSemantic, self.name)


class FlinkKafkaProducerBase(SinkFunction, abc.ABC):
    """
    Flink Sink to produce data into a Kafka topic.

    Please note that this producer provides at-least-once reliability guarantees when checkpoints
    are enabled and set_flush_on_checkpoint(True) is set. Otherwise, the producer doesn;t provid any
    reliability guarantees.
    """

    def __init__(self, j_flink_kafka_producer):
        super(FlinkKafkaProducerBase, self).__init__(sink_func=j_flink_kafka_producer)

    def set_log_failures_only(self, log_failures_only: bool) -> 'FlinkKafkaProducerBase':
        """
        Defines whether the producer should fail on errors, or only log them. If this is set to
        true, then exceptions will be only logged, if set to false, exceptions will be eventually
        thrown and cause the streaming program to fail (and enter recovery).

        :param log_failures_only: The flag to indicate logging-only on exceptions.
        """
        self._j_function.setLogFailuresOnly(log_failures_only)
        return self

    def set_flush_on_checkpoint(self, flush_on_checkpoint: bool) -> 'FlinkKafkaProducerBase':
        """
        If set to true, the Flink producer will wait for all outstanding messages in the Kafka
        buffers to be acknowledged by the Kafka producer on a checkpoint.

        This way, the producer can guarantee that messages in the Kafka buffers are part of the
        checkpoint.

        :param flush_on_checkpoint: Flag indicating the flush mode (true = flush on checkpoint)
        """
        self._j_function.setFlushOnCheckpoint(flush_on_checkpoint)
        return self

    def set_write_timestamp_to_kafka(self,
                                     write_timestamp_to_kafka: bool) -> 'FlinkKafkaProducerBase':
        """
        If set to true, Flink will write the (event time) timestamp attached to each record into
        Kafka. Timestamps must be positive for Kafka to accept them.

        :param write_timestamp_to_kafka: Flag indicating if Flink's internal timestamps are written
                                         to Kafka.
        """
        self._j_function.setWriteTimestampToKafka(write_timestamp_to_kafka)
        return self


class FlinkKafkaProducer(FlinkKafkaProducerBase):
    """
    Flink Sink to produce data into a Kafka topic. By
    default producer will use AT_LEAST_ONCE semantic. Before using EXACTLY_ONCE please refer to
    Flink's Kafka connector documentation.
    """

    def __init__(self, topic: str, serialization_schema: SerializationSchema,
                 producer_config: Dict, kafka_producer_pool_size: int = 5,
                 semantic=Semantic.AT_LEAST_ONCE):
        """
        Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to the topic.

        Using this constructor, the default FlinkFixedPartitioner will be used as the partitioner.
        This default partitioner maps each sink subtask to a single Kafka partition (i.e. all
        records received by a sink subtask will end up in the same Kafka partition).

        :param topic: ID of the Kafka topic.
        :param serialization_schema: User defined key-less serialization schema.
        :param producer_config: Properties with the producer configuration.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in producer_config.items():
            j_properties.setProperty(key, value)

        JFlinkKafkaProducer = gateway.jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

        j_flink_kafka_producer = JFlinkKafkaProducer(
            topic, serialization_schema._j_serialization_schema, j_properties, None,
            semantic._to_j_semantic(), kafka_producer_pool_size)
        super(FlinkKafkaProducer, self).__init__(j_flink_kafka_producer=j_flink_kafka_producer)

    def ignore_failures_after_transaction_timeout(self) -> 'FlinkKafkaProducer':
        """
        Disables the propagation of exceptions thrown when committing presumably timed out Kafka
        transactions during recovery of the job. If a Kafka transaction is timed out, a commit will
        never be successful. Hence, use this feature to avoid recovery loops of the Job. Exceptions
        will still be logged to inform the user that data loss might have occurred.

        Note that we use the System.currentTimeMillis() to track the age of a transaction. Moreover,
        only exceptions thrown during the recovery are caught, i.e., the producer will attempt at
        least one commit of the transaction before giving up.

        :return: This FlinkKafkaProducer.
        """
        self._j_function.ignoreFailuresAfterTransactionTimeout()
        return self


# ---- KafkaSource ----


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

    def set_properties(self, props: Dict) -> 'KafkaSourceBuilder':
        """
        Set arbitrary properties for the KafkaSource and KafkaConsumer. The valid keys can be found
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
        * ``client.id`` is overridden to "client.id.prefix-RANDOM_LONG", or "group.id-RANDOM_LONG"
          if the client id prefix is not set.

        :param props: the properties to set for the KafkaSource.
        :return: this KafkaSourceBuilder.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in props.items():
            j_properties.setProperty(key, value)
        self._j_builder.setProperties(j_properties)
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
        JOffsetResetStrategy = get_gateway().jvm.org.apache.flink.kafka.shaded.org.apache.kafka.\
            clients.consumer.OffsetResetStrategy
        return getattr(JOffsetResetStrategy, self.name)


class KafkaOffsetsInitializer(object):
    """
    An interface for users to specify the starting / stopping offset of a KafkaPartitionSplit.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_initializer: JavaObject):
        self._j_initializer = j_initializer

    @staticmethod
    def committed_offsets(
            offset_reset_strategy: 'KafkaOffsetResetStrategy' = KafkaOffsetResetStrategy.NONE) -> \
            'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the committed
        offsets. An exception will be thrown at runtime if there is no committed offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets if
        the committed offsets does not exist.

        :param offset_reset_strategy: the offset reset strategy to use when the committed offsets do
            not exist.
        :return: an offset initializer which initialize the offsets to the committed offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.\
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.committedOffsets(
            offset_reset_strategy._to_j_offset_reset_strategy()))

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
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.timestamp(timestamp))

    @staticmethod
    def earliest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
        available offsets of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
            available offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.earliest())

    @staticmethod
    def latest() -> 'KafkaOffsetsInitializer':
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest offsets
        of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest
            offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.latest())

    @staticmethod
    def offsets(offsets: Dict['KafkaTopicPartition', int],
                offset_reset_strategy: 'KafkaOffsetResetStrategy' =
                KafkaOffsetResetStrategy.EARLIEST) -> 'KafkaOffsetsInitializer':
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
        j_map_wrapper = jvm.org.apache.flink.python.util.HashMapWrapper(
            None, get_java_class(jvm.Long))
        for tp, offset in offsets.items():
            j_map_wrapper.put(tp._to_j_topic_partition(), offset)

        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source. \
            enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.offsets(
            j_map_wrapper.asMap(), offset_reset_strategy._to_j_offset_reset_strategy()))
