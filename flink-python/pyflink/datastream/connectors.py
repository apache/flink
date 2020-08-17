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
from typing import Dict, List, Union

from pyflink.common import typeinfo
from pyflink.common.serialization_schemas import DeserializationSchema, SerializationSchema
from pyflink.datastream.functions import SourceFunction, SinkFunction
from pyflink.java_gateway import get_gateway


class FlinkKafkaConsumerBase(SourceFunction, abc.ABC):
    """
    Base class of all Flink Kafka Consumer data sources. This implements the common behavior across
    all kafka versions.

    The Kafka version specific behavior is defined mainly in the specific subclasses.
    """

    def __init__(self, j_flink_kafka_consumer):
        super(FlinkKafkaConsumerBase, self).__init__(source_func=j_flink_kafka_consumer)

    def set_commit_offsets_on_checkpoints(self, commit_on_checkpoints: bool):
        """
        Specifies whether or not the consumer should commit offsets back to kafka on checkpoints.
        This setting will only have effect if checkpointing is enabled for the job. If checkpointing
        isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
        property settings will be used.
        """
        self._j_function = self._j_function \
            .setCommitOffsetsOnCheckpoints(commit_on_checkpoints)
        return self

    def set_start_from_earliest(self):
        """
        Specifies the consumer to start reading from the earliest offset for all partitions. This
        lets the consumer ignore any committed group offsets in Zookeeper/ Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function.setStartFromEarliest()
        return self

    def set_start_from_latest(self):
        """
        Specifies the consuer to start reading from the latest offset for all partitions. This lets
        the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.
        """
        self._j_function = self._j_function.setStartFromLatest()
        return self

    def set_start_from_timestamp(self, startup_offsets_timestamp: int):
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

    def set_start_from_group_offsets(self):
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

    def disable_filter_restored_partitions_with_subscribed_topics(self):
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

    def get_produced_type(self):
        return typeinfo._from_java_type(self._j_function.getProducedType())


class FlinkKafkaConsumer010(FlinkKafkaConsumerBase):
    """
    The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
    Apache Kafka 0.10.x. The consumer can run in multiple parallel instances, each of which will
    pull data from one or more Kafka partitions.

    The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
    during a failure, and taht the computation processes elements 'exactly once. (These guarantees
    naturally assume that Kafka itself does not loose any data.)

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

        JFlinkKafkaConsumer010 = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
        j_flink_kafka_consumer_010 = _get_kafka_consumer(topics, properties, deserialization_schema,
                                                         JFlinkKafkaConsumer010)
        super(FlinkKafkaConsumer010, self).__init__(
            j_flink_kafka_consumer=j_flink_kafka_consumer_010)


class FlinkKafkaConsumer011(FlinkKafkaConsumerBase):
    """
    The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
    Apache Kafka 0.10.x. The consumer can run in multiple parallel instances, each of which will
    pull data from one or more Kafka partitions.

    The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
    during a failure, and taht the computation processes elements 'exactly once. (These guarantees
    naturally assume that Kafka itself does not loose any data.)

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
        JFlinkKafkaConsumer011 = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
        j_flink_kafka_consumer_011 = _get_kafka_consumer(topics, properties, deserialization_schema,
                                                         JFlinkKafkaConsumer011)
        super(FlinkKafkaConsumer011, self).__init__(j_flink_kafka_consumer_011)


class FlinkKafkaConsumer(FlinkKafkaConsumerBase):
    """
    The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
    Apache Kafka 0.10.x. The consumer can run in multiple parallel instances, each of which will
    pull data from one or more Kafka partitions.

    The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
    during a failure, and taht the computation processes elements 'exactly once. (These guarantees
    naturally assume that Kafka itself does not loose any data.)

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

        JFlinkKafkaConsumer = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
        j_flink_kafka_consumer = _get_kafka_consumer(topics, properties, deserialization_schema,
                                                     JFlinkKafkaConsumer)
        super(FlinkKafkaConsumer, self).__init__(j_flink_kafka_consumer=j_flink_kafka_consumer)


class FlinkKafkaProducerBase(SinkFunction, abc.ABC):
    """
    Flink Sink to produce data into a Kafka topic.

    Please note that this producer provides at-least-once reliability guarantees when checkpoints
    are enabled and set_flush_on_checkpoint(True) is set. Otherwise, the producer doesn;t provid any
    reliability guarantees.
    """

    def __init__(self, j_flink_kafka_producer):
        super(FlinkKafkaProducerBase, self).__init__(sink_func=j_flink_kafka_producer)

    def set_log_failures_only(self, log_failures_only: bool):
        """
        Defines whether the producer should fail on errors, or only log them. If this is set to
        true, then exceptions will be only logged, if set to false, exceptions will be eventually
        thrown and cause the streaming program to fail (and enter recovery).

        :param log_failures_only: The flag to indicate logging-only on exceptions.
        """
        self._j_function.setLogFailuresOnly(log_failures_only)

    def set_flush_on_checkpoint(self, flush_on_checkpoint: bool):
        """
        If set to true, the Flink producer will wait for all outstanding messages in the Kafka
        buffers to be acknowledged by the Kafka producer on a checkpoint.

        This way, the producer can guarantee that messages in the Kafka buffers are part of the
        checkpoint.

        :param flush_on_checkpoint: Flag indicating the flush mode (true = flush on checkpoint)
        """
        self._j_function.setFlushOnCheckpoint(flush_on_checkpoint)

    def set_write_timestamp_to_kafka(self, write_timestamp_to_kafka: bool):
        """
        If set to true, Flink will write the (event time) timestamp attached to each record into
        Kafka. Timestamps must be positive for Kafka to accept them.

        :param write_timestamp_to_kafka: Flag indicating if Flink's internal timestamps are written
                                         to Kafka.
        """
        self._j_function.setWriteTimestampToKafka(write_timestamp_to_kafka)


class FlinkKafkaProducer010(FlinkKafkaProducerBase):
    """
    Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.10.x.
    """

    def __init__(self, topic: str, serialization_schema: SerializationSchema,
                 producer_config: Dict):
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

        JFlinkKafkaProducer010 = gateway.jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
        j_flink_kafka_producer = JFlinkKafkaProducer010(
            topic, serialization_schema._j_serialization_schema, j_properties)
        super(FlinkKafkaProducer010, self).__init__(j_flink_kafka_producer=j_flink_kafka_producer)


class Semantic(object):
    """
    Semantics that can be chosen.
    :data: `EXACTLY_ONCE`:
    The Flink producer will write all messages in a Kafka transaction that will be committed to
    the Kafka on a checkpoint. In this mode FlinkKafkaProducer011 sets up a pool of
    FlinkKafkaProducer. Between each checkpoint there is created new Kafka transaction, which is
    being committed on FlinkKafkaProducer011.notifyCheckpointComplete(long). If checkpoint
    complete notifications are running late, FlinkKafkaProducer011 can run out of
    FlinkKafkaProducers in the pool. In that case any subsequent FlinkKafkaProducer011.snapshot-
    State() requests will fail and the FlinkKafkaProducer011 will keep using the
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

    @staticmethod
    def _to_j_semantic(semantic, j_semantic):
        if semantic == Semantic.EXACTLY_ONCE:
            return j_semantic.EXACTLY_ONCE
        elif semantic == Semantic.AT_LEAST_ONCE:
            return j_semantic.AT_LEAST_ONCE
        elif semantic == Semantic.NONE:
            return j_semantic.NONE
        else:
            raise TypeError("Unsupported semantic: %s, supported semantics are: "
                            "Semantic.EXACTLY_ONCE, Semantic.AT_LEAST_ONCE, Semantic.NONE"
                            % semantic)


class FlinkKafkaProducer011(FlinkKafkaProducerBase):
    """
    Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.11.x. By
    default producer will use AT_LEAST_ONCE sematic. Before using EXACTLY_ONCE please refer to
    Flink's Kafka connector documentation.
    """

    def __init__(self, topic: str, serialization_schema: SerializationSchema,
                 producer_config: Dict, kafka_producer_pool_size: int = 5,
                 semantic: Semantic = Semantic.AT_LEAST_ONCE):
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

        JFlinkKafkaProducer011 = gateway.jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

        JFlinkFixedPartitioner = gateway.jvm\
            .org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
        JSemantic = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
        j_keyed_serialization_schema = gateway.jvm\
            .org.apache.flink.streaming.connectors.kafka.internals\
            .KeyedSerializationSchemaWrapper(serialization_schema._j_serialization_schema)
        j_flink_kafka_producer = JFlinkKafkaProducer011(
            topic, j_keyed_serialization_schema, j_properties,
            gateway.jvm.java.util.Optional.of(JFlinkFixedPartitioner()),
            Semantic._to_j_semantic(semantic, JSemantic), kafka_producer_pool_size)
        super(FlinkKafkaProducer011, self).__init__(j_flink_kafka_producer=j_flink_kafka_producer)

    def ignore_failures_after_transaction_timeout(self) -> 'FlinkKafkaProducer011':
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


class FlinkKafkaProducer(FlinkKafkaProducerBase):
    """
    Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.11.x. By
    default producer will use AT_LEAST_ONCE sematic. Before using EXACTLY_ONCE please refer to
    Flink's Kafka connector documentation.
    """

    def __init__(self, topic: str, serialization_schema: SerializationSchema,
                 producer_config: Dict, kafka_producer_pool_size: int = 5,
                 semantic: Semantic = Semantic.AT_LEAST_ONCE):
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
        JSemantic = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic

        j_flink_kafka_producer = JFlinkKafkaProducer(
            topic, serialization_schema._j_serialization_schema, j_properties, None,
            Semantic._to_j_semantic(semantic, JSemantic), kafka_producer_pool_size)
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
