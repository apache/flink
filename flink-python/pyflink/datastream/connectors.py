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
from enum import Enum
from typing import Dict, List, Union

from pyflink.common import typeinfo, Duration, ConfigOption, ExecutionConfig
from pyflink.common.serialization import DeserializationSchema, Encoder, SerializationSchema
from pyflink.common.typeinfo import RowTypeInfo, TypeInformation
from pyflink.datastream.functions import SourceFunction, SinkFunction, JavaFunctionWrapper
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray

from py4j.java_gateway import JavaObject

__all__ = [
    'FileEnumeratorProvider',
    'FileSink',
    'FileSource',
    'FileSourceBuilder',
    'FileSplitAssignerProvider',
    'FlinkKafkaConsumer',
    'FlinkKafkaProducer',
    'JdbcSink',
    'JdbcConnectionOptions',
    'JdbcExecutionOptions',
    'NumberSequenceSource',
    'OutputFileConfig',
    'PulsarDeserializationSchema',
    'PulsarSource',
    'PulsarSourceBuilder',
    'RMQConnectionConfig',
    'RMQSource',
    'RMQSink',
    'RollingPolicy',
    'Sink',
    'Source',
    'StartCursor',
    'StopCursor',
    'StreamFormat',
    'StreamingFileSink',
    'SubscriptionType']


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


class JdbcSink(SinkFunction):

    def __init__(self, j_jdbc_sink):
        super(JdbcSink, self).__init__(sink_func=j_jdbc_sink)

    @staticmethod
    def sink(sql: str, type_info: RowTypeInfo, jdbc_connection_options: 'JdbcConnectionOptions',
             jdbc_execution_options: 'JdbcExecutionOptions' = None):
        """
        Create a JDBC sink.

        :param sql: arbitrary DML query (e.g. insert, update, upsert)
        :param type_info: A RowTypeInfo for query field types.
        :param jdbc_execution_options:  parameters of execution, such as batch size and maximum
                                        retries.
        :param jdbc_connection_options: parameters of connection, such as JDBC URL.
        :return: A JdbcSink.
        """
        sql_types = []
        gateway = get_gateway()
        JJdbcTypeUtil = gateway.jvm.org.apache.flink.connector.jdbc.utils.JdbcTypeUtil
        for field_type in type_info.get_field_types():
            sql_types.append(JJdbcTypeUtil
                             .typeInformationToSqlType(field_type.get_java_type_info()))
        j_sql_type = to_jarray(gateway.jvm.int, sql_types)
        output_format_clz = gateway.jvm.Class\
            .forName('org.apache.flink.connector.jdbc.internal.JdbcOutputFormat', False,
                     get_gateway().jvm.Thread.currentThread().getContextClassLoader())
        j_int_array_type = to_jarray(gateway.jvm.int, []).getClass()
        j_builder_method = output_format_clz.getDeclaredMethod('createRowJdbcStatementBuilder',
                                                               to_jarray(gateway.jvm.Class,
                                                                         [j_int_array_type]))
        j_builder_method.setAccessible(True)
        j_statement_builder = j_builder_method.invoke(None, to_jarray(gateway.jvm.Object,
                                                                      [j_sql_type]))

        jdbc_execution_options = jdbc_execution_options if jdbc_execution_options is not None \
            else JdbcExecutionOptions.defaults()
        j_jdbc_sink = gateway.jvm.org.apache.flink.connector.jdbc.JdbcSink\
            .sink(sql, j_statement_builder, jdbc_execution_options._j_jdbc_execution_options,
                  jdbc_connection_options._j_jdbc_connection_options)
        return JdbcSink(j_jdbc_sink=j_jdbc_sink)


class JdbcConnectionOptions(object):
    """
    JDBC connection options.
    """
    def __init__(self, j_jdbc_connection_options):
        self._j_jdbc_connection_options = j_jdbc_connection_options

    def get_db_url(self) -> str:
        return self._j_jdbc_connection_options.getDbURL()

    def get_driver_name(self) -> str:
        return self._j_jdbc_connection_options.getDriverName()

    def get_user_name(self) -> str:
        return self._j_jdbc_connection_options.getUsername()

    def get_password(self) -> str:
        return self._j_jdbc_connection_options.getPassword()

    class JdbcConnectionOptionsBuilder(object):
        """
        Builder for JdbcConnectionOptions.
        """
        def __init__(self):
            self._j_options_builder = get_gateway().jvm.org.apache.flink.connector\
                .jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()

        def with_url(self, url: str) -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withUrl(url)
            return self

        def with_driver_name(self, driver_name: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withDriverName(driver_name)
            return self

        def with_user_name(self, user_name: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withUsername(user_name)
            return self

        def with_password(self, password: str) \
                -> 'JdbcConnectionOptions.JdbcConnectionOptionsBuilder':
            self._j_options_builder.withPassword(password)
            return self

        def build(self) -> 'JdbcConnectionOptions':
            return JdbcConnectionOptions(j_jdbc_connection_options=self._j_options_builder.build())


class JdbcExecutionOptions(object):
    """
    JDBC sink batch options.
    """
    def __init__(self, j_jdbc_execution_options):
        self._j_jdbc_execution_options = j_jdbc_execution_options

    def get_batch_interval_ms(self) -> int:
        return self._j_jdbc_execution_options.getBatchIntervalMs()

    def get_batch_size(self) -> int:
        return self._j_jdbc_execution_options.getBatchSize()

    def get_max_retries(self) -> int:
        return self._j_jdbc_execution_options.getMaxRetries()

    @staticmethod
    def defaults() -> 'JdbcExecutionOptions':
        return JdbcExecutionOptions(
            j_jdbc_execution_options=get_gateway().jvm
            .org.apache.flink.connector.jdbc.JdbcExecutionOptions.defaults())

    @staticmethod
    def builder() -> 'Builder':
        return JdbcExecutionOptions.Builder()

    class Builder(object):
        """
        Builder for JdbcExecutionOptions.
        """
        def __init__(self):
            self._j_builder = get_gateway().jvm\
                .org.apache.flink.connector.jdbc.JdbcExecutionOptions.builder()

        def with_batch_size(self, size: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withBatchSize(size)
            return self

        def with_batch_interval_ms(self, interval_ms: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withBatchIntervalMs(interval_ms)
            return self

        def with_max_retries(self, max_retries: int) -> 'JdbcExecutionOptions.Builder':
            self._j_builder.withMaxRetries(max_retries)
            return self

        def build(self) -> 'JdbcExecutionOptions':
            return JdbcExecutionOptions(j_jdbc_execution_options=self._j_builder.build())


class RollingPolicy(object):
    """
    The policy based on which a Bucket in the FileSink rolls its currently
    open part file and opens a new one.
    """

    def __init__(self, j_rolling_policy):
        self._j_rolling_policy = j_rolling_policy

    @staticmethod
    def default_rolling_policy(
            part_size: int = 1024 * 1024 * 128,
            rollover_interval: int = 60 * 1000,
            inactivity_interval: int = 60 * 1000) -> 'RollingPolicy':
        """
        Returns the default implementation of the RollingPolicy.

        This policy rolls a part file if:

            - there is no open part file,
            - the current file has reached the maximum bucket size (by default 128MB),
            - the current file is older than the roll over interval (by default 60 sec), or
            - the current file has not been written to for more than the allowed inactivityTime (by
              default 60 sec).

        :param part_size: The maximum part file size before rolling.
        :param rollover_interval: The maximum time duration a part file can stay open before
                                  rolling.
        :param inactivity_interval: The time duration of allowed inactivity after which a part file
                                    will have to roll.
        """
        JDefaultRollingPolicy = get_gateway().jvm.org.apache.flink.streaming.api.functions.\
            sink.filesystem.rollingpolicies.DefaultRollingPolicy
        j_rolling_policy = JDefaultRollingPolicy.builder()\
            .withMaxPartSize(part_size) \
            .withRolloverInterval(rollover_interval) \
            .withInactivityInterval(inactivity_interval) \
            .build()
        return RollingPolicy(j_rolling_policy)

    @staticmethod
    def on_checkpoint_rolling_policy() -> 'RollingPolicy':
        """
        Returns a RollingPolicy which rolls (ONLY) on every checkpoint.
        """
        JOnCheckpointRollingPolicy = get_gateway().jvm.org.apache.flink.streaming.api.functions. \
            sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
        return RollingPolicy(JOnCheckpointRollingPolicy.build())


class BucketAssigner(object):
    """
    A BucketAssigner is used with a file sink to determine the bucket each incoming element should
    be put into.

    The StreamingFileSink can be writing to many buckets at a time, and it is responsible
    for managing a set of active buckets. Whenever a new element arrives it will ask the
    BucketAssigner for the bucket the element should fall in. The BucketAssigner can, for
    example, determine buckets based on system time.
    """

    def __init__(self, j_bucket_assigner):
        self._j_bucket_assigner = j_bucket_assigner

    @staticmethod
    def base_path_bucket_assigner() -> 'BucketAssigner':
        """
        Creates a BucketAssigner that does not perform any bucketing of files. All files are
        written to the base path.
        """
        return BucketAssigner(get_gateway().jvm.org.apache.flink.streaming.api.functions.sink.
                              filesystem.bucketassigners.BasePathBucketAssigner())

    @staticmethod
    def date_time_bucket_assigner(format_str: str = "yyyy-MM-dd--HH", timezone_id: str = None):
        """
        Creates a BucketAssigner that assigns to buckets based on current system time.

        It will create directories of the following form: /{basePath}/{dateTimePath}/}.
        The basePath is the path that was specified as a base path when creating the new bucket.
        The dateTimePath is determined based on the current system time and the user provided format
        string.

        The Java DateTimeFormatter is used to derive a date string from the current system time and
        the date format string. The default format string is "yyyy-MM-dd--HH" so the rolling files
        will have a granularity of hours.

        :param format_str: The format string used to determine the bucket id.
        :param timezone_id: The timezone id, either an abbreviation such as "PST", a full name
                            such as "America/Los_Angeles", or a custom timezone_id such as
                            "GMT-08:00". Th e default time zone will b used if it's None.
        """
        if timezone_id is not None and isinstance(timezone_id, str):
            j_timezone = get_gateway().jvm.java.time.ZoneId.of(timezone_id)
        else:
            j_timezone = get_gateway().jvm.java.time.ZoneId.systemDefault()
        return BucketAssigner(
            get_gateway().jvm.org.apache.flink.streaming.api.functions.sink.
            filesystem.bucketassigners.DateTimeBucketAssigner(format_str, j_timezone))


class StreamingFileSink(SinkFunction):
    """
    Sink that emits its input elements to `FileSystem` files within buckets. This is
    integrated with the checkpointing mechanism to provide exactly once semantics.


    When creating the sink a `basePath` must be specified. The base directory contains
    one directory for every bucket. The bucket directories themselves contain several part files,
    with at least one for each parallel subtask of the sink which is writing data to that bucket.
    These part files contain the actual output data.
    """

    def __init__(self, j_obj):
        super(StreamingFileSink, self).__init__(j_obj)

    class DefaultRowFormatBuilder(object):
        """
        Builder for the vanilla `StreamingFileSink` using a row format.
        """

        def __init__(self, j_default_row_format_builder):
            self.j_default_row_format_builder = j_default_row_format_builder

        def with_bucket_check_interval(
                self, interval: int) -> 'StreamingFileSink.DefaultRowFormatBuilder':
            self.j_default_row_format_builder.withBucketCheckInterval(interval)
            return self

        def with_bucket_assigner(
                self,
                bucket_assigner: BucketAssigner) -> 'StreamingFileSink.DefaultRowFormatBuilder':
            self.j_default_row_format_builder.withBucketAssigner(bucket_assigner._j_bucket_assigner)
            return self

        def with_rolling_policy(
                self,
                policy: RollingPolicy) -> 'StreamingFileSink.DefaultRowFormatBuilder':
            self.j_default_row_format_builder.withRollingPolicy(policy._j_rolling_policy)
            return self

        def with_output_file_config(
            self,
            output_file_config: 'OutputFileConfig') \
                -> 'StreamingFileSink.DefaultRowFormatBuilder':
            self.j_default_row_format_builder.withOutputFileConfig(
                output_file_config._j_output_file_config)
            return self

        def build(self) -> 'StreamingFileSink':
            j_stream_file_sink = self.j_default_row_format_builder.build()
            return StreamingFileSink(j_stream_file_sink)

    @staticmethod
    def for_row_format(base_path: str, encoder: Encoder) -> 'DefaultRowFormatBuilder':
        j_path = get_gateway().jvm.org.apache.flink.core.fs.Path(base_path)
        j_default_row_format_builder = get_gateway().jvm.org.apache.flink.streaming.api.\
            functions.sink.filesystem.StreamingFileSink.forRowFormat(j_path, encoder._j_encoder)
        return StreamingFileSink.DefaultRowFormatBuilder(j_default_row_format_builder)


class OutputFileConfig(object):
    """
    Part file name configuration.
    This allow to define a prefix and a suffix to the part file name.
    """

    @staticmethod
    def builder():
        return OutputFileConfig.OutputFileConfigBuilder()

    def __init__(self, part_prefix: str, part_suffix: str):
        self._j_output_file_config = get_gateway().jvm.org.apache.flink.streaming.api.\
            functions.sink.filesystem.OutputFileConfig(part_prefix, part_suffix)

    def get_part_prefix(self) -> str:
        """
        The prefix for the part name.
        """
        return self._j_output_file_config.getPartPrefix()

    def get_part_suffix(self) -> str:
        """
        The suffix for the part name.
        """
        return self._j_output_file_config.getPartSuffix()

    class OutputFileConfigBuilder(object):
        """
        A builder to create the part file configuration.
        """

        def __init__(self):
            self.part_prefix = "part"
            self.part_suffix = ""

        def with_part_prefix(self, prefix) -> 'OutputFileConfig.OutputFileConfigBuilder':
            self.part_prefix = prefix
            return self

        def with_part_suffix(self, suffix) -> 'OutputFileConfig.OutputFileConfigBuilder':
            self.part_suffix = suffix
            return self

        def build(self) -> 'OutputFileConfig':
            return OutputFileConfig(self.part_prefix, self.part_suffix)


class Source(JavaFunctionWrapper):
    """
    Base class for all unified data source in Flink.
    """

    def __init__(self, source: Union[str, JavaObject]):
        """
        Constructor of Source.

        :param source: The java Source object.
        """
        super(Source, self).__init__(source)


class Sink(JavaFunctionWrapper):
    """
    Base class for all unified data sink in Flink.
    """

    def __init__(self, sink: Union[str, JavaObject]):
        """
        Constructor of Sink.

        :param sink: The java Sink object.
        """
        super(Sink, self).__init__(sink)


class StreamFormat(object):
    """
    A reader format that reads individual records from a stream.

    Compared to the :class:`~pyflink.datastream.connectors.FileSource.BulkFormat`, the stream
    format handles a few things out-of-the-box, like deciding how to batch records or dealing
    with compression.

    Internally in the file source, the readers pass batches of records from the reading threads
    (that perform the typically blocking I/O operations) to the async mailbox threads that do
    the streaming and batch data processing. Passing records in batches
    (rather than one-at-a-time) much reduces the thread-to-thread handover overhead.

    This batching is by default based on I/O fetch size for the StreamFormat, meaning the
    set of records derived from one I/O buffer will be handed over as one. See config option
    `source.file.stream.io-fetch-size` to configure that fetch size.
    """

    def __init__(self, j_stream_format):
        self._j_stream_format = j_stream_format

    @staticmethod
    def text_line_format(charset_name: str = "UTF-8") -> 'StreamFormat':
        """
        Creates a reader format that text lines from a file.

        The reader uses Java's built-in java.io.InputStreamReader to decode the byte stream
        using various supported charset encodings.

        This format does not support optimized recovery from checkpoints. On recovery, it will
        re-read and discard the number of lined that were processed before the last checkpoint.
        That is due to the fact that the offsets of lines in the file cannot be tracked through
        the charset decoders with their internal buffering of stream input and charset decoder
        state.

        :param charset_name: The charset to decode the byte stream.
        """
        j_stream_format = get_gateway().jvm.org.apache.flink.connector.file.src.reader. \
            TextLineInputFormat(charset_name)
        return StreamFormat(j_stream_format)


class FileEnumeratorProvider(object):
    """
    Factory for FileEnumerator which task is to discover all files to be read and to split them
    into a set of file source splits. This includes possibly, path traversals, file filtering
    (by name or other patterns) and deciding whether to split files into multiple splits, and
    how to split them.
    """

    def __init__(self, j_file_enumerator_provider):
        self._j_file_enumerator_provider = j_file_enumerator_provider

    @staticmethod
    def default_splittable_file_enumerator() -> 'FileEnumeratorProvider':
        """
        The default file enumerator used for splittable formats. The enumerator recursively
        enumerates files, split files that consist of multiple distributed storage blocks into
        multiple splits, and filters hidden files (files starting with '.' or '_'). Files with
        suffixes of common compression formats (for example '.gzip', '.bz2', '.xy', '.zip', ...)
        will not be split.
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileEnumeratorProvider(JFileSource.DEFAULT_SPLITTABLE_FILE_ENUMERATOR)

    @staticmethod
    def default_non_splittable_file_enumerator() -> 'FileEnumeratorProvider':
        """
        The default file enumerator used for non-splittable formats. The enumerator recursively
        enumerates files, creates one split for the file, and filters hidden files
        (files starting with '.' or '_').
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileEnumeratorProvider(JFileSource.DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR)


class FileSplitAssignerProvider(object):
    """
    Factory for FileSplitAssigner which is responsible for deciding what split should be
    processed next by which node. It determines split processing order and locality.
    """

    def __init__(self, j_file_split_assigner):
        self._j_file_split_assigner = j_file_split_assigner

    @staticmethod
    def locality_aware_split_assigner() -> 'FileSplitAssignerProvider':
        """
        A FileSplitAssigner that assigns to each host preferably splits that are local, before
        assigning splits that are not local.
        """
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        return FileSplitAssignerProvider(JFileSource.DEFAULT_SPLIT_ASSIGNER)


class FileSourceBuilder(object):
    """
    The builder for the :class:`~pyflink.datastream.connectors.FileSource`, to configure the
    various behaviors.

    Start building the source via one of the following methods:

        - :func:`~pyflink.datastream.connectors.FileSource.for_record_stream_format`
    """

    def __init__(self, j_file_source_builder):
        self._j_file_source_builder = j_file_source_builder

    def monitor_continuously(
            self,
            discovery_interval: Duration) -> 'FileSourceBuilder':
        """
        Sets this source to streaming ("continuous monitoring") mode.

        This makes the source a "continuous streaming" source that keeps running, monitoring
        for new files, and reads these files when they appear and are discovered by the
        monitoring.

        The interval in which the source checks for new files is the discovery_interval. Shorter
        intervals mean that files are discovered more quickly, but also imply more frequent
        listing or directory traversal of the file system / object store.
        """
        self._j_file_source_builder.monitorContinuously(discovery_interval._j_duration)
        return self

    def process_static_file_set(self) -> 'FileSourceBuilder':
        """
        Sets this source to bounded (batch) mode.

        In this mode, the source processes the files that are under the given paths when the
        application is started. Once all files are processed, the source will finish.

        This setting is also the default behavior. This method is mainly here to "switch back"
        to bounded (batch) mode, or to make it explicit in the source construction.
        """
        self._j_file_source_builder.processStaticFileSet()
        return self

    def set_file_enumerator(
            self,
            file_enumerator: 'FileEnumeratorProvider') -> 'FileSourceBuilder':
        """
        Configures the FileEnumerator for the source. The File Enumerator is responsible
        for selecting from the input path the set of files that should be processed (and which
        to filter out). Furthermore, the File Enumerator may split the files further into
        sub-regions, to enable parallelization beyond the number of files.
        """
        self._j_file_source_builder.setFileEnumerator(
            file_enumerator._j_file_enumerator_provider)
        return self

    def set_split_assigner(
            self,
            split_assigner: 'FileSplitAssignerProvider') -> 'FileSourceBuilder':
        """
        Configures the FileSplitAssigner for the source. The File Split Assigner
        determines which parallel reader instance gets which {@link FileSourceSplit}, and in
        which order these splits are assigned.
        """
        self._j_file_source_builder.setSplitAssigner(split_assigner._j_file_split_assigner)
        return self

    def build(self) -> 'FileSource':
        """
        Creates the file source with the settings applied to this builder.
        """
        return FileSource(self._j_file_source_builder.build())


class FileSource(Source):
    """
    A unified data source that reads files - both in batch and in streaming mode.

    This source supports all (distributed) file systems and object stores that can be accessed via
    the Flink's FileSystem class.

    Start building a file source via one of the following calls:

        - :func:`~pyflink.datastream.connectors.FileSource.for_record_stream_format`

    This creates a :class:`~pyflink.datastream.connectors.FileSource.FileSourceBuilder` on which
    you can configure all the properties of the file source.

    <h2>Batch and Streaming</h2>

    This source supports both bounded/batch and continuous/streaming data inputs. For the
    bounded/batch case, the file source processes all files under the given path(s). In the
    continuous/streaming case, the source periodically checks the paths for new files and will start
    reading those.

    When you start creating a file source (via the
    :class:`~pyflink.datastream.connectors.FileSource.FileSourceBuilder` created
    through one of the above-mentioned methods) the source is by default in bounded/batch mode. Call
    :func:`~pyflink.datastream.connectors.FileSource.FileSourceBuilder.monitor_continuously` to put
    the source into continuous streaming mode.

    <h2>Format Types</h2>

    The reading of each file happens through file readers defined by <i>file formats</i>. These
    define the parsing logic for the contents of the file. There are multiple classes that the
    source supports. Their interfaces trade of simplicity of implementation and
    flexibility/efficiency.

        - A :class:`~pyflink.datastream.connectors.FileSource.StreamFormat` reads the contents of
          a file from a file stream. It is the simplest format to implement, and provides many
          features out-of-the-box (like checkpointing logic) but is limited in the optimizations it
          can apply (such as object reuse, batching, etc.).

    <h2>Discovering / Enumerating Files</h2>

    The way that the source lists the files to be processes is defined by the
    :class:`~pyflink.datastream.connectors.FileSource.FileEnumeratorProvider`. The
    FileEnumeratorProvider is responsible to select the relevant files (for example filter out
    hidden files) and to optionally splits files into multiple regions (= file source splits) that
    can be read in parallel).
    """

    def __init__(self, j_file_source):
        super(FileSource, self).__init__(source=j_file_source)

    @staticmethod
    def for_record_stream_format(stream_format: StreamFormat, *paths: str) -> FileSourceBuilder:
        """
        Builds a new FileSource using a
        :class:`~pyflink.datastream.connectors.FileSource.StreamFormat` to read record-by-record
        from a file stream.

        When possible, stream-based formats are generally easier (preferable) to file-based
        formats, because they support better default behavior around I/O batching or progress
        tracking (checkpoints).

        Stream formats also automatically de-compress files based on the file extension. This
        supports files ending in ".deflate" (Deflate), ".xz" (XZ), ".bz2" (BZip2), ".gz", ".gzip"
        (GZip).
        """
        JPath = get_gateway().jvm.org.apache.flink.core.fs.Path
        JFileSource = get_gateway().jvm.org.apache.flink.connector.file.src.FileSource
        j_paths = to_jarray(JPath, [JPath(p) for p in paths])
        return FileSourceBuilder(
            JFileSource.forRecordStreamFormat(stream_format._j_stream_format, j_paths))


class NumberSequenceSource(Source):
    """
    A data source that produces a sequence of numbers (longs). This source is useful for testing and
    for cases that just need a stream of N events of any kind.

    The source splits the sequence into as many parallel sub-sequences as there are parallel
    source readers. Each sub-sequence will be produced in order. Consequently, if the parallelism is
    limited to one, this will produce one sequence in order.

    This source is always bounded. For very long sequences (for example over the entire domain of
    long integer values), user may want to consider executing the application in a streaming manner,
    because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
    """

    def __init__(self, start, end):
        """
        Creates a new NumberSequenceSource that produces parallel sequences covering the
        range start to end (both boundaries are inclusive).
        """
        JNumberSequenceSource = get_gateway().jvm.org.apache.flink.api.connector.source.lib.\
            NumberSequenceSource
        j_seq_source = JNumberSequenceSource(start, end)
        super(NumberSequenceSource, self).__init__(source=j_seq_source)


class FileSink(Sink):
    """
    A unified sink that emits its input elements to FileSystem files within buckets. This
    sink achieves exactly-once semantics for both BATCH and STREAMING.

    When creating the sink a basePath must be specified. The base directory contains one
    directory for every bucket. The bucket directories themselves contain several part files, with
    at least one for each parallel subtask of the sink which is writing data to that bucket.
    These part files contain the actual output data.

    The sink uses a BucketAssigner to determine in which bucket directory each element
    should be written to inside the base directory. The BucketAssigner can, for example, roll
    on every checkpoint or use time or a property of the element to determine the bucket directory.
    The default BucketAssigner is a DateTimeBucketAssigner which will create one new
    bucket every hour. You can specify a custom BucketAssigner using the
    :func:`~pyflink.datastream.connectors.FileSink.RowFormatBuilder.with_bucket_assigner`,
    after calling :class:`~pyflink.datastream.connectors.FileSink.for_row_format`.

    The names of the part files could be defined using OutputFileConfig. This
    configuration contains a part prefix and a part suffix that will be used with a random uid
    assigned to each subtask of the sink and a rolling counter to determine the file names. For
    example with a prefix "prefix" and a suffix ".ext", a file named {@code
    "prefix-81fc4980-a6af-41c8-9937-9939408a734b-17.ext"} contains the data from subtask with uid
    {@code 81fc4980-a6af-41c8-9937-9939408a734b} of the sink and is the {@code 17th} part-file
    created by that subtask.

    Part files roll based on the user-specified RollingPolicy. By default, a DefaultRollingPolicy
    is used for row-encoded sink output; a OnCheckpointRollingPolicy is
    used for bulk-encoded sink output.

    In some scenarios, the open buckets are required to change based on time. In these cases, the
    user can specify a bucket_check_interval (by default 1m) and the sink will check
    periodically and roll the part file if the specified rolling policy says so.

    Part files can be in one of three states: in-progress, pending or finished. The reason for this
    is how the sink works to provide exactly-once semantics and fault-tolerance. The part file that
    is currently being written to is in-progress. Once a part file is closed for writing it becomes
    pending. When a checkpoint is successful (for STREAMING) or at the end of the job (for BATCH)
    the currently pending files will be moved to finished.

    For STREAMING in order to guarantee exactly-once semantics in case of a failure, the
    sink should roll back to the state it had when that last successful checkpoint occurred. To this
    end, when restoring, the restored files in pending state are transferred into the finished state
    while any in-progress files are rolled back, so that they do not contain data that arrived after
    the checkpoint from which we restore.
    """

    def __init__(self, j_file_sink):
        super(FileSink, self).__init__(sink=j_file_sink)

    class RowFormatBuilder(object):
        """
        Builder for the vanilla FileSink using a row format.
        """

        def __init__(self, j_row_format_builder):
            self._j_row_format_builder = j_row_format_builder

        def with_bucket_check_interval(self, interval: int):
            """
            :param interval: The check interval in milliseconds.
            """
            self._j_row_format_builder.withBucketCheckInterval(interval)
            return self

        def with_bucket_assigner(self, bucket_assigner: BucketAssigner):
            self._j_row_format_builder.withBucketAssigner(bucket_assigner._j_bucket_assigner)
            return self

        def with_rolling_policy(self, rolling_policy: RollingPolicy):
            self._j_row_format_builder.withRollingPolicy(rolling_policy._j_rolling_policy)
            return self

        def with_output_file_config(self, output_file_config: OutputFileConfig):
            self._j_row_format_builder.withOutputFileConfig(
                output_file_config._j_output_file_config)
            return self

        def build(self):
            return FileSink(self._j_row_format_builder.build())

    @staticmethod
    def for_row_format(base_path: str, encoder: Encoder) -> 'FileSink.RowFormatBuilder':
        JPath = get_gateway().jvm.org.apache.flink.core.fs.Path
        JFileSink = get_gateway().jvm.org.apache.flink.connector.file.sink.FileSink

        return FileSink.RowFormatBuilder(
            JFileSink.forRowFormat(JPath(base_path), encoder._j_encoder))


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
        using a topic regex. You can set topics once either with set_topics or set_topic_pattern in
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
