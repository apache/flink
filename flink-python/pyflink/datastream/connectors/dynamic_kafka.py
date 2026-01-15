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
from typing import Dict, Set, Union

from py4j.java_gateway import JavaObject

from pyflink.common import DeserializationSchema
from pyflink.datastream.connectors import Source
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer
from pyflink.java_gateway import get_gateway

__all__ = [
    'DynamicKafkaSource',
    'DynamicKafkaSourceBuilder',
    'KafkaMetadataService',
    'KafkaRecordDeserializationSchema',
    'KafkaStreamSetSubscriber',
    'KafkaStreamSubscriber',
    'StreamPatternSubscriber',
    'SingleClusterTopicMetadataService'
]


class KafkaMetadataService(object):
    """
    Base class for Kafka metadata service wrappers.
    """

    def __init__(self, j_metadata_service: JavaObject):
        self._j_metadata_service = j_metadata_service


class SingleClusterTopicMetadataService(KafkaMetadataService):
    """
    A KafkaMetadataService backed by a single Kafka cluster where stream ids map to topics.
    """

    def __init__(self, kafka_cluster_id: str, properties: Dict[str, str]):
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in properties.items():
            j_properties.setProperty(key, value)
        j_service = gateway.jvm.org.apache.flink.connector.kafka.dynamic.metadata \
            .SingleClusterTopicMetadataService(kafka_cluster_id, j_properties)
        super().__init__(j_service)


class KafkaStreamSubscriber(object):
    """
    Wrapper for Java KafkaStreamSubscriber implementations.
    """

    def __init__(self, j_kafka_stream_subscriber: JavaObject):
        self._j_kafka_stream_subscriber = j_kafka_stream_subscriber


class KafkaStreamSetSubscriber(KafkaStreamSubscriber):
    """
    Subscriber that consumes from a fixed set of stream ids.
    """

    def __init__(self, stream_ids: Set[str]):
        gateway = get_gateway()
        j_stream_ids = gateway.jvm.java.util.HashSet()
        for stream_id in stream_ids:
            j_stream_ids.add(stream_id)
        j_subscriber = gateway.jvm.org.apache.flink.connector.kafka.dynamic.source.enumerator \
            .subscriber.KafkaStreamSetSubscriber(j_stream_ids)
        super().__init__(j_subscriber)


class StreamPatternSubscriber(KafkaStreamSubscriber):
    """
    Subscriber that consumes from stream ids matching a regex pattern.
    """

    def __init__(self, stream_pattern: str):
        gateway = get_gateway()
        j_pattern = gateway.jvm.java.util.regex.Pattern.compile(stream_pattern)
        j_subscriber = gateway.jvm.org.apache.flink.connector.kafka.dynamic.source.enumerator \
            .subscriber.StreamPatternSubscriber(j_pattern)
        super().__init__(j_subscriber)


class KafkaRecordDeserializationSchema(DeserializationSchema):
    """
    Wrapper for KafkaRecordDeserializationSchema.
    """

    def __init__(self, j_deserialization_schema: JavaObject):
        super().__init__(j_deserialization_schema)

    @staticmethod
    def value_only(deserialization_schema: DeserializationSchema) -> \
            'KafkaRecordDeserializationSchema':
        jvm = get_gateway().jvm
        j_deserializer = jvm.org.apache.flink.connector.kafka.source.reader.deserializer \
            .KafkaRecordDeserializationSchema.valueOnly(
                deserialization_schema._j_deserialization_schema)
        return KafkaRecordDeserializationSchema(j_deserializer)


class DynamicKafkaSource(Source):
    """
    Source implementation for dynamic Kafka streams.

    Example:
    ::

        >>> metadata_service = SingleClusterTopicMetadataService(
        ...     'cluster-a', {'bootstrap.servers': 'localhost:9092'})
        >>> source = DynamicKafkaSource.builder() \\
        ...     .set_stream_ids({'stream-a'}) \\
        ...     .set_kafka_metadata_service(metadata_service) \\
        ...     .set_value_only_deserializer(SimpleStringSchema()) \\
        ...     .build()
    """

    def __init__(self, j_dynamic_kafka_source: JavaObject):
        super().__init__(j_dynamic_kafka_source)

    @staticmethod
    def builder() -> 'DynamicKafkaSourceBuilder':
        return DynamicKafkaSourceBuilder()


class DynamicKafkaSourceBuilder(object):
    """
    Builder for DynamicKafkaSource.
    """

    def __init__(self):
        self._j_builder = get_gateway().jvm.org.apache.flink.connector.kafka.dynamic.source \
            .DynamicKafkaSource.builder()

    def build(self) -> DynamicKafkaSource:
        return DynamicKafkaSource(self._j_builder.build())

    def set_stream_ids(self, stream_ids: Set[str]) -> 'DynamicKafkaSourceBuilder':
        """
        Set the stream ids to consume.
        """
        j_set = get_gateway().jvm.java.util.HashSet()
        for stream_id in stream_ids:
            j_set.add(stream_id)
        self._j_builder.setStreamIds(j_set)
        return self

    def set_stream_pattern(self, stream_pattern: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set a regex pattern to match stream ids.
        """
        self._j_builder.setStreamPattern(get_gateway().jvm.java.util.regex
                                         .Pattern.compile(stream_pattern))
        return self

    def set_kafka_stream_subscriber(
            self,
            kafka_stream_subscriber: Union[KafkaStreamSubscriber, JavaObject]) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set a custom KafkaStreamSubscriber.
        """
        if isinstance(kafka_stream_subscriber, KafkaStreamSubscriber):
            j_subscriber = kafka_stream_subscriber._j_kafka_stream_subscriber
        else:
            j_subscriber = kafka_stream_subscriber
        self._j_builder.setKafkaStreamSubscriber(j_subscriber)
        return self

    def set_kafka_metadata_service(
            self,
            kafka_metadata_service: Union[KafkaMetadataService, JavaObject]) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the KafkaMetadataService.
        """
        if isinstance(kafka_metadata_service, KafkaMetadataService):
            j_metadata_service = kafka_metadata_service._j_metadata_service
        else:
            j_metadata_service = kafka_metadata_service
        self._j_builder.setKafkaMetadataService(j_metadata_service)
        return self

    def set_deserializer(self,
                         deserializer: Union[KafkaRecordDeserializationSchema, JavaObject]) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the KafkaRecordDeserializationSchema.
        """
        if isinstance(deserializer, KafkaRecordDeserializationSchema):
            j_deserializer = deserializer._j_deserialization_schema
        else:
            j_deserializer = deserializer
        self._j_builder.setDeserializer(j_deserializer)
        return self

    def set_value_only_deserializer(self, deserialization_schema: DeserializationSchema) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set a value-only DeserializationSchema.
        """
        return self.set_deserializer(
            KafkaRecordDeserializationSchema.value_only(deserialization_schema))

    def set_starting_offsets(self, starting_offsets_initializer: KafkaOffsetsInitializer) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the starting offsets for all streams.
        """
        self._j_builder.setStartingOffsets(starting_offsets_initializer._j_initializer)
        return self

    def set_bounded(self, stopping_offsets_initializer: KafkaOffsetsInitializer) \
            -> 'DynamicKafkaSourceBuilder':
        """
        Set the source to bounded mode with stopping offsets.
        """
        self._j_builder.setBounded(stopping_offsets_initializer._j_initializer)
        return self

    def set_properties(self, props: Dict[str, str]) -> 'DynamicKafkaSourceBuilder':
        """
        Set consumer properties for all clusters.
        """
        gateway = get_gateway()
        j_properties = gateway.jvm.java.util.Properties()
        for key, value in props.items():
            j_properties.setProperty(key, value)
        self._j_builder.setProperties(j_properties)
        return self

    def set_property(self, key: str, value: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set a consumer property for all clusters.
        """
        self._j_builder.setProperty(key, value)
        return self

    def set_group_id(self, group_id: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the consumer group id for all clusters.
        """
        self._j_builder.setGroupId(group_id)
        return self

    def set_client_id_prefix(self, prefix: str) -> 'DynamicKafkaSourceBuilder':
        """
        Set the client id prefix for all clusters.
        """
        self._j_builder.setClientIdPrefix(prefix)
        return self
