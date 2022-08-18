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
from typing import Union, Tuple

from py4j.java_gateway import JavaObject

from pyflink.common import Duration, DeserializationSchema, SerializationSchema
from pyflink.datastream.connectors import Source, Sink
from pyflink.java_gateway import get_gateway

__all__ = [
    'PubSubSource',
    'PubSubSink',
    'Credentials',
    'PubSubSubscriberFactory'
]


class Credentials(object):
    """
    The authority authentication policy.
    """

    def __init__(self, j_credentials):
        self._j_credentials = j_credentials

    @staticmethod
    def emulator_credentials() -> 'Credentials':
        """
        A placeholder for credentials to signify that requests sent to the server should not be
        authenticated.
        This is typically useful when using local service emulators.
        """
        JEmulatorCredentialsProvider = get_gateway().jvm. \
            org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentialsProvider
        return Credentials(JEmulatorCredentialsProvider.create().getCredentials())


class PubSubSubscriberFactory(object):
    """
    A factory class to create a SubscriberStub.
    This allows for customized Subscribers with for instance tweaked configurations.
    """

    def __init__(self, j_pubsub_subscriber_factory: Union[Tuple, JavaObject]):
        self._j_pubsub_subscriber_factory = j_pubsub_subscriber_factory

    @staticmethod
    def default(max_messages_per_pull: int, timeout: Duration, retries: int) \
            -> 'PubSubSubscriberFactory':
        """
        A default PubSubSubscriberFactory configuration.
        """
        return PubSubSubscriberFactory((max_messages_per_pull, timeout, retries))

    @staticmethod
    def emulator(host_and_port: str, project: str, subscription: str, retries: int,
                 timeout: Duration, max_messages_per_pull: int) -> 'PubSubSubscriberFactory':
        """
        A convenience PubSubSubscriberFactory that can be used to connect to a PubSub emulator.
        The PubSub emulators do not support SSL or Credentials and as such this SubscriberStub does
        not require or provide this.
        """
        JPubSubSubscriberFactoryForEmulator = get_gateway().jvm.org.apache.flink. \
            streaming.connectors.gcp.pubsub.emulator.PubSubSubscriberFactoryForEmulator
        return PubSubSubscriberFactory(
            JPubSubSubscriberFactoryForEmulator(host_and_port, project, subscription, retries,
                                                timeout._j_duration, max_messages_per_pull))


class PubSubSource(Source):
    """
    PubSub Source, this Source will consume PubSub messages from a subscription and Acknowledge
    them on the next checkpoint. This ensures every message will get acknowledged at least once.
    """

    def __init__(self, j_pubsub_source: JavaObject):
        super(PubSubSource, self).__init__(j_pubsub_source)

    @staticmethod
    def new_builder() -> 'DeserializationSchemaBuilder':
        """
        Create a builder for a new PubSubSource.
        """
        JPubSubSource = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource
        return PubSubSource.DeserializationSchemaBuilder(JPubSubSource.newBuilder())

    class DeserializationSchemaBuilder(object):
        """
        Part of PubSubSource.PubSubSourceBuilder to set required fields.
        """

        def __init__(self, j_deserialization_schema_builder):
            self._j_deserialization_schema_builder = j_deserialization_schema_builder

        def with_deserialization_schema(self, deserialization_schema: DeserializationSchema) \
                -> 'PubSubSource.ProjectNameBuilder':
            """
            Set the DeserializationSchema used to deserialize incoming PubSubMessages.
            """
            _j_deserialization_schema = self._j_deserialization_schema_builder \
                .withDeserializationSchema(deserialization_schema._j_deserialization_schema)
            return PubSubSource.ProjectNameBuilder(_j_deserialization_schema)

    class ProjectNameBuilder(object):
        """
        Part of PubSubSource.PubSubSourceBuilder to set required fields.
        """

        def __init__(self, j_project_name_builder):
            self._j_project_name_builder = j_project_name_builder

        def with_project_name(self, project_name: str) -> 'PubSubSource.SubscriptionNameBuilder':
            """
            Set the project name of the subscription to pull messages from.
            """
            _j_project_name = self._j_project_name_builder.withProjectName(project_name)
            return PubSubSource.SubscriptionNameBuilder(_j_project_name)

    class SubscriptionNameBuilder(object):
        """
        Part of PubSubSource.PubSubSourceBuilder to set required fields.
        """

        def __init__(self, j_subscription_name_builder):
            self._j_subscription_name_builder = j_subscription_name_builder

        def with_subscription_name(self, subscription_name: str) \
                -> 'PubSubSource.PubSubSourceBuilder':
            """
            Set the subscription name of the subscription to pull messages from.
            """
            _j_subscription_name = self._j_subscription_name_builder.withSubscriptionName(
                subscription_name)
            return PubSubSource.PubSubSourceBuilder(_j_subscription_name)

    class PubSubSourceBuilder(object):
        """
        Builder to create PubSubSource.
        """

        def __init__(self, j_pubsub_source_builder):
            self._j_pubsub_source_builder = j_pubsub_source_builder

        def with_credentials(self, credentials: Credentials) \
                -> 'PubSubSource.PubSubSourceBuilder':
            """
            Set the credentials. If this is not used then the credentials are picked up from the
            environment variables.
            """
            self._j_pubsub_source_builder.withCredentials(credentials._j_credentials)
            return self

        def with_pubsub_subscriber_factory(self, factory: 'PubSubSubscriberFactory') \
                -> 'PubSubSource.PubSubSourceBuilder':
            j_factory = factory._j_pubsub_subscriber_factory
            if isinstance(j_factory, tuple):
                self._j_pubsub_source_builder.withPubSubSubscriberFactory(
                    j_factory[0],
                    j_factory[1]._j_duration,
                    j_factory[2])
            else:
                self._j_pubsub_source_builder.withPubSubSubscriberFactory(j_factory)
            return self

        def with_message_rate_limit(self, message_per_second_rate_limit: int) \
                -> 'PubSubSource.PubSubSourceBuilder':
            """
            Set a limit on the rate of messages per second received. This limit is per parallel
            instance of the source function. Default is set to 100000 messages per second
            """
            self._j_pubsub_source_builder.withMessageRateLimit(message_per_second_rate_limit)
            return self

        def build(self) -> 'PubSubSource':
            """
            Actually build the desired instance of the PubSubSourceBuilder.
            """
            return PubSubSource(self._j_pubsub_source_builder.build())


class PubSubSink(Sink):
    """
    A sink function that outputs to PubSub.
    """

    def __init__(self, j_pub_sub_sink: JavaObject):
        super(PubSubSink, self).__init__(sink=j_pub_sub_sink)

    @staticmethod
    def new_builder() -> 'SerializationSchemaBuilder':
        """
        Create a builder for a new PubSubSink.
        """
        JPubSubSink = get_gateway().jvm. \
            org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink
        return PubSubSink.SerializationSchemaBuilder(JPubSubSink.newBuilder())

    class SerializationSchemaBuilder(object):
        """
        Part of PubSubSink.PubSubSinkBuilder to set required fields.
        """

        def __init__(self, j_serialization_schema_builder):
            self._j_serialization_schema_builder = j_serialization_schema_builder

        def with_serialization_schema(self, deserialization_schema: SerializationSchema) \
                -> 'PubSubSink.ProjectNameBuilder':
            """
            Set the SerializationSchema used to Serialize objects to be added as payloads of
            PubSubMessages.
            """
            _j_serialization_schema = self._j_serialization_schema_builder.withSerializationSchema(
                deserialization_schema._j_serialization_schema)
            return PubSubSink.ProjectNameBuilder(_j_serialization_schema)

    class ProjectNameBuilder(object):
        """
        Part of PubSubSource.PubSubSourceBuilder to set required fields.
        """

        def __init__(self, j_project_name_builder):
            self._j_project_name_builder = j_project_name_builder

        def with_project_name(self, project_name: str) -> 'PubSubSink.TopicNameBuilder':
            """
            Set the project name of the subscription to pull messages from.
            """
            _j_project_name = self._j_project_name_builder.withProjectName(project_name)
            return PubSubSink.TopicNameBuilder(_j_project_name)

    class TopicNameBuilder(object):
        """
        Part of PubSubSink.PubSubSinkBuilder to set required fields.
        """

        def __init__(self, j_topic_name_builder):
            self._j_topic_name_builder = j_topic_name_builder

        def with_topic_name(self, topic_name: str) -> 'PubSubSink.PubSubSinkBuilder':
            """
            Set the subscription name of the subscription to pull messages from.
            """
            _j_topic_name_builder = self._j_topic_name_builder.withTopicName(topic_name)
            return PubSubSink.PubSubSinkBuilder(_j_topic_name_builder)

    class PubSubSinkBuilder(object):
        """
        PubSubSinkBuilder to create a PubSubSink.
        """

        def __init__(self, j_pubsub_sink_builder):
            self._j_pubsub_sink_builder = j_pubsub_sink_builder

        def with_credentials(self, credentials: Credentials) -> 'PubSubSink.PubSubSinkBuilder':
            """
            Set the credentials. If this is not used then the credentials are picked up from the
            environment variables.
            """
            self._j_pubsub_sink_builder.withCredentials(credentials._j_credentials)
            return self

        def with_host_and_port_for_emulator(self, host_and_port: str) \
                -> 'PubSubSink.PubSubSinkBuilder':
            """
            Set the custom hostname/port combination of PubSub. The ONLY reason to use this is
            during tests with the emulator provided by Google.
            """
            self._j_pubsub_sink_builder.withHostAndPortForEmulator(host_and_port)
            return self

        def build(self) -> 'PubSubSink':
            """
            Actually builder the desired instance of the PubSubSink.
            """
            return PubSubSink(self._j_pubsub_sink_builder.build())
