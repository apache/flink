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
from typing import List, Union

from pyflink.datastream.connectors import Sink, DeliveryGuarantee
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


__all__ = ['FlushBackoffType',
           'ElasticsearchEmitter',
           'Elasticsearch6SinkBuilder',
           'Elasticsearch7SinkBuilder',
           'ElasticsearchSink']


class FlushBackoffType(Enum):
    """
    Used to control whether the sink should retry failed requests at all or with which kind back off
    strategy.

    :data: `CONSTANT`:

    After every failure, it waits a configured time until the retries are exhausted.

    :data: `EXPONENTIAL`:

    After every failure, it waits initially the configured time and increases the waiting time
    exponentially until the retries are exhausted.

    :data: `NONE`:

    The failure is not retried.
    """

    CONSTANT = 0,
    EXPONENTIAL = 1,
    NONE = 2,

    def _to_j_flush_backoff_type(self):
        JFlushBackoffType = get_gateway().jvm \
            .org.apache.flink.connector.elasticsearch.sink.FlushBackoffType
        return getattr(JFlushBackoffType, self.name)


class ElasticsearchEmitter(object):
    """
    Emitter which is used by sinks to prepare elements for sending them to Elasticsearch.
    """

    def __init__(self, j_emitter):
        self._j_emitter = j_emitter

    @staticmethod
    def static_index(index: str, key_field: str = None, doc_type: str = None) \
            -> 'ElasticsearchEmitter':
        """
        Creates an emitter with static index which is invoked on every record to convert it to
        Elasticsearch actions.
        """
        JMapElasticsearchEmitter = get_gateway().jvm \
            .org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter
        j_emitter = JMapElasticsearchEmitter(index, doc_type, key_field, False)
        return ElasticsearchEmitter(j_emitter)

    @staticmethod
    def dynamic_index(index_field: str, key_field: str = None, doc_type: str = None) \
            -> 'ElasticsearchEmitter':
        """
        Creates an emitter with dynamic index which is invoked on every record to convert it to
        Elasticsearch actions.
        """
        JMapElasticsearchEmitter = get_gateway().jvm \
            .org.apache.flink.connector.elasticsearch.sink.MapElasticsearchEmitter
        j_emitter = JMapElasticsearchEmitter(index_field, doc_type, key_field, True)
        return ElasticsearchEmitter(j_emitter)


class ElasticsearchSinkBuilderBase(abc.ABC):
    """
    Base builder to construct a ElasticsearchSink.
    """

    @abc.abstractmethod
    def __init__(self):
        self._j_elasticsearch_sink_builder = None

    @abc.abstractmethod
    def get_http_host_class(self):
        """
        Gets the org.apache.http.HttpHost class which path is different in different Elasticsearch
        version.
        """
        pass

    def set_emitter(self, emitter: ElasticsearchEmitter) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the emitter which is invoked on every record to convert it to Elasticsearch actions.

        :param emitter: The emitter to process records into Elasticsearch actions.
        """
        self._j_elasticsearch_sink_builder.setEmitter(emitter._j_emitter)
        return self

    def set_hosts(self, hosts: Union[str, List[str]]) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the hosts where the Elasticsearch cluster nodes are reachable.
        """
        if not isinstance(hosts, list):
            hosts = [hosts]
        JHttpHost = self.get_http_host_class()
        j_http_hosts_list = [JHttpHost.create(x) for x in hosts]
        j_http_hosts_array = to_jarray(JHttpHost, j_http_hosts_list)
        self._j_elasticsearch_sink_builder.setHosts(j_http_hosts_array)
        return self

    def set_delivery_guarantee(self, delivery_guarantee: DeliveryGuarantee) \
            -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the wanted DeliveryGuarantee. The default delivery guarantee is DeliveryGuarantee#NONE
        """
        j_delivery_guarantee = delivery_guarantee._to_j_delivery_guarantee()
        self._j_elasticsearch_sink_builder.setDeliveryGuarantee(j_delivery_guarantee)
        return self

    def set_bulk_flush_max_actions(self, num_max_actions: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
        disable it. The default flush size 1000.
        """
        self._j_elasticsearch_sink_builder.setBulkFlushMaxActions(num_max_actions)
        return self

    def set_bulk_flush_max_size_mb(self, max_size_mb: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
        disable it.
        """
        self._j_elasticsearch_sink_builder.setBulkFlushMaxSizeMb(max_size_mb)
        return self

    def set_bulk_flush_interval(self, interval_millis: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
        """
        self._j_elasticsearch_sink_builder.setBulkFlushInterval(interval_millis)
        return self

    def set_bulk_flush_backoff_strategy(self,
                                        flush_backoff_type: FlushBackoffType,
                                        max_retries: int,
                                        delay_millis: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the type of back off to use when flushing bulk requests. The default bulk flush back
        off type is FlushBackoffType#NONE.

        Sets the amount of delay between each backoff attempt when flushing bulk requests, in
        milliseconds.

        Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
        """
        self._j_elasticsearch_sink_builder.setBulkFlushBackoffStrategy(
            flush_backoff_type._to_j_flush_backoff_type(), max_retries, delay_millis)
        return self

    def set_connection_username(self, username: str) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the username used to authenticate the connection with the Elasticsearch cluster.
        """
        self._j_elasticsearch_sink_builder.setConnectionUsername(username)
        return self

    def set_connection_password(self, password: str) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the password used to authenticate the connection with the Elasticsearch cluster.
        """
        self._j_elasticsearch_sink_builder.setConnectionPassword(password)
        return self

    def set_connection_path_prefix(self, prefix: str) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets a prefix which used for every REST communication to the Elasticsearch cluster.
        """
        self._j_elasticsearch_sink_builder.setConnectionPathPrefix(prefix)
        return self

    def set_connection_request_timeout(self, timeout: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the timeout for requesting the connection of the Elasticsearch cluster from the
        connection manager.
        """
        self._j_elasticsearch_sink_builder.setConnectionRequestTimeout(timeout)
        return self

    def set_connection_timeout(self, timeout: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the timeout for establishing a connection of the Elasticsearch cluster.
        """
        self._j_elasticsearch_sink_builder.setConnectionTimeout(timeout)
        return self

    def set_socket_timeout(self, timeout: int) -> 'ElasticsearchSinkBuilderBase':
        """
        Sets the timeout for waiting for data or, put differently, a maximum period inactivity
        between two consecutive data packets.
        """
        self._j_elasticsearch_sink_builder.setSocketTimeout(timeout)
        return self

    def build(self) -> 'ElasticsearchSink':
        """
        Constructs the ElasticsearchSink with the properties configured this builder.
        """
        return ElasticsearchSink(self._j_elasticsearch_sink_builder.build())


class Elasticsearch6SinkBuilder(ElasticsearchSinkBuilderBase):
    """
    Builder to construct an Elasticsearch 6 compatible ElasticsearchSink.

    The following example shows the minimal setup to create a ElasticsearchSink that submits
    actions on checkpoint or the default number of actions was buffered (1000).

    Example:
    ::

        >>> sink = Elasticsearch6SinkBuilder() \\
        ...     .set_hosts('localhost:9200') \\
        ...     .set_emitter(ElasticsearchEmitter.static_index("user", "key_col")) \\
        ...     .build()
    """

    def __init__(self):
        self._j_elasticsearch_sink_builder = get_gateway().jvm \
            .org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder()

    def get_http_host_class(self):
        return get_gateway().jvm.org.apache.flink.elasticsearch6.shaded.org.apache.http.HttpHost


class Elasticsearch7SinkBuilder(ElasticsearchSinkBuilderBase):
    """
    Builder to construct an Elasticsearch 7 compatible ElasticsearchSink.

    The following example shows the minimal setup to create a ElasticsearchSink that submits
    actions on checkpoint or the default number of actions was buffered (1000).

    Example:
    ::

        >>> sink = Elasticsearch7SinkBuilder() \\
        ...     .set_hosts('localhost:9200') \\
        ...     .set_emitter(ElasticsearchEmitter.dynamic_index("index_col", "key_col")) \\
        ...     .build()
    """

    def __init__(self):
        self._j_elasticsearch_sink_builder = get_gateway().jvm \
            .org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder()

    def get_http_host_class(self):
        return get_gateway().jvm.org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost


class ElasticsearchSink(Sink):
    """
    Flink Sink to insert or update data in an Elasticsearch index. The sink supports the following
    delivery guarantees.

    DeliveryGuarantee.NONE does not provide any guarantees: actions are flushed to Elasticsearch
    only depending on the configurations of the bulk processor. In case of a failure, it might
    happen that actions are lost if the bulk processor still has buffered actions.

    DeliveryGuarantee.AT_LEAST_ONCE on a checkpoint the sink will wait until all buffered actions
    are flushed to and acknowledged by Elasticsearch. No actions will be lost but actions might be
    sent to Elasticsearch multiple times when Flink restarts. These additional requests may cause
    inconsistent data in ElasticSearch right after the restart, but eventually everything will be
    consistent again.
    """
    def __init__(self, j_elasticsearch_sink):
        super(ElasticsearchSink, self).__init__(sink=j_elasticsearch_sink)
