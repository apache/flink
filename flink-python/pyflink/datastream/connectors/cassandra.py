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

from pyflink.common import Duration
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import to_jarray


__all__ = ['ConsistencyLevel',
           'MapperOptions',
           'LoadBalancingPolicy',
           'ReconnectionPolicy',
           'RetryPolicy',
           'SpeculativeExecutionPolicy',
           'ClusterBuilder',
           'CassandraCommitter',
           'CassandraFailureHandler'
           ]

# ---- Classes introduced to construct the MapperOptions ----


class ConsistencyLevel(Enum):
    """
    The consistency level
    """
    ANY = 0
    ONE = 1
    TWO = 2
    THREE = 3
    QUORUM = 4
    ALL = 5
    LOCAL_QUORUM = 6
    EACH_QUORUM = 7
    SERIAL = 8
    LOCAL_SERIAL = 9
    LOCAL_ONE = 10

    def _to_j_consistency_level(self):
        JConsistencyLevel = get_gateway().jvm.com.datastax.driver.core.ConsistencyLevel
        return getattr(JConsistencyLevel, self.name)


class MapperOptions(object):
    """
    This class is used to configure a Mapper after deployment.
    """

    def __init__(self):
        """
        A simple method to construct MapperOptions.

        Example:
        ::

        >>> mapper_option = MapperOptions() \\
        ...    .ttl(1800) \\
        ...    .timestamp(3600) \\
        ...    .consistency_level(ConsistencyLevel.ANY) \\
        ...    .tracing(True) \\
        ...    .save_null_fields(True)
        """
        JSimpleMapperOptions = get_gateway().jvm.org.apache.flink.streaming.connectors. \
            cassandra.SimpleMapperOptions
        self._j_mapper_options = JSimpleMapperOptions()

    def ttl(self, ttl: int) -> 'MapperOptions':
        """
        Creates a new Option object to add time-to-live to a mapper operation. This is only
        valid for save operations.
        """
        self._j_mapper_options.ttl(ttl)
        return self

    def timestamp(self, timestamp: int) -> 'MapperOptions':
        """
        Creates a new Option object to add a timestamp to a mapper operation. This is only
        valid for save and delete operations.
        """
        self._j_mapper_options.timestamp(timestamp)
        return self

    def consistency_level(self, cl: ConsistencyLevel) -> 'MapperOptions':
        """
        Creates a new Option object to add a consistency level value to a mapper operation.
        This is valid for save, delete and get operations.
        """
        self._j_mapper_options.consistencyLevel(cl._to_j_consistency_level())
        return self

    def tracing(self, enabled: bool) -> 'MapperOptions':
        """
        Creates a new Option object to enable query tracing for a mapper operation. This is
        valid for save, delete and get operations.
        """
        self._j_mapper_options.tracing(enabled)
        return self

    def save_null_fields(self, enabled: bool) -> 'MapperOptions':
        """
        Creates a new Option object to specify whether null entity fields should be included in
        insert queries. This option is valid only for save operations.
        """
        self._j_mapper_options.saveNullFields(enabled)
        return self

    def if_not_exists(self, enabled: bool) -> 'MapperOptions':
        """
        Creates a new Option object to specify whether an IF NOT EXISTS clause should be included in
        insert queries. This option is valid only for save operations.

        If this option is not specified, it defaults to false (IF NOT EXISTS statements are not
        used).
        """
        self._j_mapper_options.ifNotExists(enabled)
        return self


# ---- Classes introduced to construct the ClusterBuilder ----


class LoadBalancingPolicy(object):
    """
    The policy that decides which Cassandra hosts to contact for each new query.

    The LoadBalancingPolicy is informed of hosts up/down events. For efficiency purposes, the policy
    is expected to exclude down hosts from query plans.
    """

    def __init__(self, j_load_balancing_policy):
        self._j_load_balancing_policy = j_load_balancing_policy

    @staticmethod
    def dc_aware_round_robin_policy() -> 'LoadBalancingPolicy':
        """
        A DCAwareRoundRobinPolicy with token awareness.

        This is also the default load balancing policy.
        """
        JPolicies = get_gateway().jvm.com.datastax.driver.core.policies.Policies
        return LoadBalancingPolicy(JPolicies.defaultLoadBalancingPolicy())

    @staticmethod
    def round_robin_policy() -> 'LoadBalancingPolicy':
        """
        A Round-robin load balancing policy.

        This policy queries nodes in a round-robin fashion. For a given query, if an host fail, the
        next one (following the round-robin order) is tried, until all hosts have been tried.

        This policy is not datacenter aware and will include every known Cassandra hosts in its
        round-robin algorithm. If you use multiple datacenter this will be inefficient, and you will
        want to use the DCAwareRoundRobinPolicy load balancing policy instead.
        """
        JRoundRobinPolicy = get_gateway().jvm.com.datastax.driver.core.policies.RoundRobinPolicy
        return LoadBalancingPolicy(JRoundRobinPolicy())


class ReconnectionPolicy(object):
    """
    Policy that decides how often the reconnection to a dead node is attempted.

    Note that if the driver receives a push notification from the Cassandra cluster that a node is
    UP, any existing ReconnectionSchedule on that node will be cancelled and a new one will be
    created (in effect, the driver reset the scheduler).

    The default ExponentialReconnectionPolicy policy is usually adequate.
    """

    def __init__(self, j_reconnection_policy):
        self._j_reconnection_policy = j_reconnection_policy

    @staticmethod
    def exponential_reconnection_policy(base_delay_ms: int = 1000, max_delay_ms: int = 600000) \
            -> 'ReconnectionPolicy':
        """
        The default load reconnection policy.

        A reconnection policy that waits exponentially longer between each reconnection attempt
        (but keeps a constant delay once a maximum delay is reached).
        """
        JExponentialReconnectionPolicy = get_gateway().jvm. \
            com.datastax.driver.core.policies.ExponentialReconnectionPolicy
        return ReconnectionPolicy(JExponentialReconnectionPolicy(base_delay_ms, max_delay_ms))

    @staticmethod
    def constant_reconnection_policy(constant_delay_ms: int) -> 'ReconnectionPolicy':
        """
        A reconnection policy that waits a constant time between each reconnection attempt.
        """
        JConstantReconnectionPolicy = get_gateway().jvm.\
            com.datastax.driver.core.policies.ConstantReconnectionPolicy
        return ReconnectionPolicy(JConstantReconnectionPolicy(constant_delay_ms))


class RetryPolicy(object):
    """
    A policy that defines a default behavior to adopt when a request fails.

    There are three possible decisions:
    - RETHROW: no retry should be attempted and an exception should be thrown.
    - RETRY: the operation will be retried. The consistency level of the retry should be specified.
    - IGNORE: no retry should be attempted and the exception should be ignored. In that case, the
              operation that triggered the Cassandra exception will return an empty result set.
    """

    def __init__(self, j_retry_policy):
        self._j_retry_policy = j_retry_policy

    @staticmethod
    def consistency_retry_policy() -> 'RetryPolicy':
        """
        The default retry policy.

        This policy retries queries in only two cases:
        - On a read timeout, retries once on the same host if enough replicas replied but data was
          not retrieved.
        - On a write timeout, retries once on the same host if we timeout while writing the
          distributed log used by batch statements.
        - On an unavailable exception, retries once on the next host.
        - On a request error, such as a client timeout, the query is retried on the next host.
          Do not retry on read or write failures.
        """
        JPolicies = get_gateway().jvm.com.datastax.driver.core.policies.Policies
        return RetryPolicy(JPolicies.defaultRetryPolicy())

    @staticmethod
    def fallthrough_retry_policy() -> 'RetryPolicy':
        """
        A retry policy that never retries (nor ignores).
        """
        JFallthroughRetryPolicy = get_gateway().jvm.com.datastax.driver.core.policies. \
            FallthroughRetryPolicy
        return RetryPolicy(JFallthroughRetryPolicy.INSTANCE)

    @staticmethod
    def logging_retry_policy(policy: 'RetryPolicy') -> 'RetryPolicy':
        """
        Creates a new RetryPolicy that logs the decision of policy.
        """
        JLoggingRetryPolicy = get_gateway().jvm.com.datastax.driver.core.policies. \
            LoggingRetryPolicy
        return RetryPolicy(JLoggingRetryPolicy(policy._j_retry_policy))


class SpeculativeExecutionPolicy(object):
    """
    The policy that decides if the driver will send speculative queries to the next hosts when the
    current host takes too long to respond.

    Note that only idempotent statements will be speculatively retried.
    """

    def __init__(self, j_speculative_execution_policy):
        self._j_speculative_execution_policy = j_speculative_execution_policy

    @staticmethod
    def no_speculative_execution_policy() -> 'SpeculativeExecutionPolicy':
        """
        The default speculative retry policy.

        A SpeculativeExecutionPolicy that never schedules speculative executions.
        """
        JNoSpeculativeExecutionPolicy = get_gateway().jvm. \
            com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy
        return SpeculativeExecutionPolicy(JNoSpeculativeExecutionPolicy.INSTANCE)

    @staticmethod
    def constant_speculative_execution_policy(constant_delay_millis: int,
                                              max_speculative_executions: int) \
            -> 'SpeculativeExecutionPolicy':
        """
        A SpeculativeExecutionPolicy that schedules a given number of speculative executions,
        separated by a fixed delay.
        """
        JConstantSpeculativeExecutionPolicy = get_gateway().jvm. \
            org.apache.flink.streaming.connectors.cassandra.ConstantSpeculativeExecutionPolicyExt
        return SpeculativeExecutionPolicy(
            JConstantSpeculativeExecutionPolicy(constant_delay_millis, max_speculative_executions))


class ClusterBuilder(object):
    """
    This class is used to configure a Cluster after deployment. The cluster represents the
    connection that will be established to Cassandra.

    A simple method to construct ClusterBuilder.

    Example:
    ::

    >>> cluster_builder = ClusterBuilder() \\
    ...    .add_contact_points("127.0.0.1") \\
    ...    .with_port(9042) \\
    ...    .with_cluster_name("cluster_name") \\
    ...    .with_credentials("user", "password")
    """

    def __init__(self):
        JSimpleClusterBuilder = get_gateway().jvm.\
            org.apache.flink.streaming.connectors.cassandra.SimpleClusterBuilder
        self._j_cluster_builder = JSimpleClusterBuilder()

    def with_cluster_name(self, name: str) -> 'ClusterBuilder':
        """
        An optional name for the creation cluster.

        Note: this is not related to the Cassandra cluster name (though you are free to provide the
        same name).
        """
        self._j_cluster_builder.withClusterName(name)
        return self

    def with_port(self, port: int) -> 'ClusterBuilder':
        """
        The port to use to connect to the Cassandra host.

        If not set through this method, the default port (9042) will be used instead.
        """
        self._j_cluster_builder.withPort(port)
        return self

    def allow_beta_protocol_version(self) -> 'ClusterBuilder':
        """
        Create cluster connection using latest development protocol version, which is currently in
        beta. Calling this method will result into setting USE_BETA flag in all outgoing messages,
        which allows server to negotiate the supported protocol version even if it is currently in
        beta.

        This feature is only available starting with version V5.

        Use with caution, refer to the server and protocol documentation for the details on latest
        protocol version.
        """
        self._j_cluster_builder.allowBetaProtocolVersion()
        return self

    def with_max_schema_agreement_wait_seconds(self, max_schema_agreement_wait_seconds: int) \
            -> 'ClusterBuilder':
        """
        Sets the maximum time to wait for schema agreement before returning from a DDL query.

        If not set through this method, the default value (10 seconds) will be used.
        """
        self._j_cluster_builder.withMaxSchemaAgreementWaitSeconds(max_schema_agreement_wait_seconds)
        return self

    def add_contact_point(self, address: str) -> 'ClusterBuilder':
        """
        Adds a contact point.
        """
        self._j_cluster_builder.addContactPoint(address)
        return self

    def add_contact_points(self, *addresses: str) -> 'ClusterBuilder':
        """
        Adds contact points.
        """
        self._j_cluster_builder.addContactPoints(to_jarray(get_gateway().jvm.String, addresses))
        return self

    def add_contact_points_with_ports(self, *addresses: str) -> 'ClusterBuilder':
        """
        Adds contact points with ports.

        Note: All the addresses should be the following format: "hostname:port".
        """
        JInetSocketAddress = get_gateway().jvm.java.net.InetSocketAddress

        j_addresses = []
        for add in addresses:
            split = add.split(":")
            hostname = split[0]
            port = int(split[1])
            j_addresses.append(JInetSocketAddress(hostname, port))

        j_inet_socket_address_array = to_jarray(JInetSocketAddress, j_addresses)
        self._j_cluster_builder.addContactPointsWithPorts(j_inet_socket_address_array)
        return self

    def with_load_balancing_policy(self, policy: LoadBalancingPolicy) -> 'ClusterBuilder':
        """
        Configures the load balancing policy to use for the new cluster.

        If no load balancing policy is set through this method, the DCAwareRoundRobinPolicy will be
        used instead.
        """
        self._j_cluster_builder.withLoadBalancingPolicy(policy._j_load_balancing_policy)
        return self

    def with_reconnection_policy(self, policy: ReconnectionPolicy) -> 'ClusterBuilder':
        """
        Configures the reconnection policy to use for the new cluster.

        If no reconnection policy is set through this method, the ExponentialReconnectionPolicy
        will be used instead.
        """
        self._j_cluster_builder.withReconnectionPolicy(policy._j_reconnection_policy)
        return self

    def with_retry_policy(self, policy: RetryPolicy) -> 'ClusterBuilder':
        """
        Configures the retry policy to use for the new cluster.

        If no retry policy is set through this method, Consistency Retry policy will be used
        instead.
        """
        self._j_cluster_builder.withRetryPolicy(policy._j_retry_policy)
        return self

    def with_speculative_execution_policy(self, policy: SpeculativeExecutionPolicy) \
            -> 'ClusterBuilder':
        """
        Configures the speculative execution policy to use for the new cluster.
        """
        self._j_cluster_builder.withSpeculativeExecutionPolicy(
            policy._j_speculative_execution_policy)
        return self

    def with_credentials(self, username: str, password: str) -> 'ClusterBuilder':
        """
        Uses the provided credentials when connecting to Cassandra hosts.

        This should be used if the Cassandra cluster has been configured to use the
        PasswordAuthenticator. If the default AllowAllAuthenticator is used instead, using this
        method has no effect.
        """
        self._j_cluster_builder.withCredentials(username, password)
        return self

    def without_metrics(self) -> 'ClusterBuilder':
        """
        Disables metrics collection for the created cluster (metrics are enabled by default
        otherwise).
        """
        self._j_cluster_builder.withoutMetrics()
        return self

    def without_jmx_reporting(self) -> 'ClusterBuilder':
        """
        Disables JMX reporting of the metrics.

        JMX reporting is enabled by default but can be disabled using this option. If metrics are
        disabled, this is a no-op
        """
        self._j_cluster_builder.withoutJMXReporting()
        return self

    def with_no_compact(self) -> 'ClusterBuilder':
        """
        Enables the NO_COMPACT startup option.

        When this option is supplied, SELECT, UPDATE, DELETE and BATCH statements on COMPACT STORAGE
        tables function in "compatibility" mode which allows seeing these tables as if they were
        "regular" CQL tables.

        This option only effects interactions with tables using COMPACT STORAGE and is only
        supported by C* 4.0+ and DSE 6.0+.
        """
        self._j_cluster_builder.withNoCompact()
        return self


class CassandraCommitter(object):
    """
    CheckpointCommitter that saves information about completed checkpoints within a separate table
    in a cassandra database.
    """

    def __init__(self, j_checkpoint_committer):
        self._j_checkpoint_committer = j_checkpoint_committer

    @staticmethod
    def default_checkpoint_committer(builder: ClusterBuilder, key_space: str = None) \
            -> 'CassandraCommitter':
        """
        CheckpointCommitter that saves information about completed checkpoints within a separate
        table in a cassandra database.

        Entries are in the form |operator_id | subtask_id | last_completed_checkpoint|
        """
        JCassandraCommitter = get_gateway().jvm.org.apache.flink.streaming.connectors.\
            cassandra.CassandraCommitter
        if key_space is None:
            j_checkpoint_committer = JCassandraCommitter(builder._j_cluster_builder)
        else:
            j_checkpoint_committer = JCassandraCommitter(builder._j_cluster_builder, key_space)
        return CassandraCommitter(j_checkpoint_committer)


class CassandraFailureHandler(object):
    """
    Handle a failed Throwable.
    """

    def __init__(self, j_cassandra_failure_handler):
        self._j_cassandra_failure_handler = j_cassandra_failure_handler

    @staticmethod
    def no_op() -> 'CassandraFailureHandler':
        """
        A CassandraFailureHandler that simply fails the sink on any failures.
        This is also the default failure handler if not specified.
        """
        return CassandraFailureHandler(get_gateway().jvm.org.apache.flink.streaming.connectors.
                                       cassandra.NoOpCassandraFailureHandler())


# ---- CassandraSink ----


class CassandraSink(object):
    """
    Sets the ClusterBuilder for this sink. A ClusterBuilder is used to configure the connection to
    cassandra.
    """

    def __init__(self, j_cassandra_sink):
        self._j_cassandra_sink = j_cassandra_sink

    def name(self, name: str) -> 'CassandraSink':
        """
        Set the name of this sink. This name is used by the visualization and logging during
        runtime.
        """
        self._j_cassandra_sink.name(name)
        return self

    def uid(self, uid: str) -> 'CassandraSink':
        """
        Sets an ID for this operator. The specified ID is used to assign the same operator ID
        across job submissions (for example when starting a job from a savepoint).
        Note that this ID needs to be unique per transformation and job. Otherwise, job submission
        will fail.
        """
        self._j_cassandra_sink.uid(uid)
        return self

    def set_uid_hash(self, uid_hash: str) -> 'CassandraSink':
        """
        Sets an user provided hash for this operator. This will be used AS IS the create the
        JobVertexID.

        The user provided hash is an alternative to the generated hashes, that is considered when
        identifying an operator through the default hash mechanics fails (e.g. because of changes
        between Flink versions).

        Note that this should be used as a workaround or for troubleshooting. The provided hash
        needs to be unique per transformation and job. Otherwise, job submission will fail.
        Furthermore, you cannot assign user-specified hash to intermediate nodes in an operator
        chain and trying so will let your job fail.

        A use case for this is in migration between Flink versions or changing the jobs in a way
        that changes the automatically generated hashes. In this case, providing the previous hashes
        directly through this method (e.g. obtained from old logs) can help to reestablish a lost
        mapping from states to their target operator.
        """
        self._j_cassandra_sink.setUidHash(uid_hash)
        return self

    def set_parallelism(self, parallelism: int) -> 'CassandraSink':
        """
        Sets the parallelism for this sink. The degree must be higher than zero.
        """
        self._j_cassandra_sink.setParallelism(parallelism)
        return self

    def disable_chaining(self) -> 'CassandraSink':
        """
        Turns off chaining for this operator so thread co-location will not be used as an
        optimization.
        """
        self._j_cassandra_sink.disableChaining()
        return self

    def slot_sharing_group(self, slot_sharing_group: str) -> 'CassandraSink':
        """
        Sets the slot sharing group of this operation. Parallel instances of operations that are in
        the same slot sharing group will be co-located in the same TaskManager slot, if possible.

        Operations inherit the slot sharing group of input operations if all input operations are in
        the same slot sharing group and no slot sharing group was explicitly specified.

        Initially an operation is in the default slot sharing group. An operation can be put into
        the default group explicitly by setting the slot sharing group to {@code "default"}.
        """
        self._j_cassandra_sink.slotSharingGroup(slot_sharing_group)
        return self

    @staticmethod
    def add_sink(input) -> 'CassandraSinkBuilder':
        """
        Writes a DataStream into a Cassandra database.
        """
        JCassandraSink = get_gateway().jvm \
            .org.apache.flink.streaming.connectors.cassandra.CassandraSink
        j_cassandra_sink_builder = JCassandraSink.addSink(input._j_data_stream)
        return CassandraSink.CassandraSinkBuilder(j_cassandra_sink_builder)

    class CassandraSinkBuilder(object):
        """
        Builder for a CassandraSink.
        """

        def __init__(self, j_cassandra_sink_builder):
            self._j_cassandra_sink_builder = j_cassandra_sink_builder

        def set_query(self, query: str) -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the query that is to be executed for every record.
            """
            self._j_cassandra_sink_builder.setQuery(query)
            return self

        def set_host(self, host: str, port: int = 9042) -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the cassandra host/port to connect to.
            """
            self._j_cassandra_sink_builder.setHost(host, port)
            return self

        def set_cluster_builder(self, builder: ClusterBuilder) \
                -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the ClusterBuilder for this sink. A ClusterBuilder is used to configure the
            connection to cassandra.
            """
            self._j_cassandra_sink_builder.setClusterBuilder(builder._j_cluster_builder)
            return self

        def enable_write_ahead_log(self, committer: CassandraCommitter = None) \
                -> 'CassandraSink.CassandraSinkBuilder':
            """
            Enables the write-ahead log, which allows exactly-once processing for non-deterministic
            algorithms that use idempotent updates.
            """
            if committer is None:
                self._j_cassandra_sink_builder.enableWriteAheadLog()
            else:
                self._j_cassandra_sink_builder.enableWriteAheadLog(
                    committer._j_checkpoint_committer)
            return self

        def set_mapper_options(self, options: MapperOptions) \
                -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the mapper options for this sink. The mapper options are used to configure the
            DataStax com.datastax.driver.mapping.Mapper when writing POJOs.
            This call has no effect if the input DataStream for this sink does not contain POJOs.
            """
            self._j_cassandra_sink_builder.setMapperOptions(options._j_mapper_options)
            return self

        def set_failure_handler(self, failure_handler: CassandraFailureHandler) \
                -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the failure handler for this sink. The failure handler is used to provide custom
            error handling.
            """
            self._j_cassandra_sink_builder.setFailureHandler(
                failure_handler._j_cassandra_failure_handler)
            return self

        def set_max_concurrent_requests(self,
                                        max_concurrent_requests: int,
                                        duration: Duration = None) \
                -> 'CassandraSink.CassandraSinkBuilder':
            """
            Sets the maximum allowed number of concurrent requests for this sink.
            """
            if duration is None:
                self._j_cassandra_sink_builder.setMaxConcurrentRequests(max_concurrent_requests)
            else:
                self._j_cassandra_sink_builder.setMaxConcurrentRequests(
                    max_concurrent_requests, duration._j_duration)
            return self

        def enable_ignore_null_fields(self) -> 'CassandraSink.CassandraSinkBuilder':
            """
            Enables ignoring null values, treats null values as unset and avoids writing null fields
            and creating tombstones.
            This call has no effect if CassandraSinkBuilder.enable_write_ahead_log() is called.
            """
            self._j_cassandra_sink_builder.enableIgnoreNullFields()
            return self

        def build(self) -> 'CassandraSink':
            """
            Finalizes the configuration of this sink.
            """
            return CassandraSink(self._j_cassandra_sink_builder.build())
