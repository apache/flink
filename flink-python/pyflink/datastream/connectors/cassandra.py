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


__all__ = [
    'CassandraSink',
    'ConsistencyLevel',
    'MapperOptions',
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


class ClusterBuilder(object):
    """
    This class is used to configure a Cluster after deployment. The cluster represents the
    connection that will be established to Cassandra.
    """

    def __init__(self, j_cluster_builder):
        self._j_cluster_builder = j_cluster_builder


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

        Entries are in the form: | operator_id | subtask_id | last_completed_checkpoint |
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

        Note that this should be used as a workaround or for trouble shooting. The provided hash
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
            This call has no effect if CassandraSinkBuilder.enableWriteAheadLog() is called.
            """
            self._j_cassandra_sink_builder.enableIgnoreNullFields()
            return self

        def build(self) -> 'CassandraSink':
            """
            Finalizes the configuration of this sink.
            """
            return CassandraSink(self._j_cassandra_sink_builder.build())
