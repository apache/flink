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

from abc import ABCMeta
from enum import Enum

from py4j.java_gateway import get_java_class
from typing import List, Optional

from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import load_java_class

__all__ = [
    'StateBackend',
    'HashMapStateBackend',
    'EmbeddedRocksDBStateBackend',
    'MemoryStateBackend',
    'FsStateBackend',
    'RocksDBStateBackend',
    'CustomStateBackend',
    'PredefinedOptions']


def _from_j_state_backend(j_state_backend):
    if j_state_backend is None:
        return None
    gateway = get_gateway()
    JStateBackend = gateway.jvm.org.apache.flink.runtime.state.StateBackend
    JHashMapStateBackend = gateway.jvm.org.apache.flink.runtime.state.hashmap.HashMapStateBackend
    JEmbeddedRocksDBStateBackend = gateway.jvm.org.apache.flink.contrib.streaming.state.\
        EmbeddedRocksDBStateBackend
    JMemoryStateBackend = gateway.jvm.org.apache.flink.runtime.state.memory.MemoryStateBackend
    JFsStateBackend = gateway.jvm.org.apache.flink.runtime.state.filesystem.FsStateBackend
    JRocksDBStateBackend = gateway.jvm.org.apache.flink.contrib.streaming.state.RocksDBStateBackend
    j_clz = j_state_backend.getClass()

    if not get_java_class(JStateBackend).isAssignableFrom(j_clz):
        raise TypeError("The input %s is not an instance of StateBackend." % j_state_backend)

    if get_java_class(JHashMapStateBackend).isAssignableFrom(j_state_backend.getClass()):
        return HashMapStateBackend(j_hashmap_state_backend=j_state_backend.getClass())
    elif get_java_class(JEmbeddedRocksDBStateBackend).isAssignableFrom(j_state_backend.getClass()):
        return EmbeddedRocksDBStateBackend(j_embedded_rocks_db_state_backend=j_state_backend)
    elif get_java_class(JMemoryStateBackend).isAssignableFrom(j_state_backend.getClass()):
        return MemoryStateBackend(j_memory_state_backend=j_state_backend)
    elif get_java_class(JFsStateBackend).isAssignableFrom(j_state_backend.getClass()):
        return FsStateBackend(j_fs_state_backend=j_state_backend)
    elif get_java_class(JRocksDBStateBackend).isAssignableFrom(j_state_backend.getClass()):
        return RocksDBStateBackend(j_rocks_db_state_backend=j_state_backend)
    else:
        return CustomStateBackend(j_state_backend)  # users' customized state backend


class StateBackend(object, metaclass=ABCMeta):
    """
    A **State Backend** defines how the state of a streaming application is stored locally within
    the cluster. Different state backends store their state in different fashions, and use different
    data structures to hold the state of running applications.

    For example, the :class:`HashMapStateBackend` keeps working state in the memory of the
    TaskManager. The backend is lightweight and without additional dependencies.

    The :class:`EmbeddedRocksDBStateBackend` keeps working state in the memory of the TaskManager
    and stores state checkpoints in a filesystem(typically a replicated highly-available filesystem,
    like `HDFS <https://hadoop.apache.org/>`_, `Ceph <https://ceph.com/>`_,
    `S3 <https://aws.amazon.com/documentation/s3/>`_, `GCS <https://cloud.google.com/storage/>`_,
    etc).

    The :class:`EmbeddedRocksDBStateBackend` stores working state in an embedded
    `RocksDB <http://rocksdb.org/>`_, instance and is able to scale working state to many
    terrabytes in size, only limited by available disk space across all task managers.

    **Raw Bytes Storage and Backends**

    The :class:`StateBackend` creates services for *raw bytes storage* and for *keyed state*
    and *operator state*.

    The `org.apache.flink.runtime.state.AbstractKeyedStateBackend and
    `org.apache.flink.runtime.state.OperatorStateBackend` created by this state backend define how
    to hold the working state for keys and operators. They also define how to checkpoint that
    state, frequently using the raw bytes storage (via the
    `org.apache.flink.runtime.state.CheckpointStreamFactory`). However, it is also possible that
    for example a keyed state backend simply implements the bridge to a key/value store, and that
    it does not need to store anything in the raw byte storage upon a checkpoint.

    **Serializability**

    State Backends need to be serializable(`java.io.Serializable`), because they distributed
    across parallel processes (for distributed execution) together with the streaming application
    code.

    Because of that, :class:`StateBackend` implementations are meant to be like *factories* that
    create the proper states stores that provide access to the persistent storage and hold the
    keyed- and operator state data structures. That way, the State Backend can be very lightweight
    (contain only configurations) which makes it easier to be serializable.

    **Thread Safety**

    State backend implementations have to be thread-safe. Multiple threads may be creating
    streams and keyed-/operator state backends concurrently.
    """

    def __init__(self, j_state_backend):
        self._j_state_backend = j_state_backend


class HashMapStateBackend(StateBackend):
    """
    This state backend holds the working state in the memory (JVM heap) of the TaskManagers
    and checkpoints based on the configured CheckpointStorage.

    **State Size Considerations**

    Working state is kept on the TaskManager heap. If a TaskManager executes multiple
    tasks concurrently (if the TaskManager has multiple slots, or if slot-sharing is used)
    then the aggregate state of all tasks needs to fit into that TaskManager's memory.

    **Configuration**

    As for all state backends, this backend can either be configured within the application (by
    creating the backend with the respective constructor parameters and setting it on the execution
    environment) or by specifying it in the Flink configuration.

    If the state backend was specified in the application, it may pick up additional configuration
    parameters from the Flink configuration. For example, if the backend if configured in the
    application without a default savepoint directory, it will pick up a default savepoint
    directory specified in the Flink configuration of the running job/cluster. That behavior is
    implemented via the :func:`configure` method.
    """

    def __init__(self, j_hashmap_state_backend=None):
        """
        Creates a new MemoryStateBackend, setting optionally the paths to persist checkpoint
        metadata and savepoints to, as well as configuring state thresholds and asynchronous
        operations.

        WARNING: Increasing the size of this value beyond the default value
        (:data:`DEFAULT_MAX_STATE_SIZE`) should be done with care.
        The checkpointed state needs to be send to the JobManager via limited size RPC messages,
        and there and the JobManager needs to be able to hold all aggregated state in its memory.

        Example:
        ::
            >>> state_backend = HashMapStateBackend()

        :param j_hashmap_state_backend: For internal use, please keep none.
        """
        if j_hashmap_state_backend is None:
            gateway = get_gateway()
            JHashMapStateBackend = gateway.jvm.org.apache.flink.runtime.state.hashmap\
                .HashMapStateBackend

            j_hashmap_state_backend = JHashMapStateBackend()

        super(HashMapStateBackend, self).__init__(j_hashmap_state_backend)

    def __str__(self):
        return self._j_state_backend.toString()


class EmbeddedRocksDBStateBackend(StateBackend):
    """
    A State Backend that stores its state in an embedded ``RocksDB`` instance. This state backend
    can store very large state that exceeds memory and spills to local disk.

    All key/value state (including windows) is stored in the key/value index of RocksDB.
    For persistence against loss of machines, please configure a CheckpointStorage instance
    for the Job.

    The behavior of the RocksDB instances can be parametrized by setting RocksDB Options
    using the methods :func:`set_predefined_options` and :func:`set_options`.
    """

    def __init__(self,
                 enable_incremental_checkpointing=None,
                 j_embedded_rocks_db_state_backend=None):
        """
        Creates a new :class:`EmbeddedRocksDBStateBackend` for storing local state.

        Example:
        ::

            >>> state_backend = EmbeddedRocksDBStateBackend()

        :param enable_incremental_checkpointing: True if incremental checkpointing is enabled.
        :param j_embedded_rocks_db_state_backend: For internal use, please keep none.
        """
        if j_embedded_rocks_db_state_backend is None:
            gateway = get_gateway()
            JTernaryBoolean = gateway.jvm.org.apache.flink.util.TernaryBoolean
            JEmbeddedRocksDBStateBackend = gateway.jvm.org.apache.flink.contrib.streaming.state \
                .EmbeddedRocksDBStateBackend

            if enable_incremental_checkpointing not in (None, True, False):
                raise TypeError("Unsupported input for 'enable_incremental_checkpointing': %s, "
                                "the value of the parameter should be None or"
                                "True or False.")

            if enable_incremental_checkpointing is None:
                j_enable_incremental_checkpointing = JTernaryBoolean.UNDEFINED
            elif enable_incremental_checkpointing is True:
                j_enable_incremental_checkpointing = JTernaryBoolean.TRUE
            else:
                j_enable_incremental_checkpointing = JTernaryBoolean.FALSE

            j_embedded_rocks_db_state_backend = \
                JEmbeddedRocksDBStateBackend(j_enable_incremental_checkpointing)

        super(EmbeddedRocksDBStateBackend, self).__init__(j_embedded_rocks_db_state_backend)

    def set_db_storage_paths(self, *paths: str):
        """
        Sets the directories in which the local RocksDB database puts its files (like SST and
        metadata files). These directories do not need to be persistent, they can be ephemeral,
        meaning that they are lost on a machine failure, because state in RocksDB is persisted
        in checkpoints.

        If nothing is configured, these directories default to the TaskManager's local
        temporary file directories.

        Each distinct state will be stored in one path, but when the state backend creates
        multiple states, they will store their files on different paths.

        Passing ``None`` to this function restores the default behavior, where the configured
        temp directories will be used.

        :param paths: The paths across which the local RocksDB database files will be spread. this
                      parameter is optional.
        """
        if len(paths) < 1:
            self._j_state_backend.setDbStoragePath(None)
        else:
            gateway = get_gateway()
            j_path_array = gateway.new_array(gateway.jvm.String, len(paths))
            for i in range(0, len(paths)):
                j_path_array[i] = paths[i]
            self._j_state_backend.setDbStoragePaths(j_path_array)

    def get_db_storage_paths(self) -> List[str]:
        """
        Gets the configured local DB storage paths, or null, if none were configured.

        Under these directories on the TaskManager, RocksDB stores its SST files and
        metadata files. These directories do not need to be persistent, they can be ephermeral,
        meaning that they are lost on a machine failure, because state in RocksDB is persisted
        in checkpoints.

        If nothing is configured, these directories default to the TaskManager's local
        temporary file directories.

        :return: The list of configured local DB storage paths.
        """
        return list(self._j_state_backend.getDbStoragePaths())

    def is_incremental_checkpoints_enabled(self) -> bool:
        """
        Gets whether incremental checkpoints are enabled for this state backend.

        :return: True if incremental checkpoints are enabled, false otherwise.
        """
        return self._j_state_backend.isIncrementalCheckpointsEnabled()

    def set_predefined_options(self, options: 'PredefinedOptions'):
        """
        Sets the predefined options for RocksDB.

        If user-configured options within ``RocksDBConfigurableOptions`` is set (through
        flink-conf.yaml) or a user-defined options factory is set (via :func:`setOptions`),
        then the options from the factory are applied on top of the here specified
        predefined options and customized options.

        Example:
        ::

            >>> state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED)

        :param options: The options to set (must not be null), see :class:`PredefinedOptions`.
        """
        self._j_state_backend\
            .setPredefinedOptions(options._to_j_predefined_options())

    def get_predefined_options(self) -> 'PredefinedOptions':
        """
        Gets the current predefined options for RocksDB.
        The default options (if nothing was set via :func:`setPredefinedOptions`)
        are :data:`PredefinedOptions.DEFAULT`.

        If user-configured options within ``RocksDBConfigurableOptions`` is set (through
        flink-conf.yaml) or a user-defined options factory is set (via :func:`setOptions`),
        then the options from the factory are applied on top of the predefined and customized
        options.

        .. seealso:: :func:`set_predefined_options`

        :return: Current predefined options.
        """
        j_predefined_options = self._j_state_backend.getPredefinedOptions()
        return PredefinedOptions._from_j_predefined_options(j_predefined_options)

    def set_options(self, options_factory_class_name: str):
        """
        Sets ``org.rocksdb.Options`` for the RocksDB instances.
        Because the options are not serializable and hold native code references,
        they must be specified through a factory.

        The options created by the factory here are applied on top of the pre-defined
        options profile selected via :func:`set_predefined_options`.
        If the pre-defined options profile is the default (:data:`PredefinedOptions.DEFAULT`),
        then the factory fully controls the RocksDB options.

        :param options_factory_class_name: The fully-qualified class name of the options
                                           factory in Java that lazily creates the RocksDB options.
                                           The options factory must have a default constructor.
        """
        gateway = get_gateway()
        JOptionsFactory = gateway.jvm.org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory
        j_options_factory_clz = load_java_class(options_factory_class_name)
        if not get_java_class(JOptionsFactory).isAssignableFrom(j_options_factory_clz):
            raise ValueError("The input class does not implement RocksDBOptionsFactory.")
        self._j_state_backend\
            .setRocksDBOptions(j_options_factory_clz.newInstance())

    def get_options(self) -> Optional[str]:
        """
        Gets the fully-qualified class name of the options factory in Java that lazily creates
        the RocksDB options.

        :return: The fully-qualified class name of the options factory in Java.
        """
        j_options_factory = self._j_state_backend.getRocksDBOptions()
        if j_options_factory is not None:
            return j_options_factory.getClass().getName()
        else:
            return None

    def get_number_of_transfer_threads(self) -> int:
        """
        Gets the number of threads used to transfer files while snapshotting/restoring.

        :return: The number of threads used to transfer files while snapshotting/restoring.
        """
        return self._j_state_backend.getNumberOfTransferThreads()

    def set_number_of_transfer_threads(self, number_of_transfering_threads: int):
        """
        Sets the number of threads used to transfer files while snapshotting/restoring.

        :param number_of_transfering_threads: The number of threads used to transfer files while
                                              snapshotting/restoring.
        """
        self._j_state_backend\
            .setNumberOfTransferThreads(number_of_transfering_threads)

    def __str__(self):
        return self._j_state_backend.toString()


class MemoryStateBackend(StateBackend):
    """
    **IMPORTANT** `MemoryStateBackend` is deprecated in favor of `HashMapStateBackend` and
    `JobManagerCheckpointStorage`. This change does not affect the runtime characteristics of your
    Jobs and is simply an API change to help better communicate the ways Flink separates local state
    storage from fault tolerance. Jobs can be upgraded without loss of state. If configuring
    your state backend via the `StreamExecutionEnvironment` please make the following changes.

    ::

        >> env.set_state_backend(HashMapStateBackend())
        >> env.get_checkpoint_config().set_checkpoint_storage(JobManagerCheckpointStorage())

    If you are configuring your state backend via the `flink-conf.yaml` please make the following
    changes.

    ```
    state.backend: hashmap
    state.checkpoint-storage: jobmanager
    ```

    This state backend holds the working state in the memory (JVM heap) of the TaskManagers.
    The state backend checkpoints state directly to the JobManager's memory (hence the backend's
    name), but the checkpoints will be persisted to a file system for high-availability setups and
    savepoints. The MemoryStateBackend is consequently a FileSystem-based backend that can work
    without a file system dependency in simple setups.

    This state backend should be used only for experimentation, quick local setups,
    or for streaming applications that have very small state: Because it requires checkpoints to
    go through the JobManager's memory, larger state will occupy larger portions of the
    JobManager's main memory, reducing operational stability.
    For any other setup, the :class:`FsStateBackend` should be used. The :class:`FsStateBackend`
    holds the working state on the TaskManagers in the same way, but checkpoints state directly to
    files rather then to the JobManager's memory, thus supporting large state sizes.

    **State Size Considerations**

    State checkpointing with this state backend is subject to the following conditions:

    - Each individual state must not exceed the configured maximum state size
      (see :func:`get_max_state_size`.

    - All state from one task (i.e., the sum of all operator states and keyed states from all
      chained operators of the task) must not exceed what the RPC system supports, which is
      be default < 10 MB. That limit can be configured up, but that is typically not advised.

    - The sum of all states in the application times all retained checkpoints must comfortably
      fit into the JobManager's JVM heap space.

    **Persistence Guarantees**

    For the use cases where the state sizes can be handled by this backend, the backend does
    guarantee persistence for savepoints, externalized checkpoints (of configured), and checkpoints
    (when high-availability is configured).

    **Configuration**

    As for all state backends, this backend can either be configured within the application (by
    creating the backend with the respective constructor parameters and setting it on the execution
    environment) or by specifying it in the Flink configuration.

    If the state backend was specified in the application, it may pick up additional configuration
    parameters from the Flink configuration. For example, if the backend if configured in the
    application without a default savepoint directory, it will pick up a default savepoint
    directory specified in the Flink configuration of the running job/cluster. That behavior is
    implemented via the :func:`configure` method.
    """

    # The default maximal size that the snapshotted memory state may have (5 MiBytes).
    DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024

    def __init__(self,
                 checkpoint_path=None,
                 savepoint_path=None,
                 max_state_size=None,
                 using_asynchronous_snapshots=None,
                 j_memory_state_backend=None):
        """
        Creates a new MemoryStateBackend, setting optionally the paths to persist checkpoint
        metadata and savepoints to, as well as configuring state thresholds and asynchronous
        operations.

        WARNING: Increasing the size of this value beyond the default value
        (:data:`DEFAULT_MAX_STATE_SIZE`) should be done with care.
        The checkpointed state needs to be send to the JobManager via limited size RPC messages,
        and there and the JobManager needs to be able to hold all aggregated state in its memory.

        Example:
        ::
            >>> state_backend = MemoryStateBackend()

        :param checkpoint_path: The path to write checkpoint metadata to. If none, the value from
                                the runtime configuration will be used.
        :param savepoint_path: The path to write savepoints to. If none, the value from
                               the runtime configuration will be used.
        :param max_state_size: The maximal size of the serialized state. If none, the
                               :data:`DEFAULT_MAX_STATE_SIZE` will be used.
        :param using_asynchronous_snapshots: Snapshots are now always asynchronous. This flag
                                             has no effect anymore in this version.
        :param j_memory_state_backend: For internal use, please keep none.
        """
        if j_memory_state_backend is None:
            gateway = get_gateway()
            JTernaryBoolean = gateway.jvm.org.apache.flink.util.TernaryBoolean
            JMemoryStateBackend = gateway.jvm.org.apache.flink.runtime.state.memory\
                .MemoryStateBackend
            if using_asynchronous_snapshots is None:
                j_asynchronous_snapshots = JTernaryBoolean.UNDEFINED
            elif using_asynchronous_snapshots is True:
                j_asynchronous_snapshots = JTernaryBoolean.TRUE
            elif using_asynchronous_snapshots is False:
                j_asynchronous_snapshots = JTernaryBoolean.FALSE
            else:
                raise TypeError("Unsupported input for 'using_asynchronous_snapshots': %s, "
                                "the value of the parameter should be None or"
                                "True or False.")
            if max_state_size is None:
                max_state_size = JMemoryStateBackend.DEFAULT_MAX_STATE_SIZE
            j_memory_state_backend = JMemoryStateBackend(checkpoint_path,
                                                         savepoint_path,
                                                         max_state_size,
                                                         j_asynchronous_snapshots)

        self._j_memory_state_backend = j_memory_state_backend
        super(MemoryStateBackend, self).__init__(j_memory_state_backend)

    def get_max_state_size(self) -> int:
        """
        Gets the maximum size that an individual state can have, as configured in the
        constructor (by default :data:`DEFAULT_MAX_STATE_SIZE`).

        :return: The maximum size that an individual state can have.
        """
        return self._j_memory_state_backend.getMaxStateSize()

    def is_using_asynchronous_snapshots(self) -> bool:
        """
        Gets whether the key/value data structures are asynchronously snapshotted.

        If not explicitly configured, this is the default value of
        ``org.apache.flink.configuration.CheckpointingOptions.ASYNC_SNAPSHOTS``.

        :return: True if the key/value data structures are asynchronously snapshotted,
                 false otherwise.
        """
        return self._j_memory_state_backend.isUsingAsynchronousSnapshots()

    def __str__(self):
        return self._j_memory_state_backend.toString()


class FsStateBackend(StateBackend):
    """
    **IMPORTANT** `FsStateBackend is deprecated in favor of `HashMapStateBackend` and
    `FileSystemCheckpointStorage`. This change does not affect the runtime characteristics
    of your Jobs and is simply an API change to help better communicate the ways Flink separates
    local state storage from fault tolerance. Jobs can be upgraded without loss of state. If
    configuring your state backend via the `StreamExecutionEnvironment` please make the following
    changes.

    ::

        >> env.set_state_backend(HashMapStateBackend())
        >> env.get_checkpoint_config().set_checkpoint_storage("hdfs://checkpoints")

    If you are configuring your state backend via the `flink-conf.yaml` please set your state
    backend type to `hashmap`.

    This state backend holds the working state in the memory (JVM heap) of the TaskManagers.
    The state backend checkpoints state as files to a file system (hence the backend's name).

    Each checkpoint individually will store all its files in a subdirectory that includes the
    checkpoint number, such as ``hdfs://namenode:port/flink-checkpoints/chk-17/``.

    **State Size Considerations**

    Working state is kept on the TaskManager heap. If a TaskManager executes multiple
    tasks concurrently (if the TaskManager has multiple slots, or if slot-sharing is used)
    then the aggregate state of all tasks needs to fit into that TaskManager's memory.

    This state backend stores small state chunks directly with the metadata, to avoid creating
    many small files. The threshold for that is configurable. When increasing this threshold, the
    size of the checkpoint metadata increases. The checkpoint metadata of all retained completed
    checkpoints needs to fit into the JobManager's heap memory. This is typically not a problem,
    unless the threshold :func:`get_min_file_size_threshold` is increased significantly.

    **Persistence Guarantees**

    Checkpoints from this state backend are as persistent and available as filesystem that is
    written to. If the file system is a persistent distributed file system, this state backend
    supports highly available setups. The backend additionally supports savepoints and externalized
    checkpoints.

    **Configuration**

    As for all state backends, this backend can either be configured within the application (by
    creating the backend with the respective constructor parameters and setting it on the execution
    environment) or by specifying it in the Flink configuration.

    If the state backend was specified in the application, it may pick up additional configuration
    parameters from the Flink configuration. For example, if the backend if configured in the
    application without a default savepoint directory, it will pick up a default savepoint
    directory specified in the Flink configuration of the running job/cluster. That behavior is
    implemented via the :func:`configure` method.
    """

    def __init__(self,
                 checkpoint_directory_uri=None,
                 default_savepoint_directory_uri=None,
                 file_state_size_threshold=None,
                 write_buffer_size=None,
                 using_asynchronous_snapshots=None,
                 j_fs_state_backend=None):
        """
        Creates a new state backend that stores its checkpoint data in the file system and location
        defined by the given URI.

        A file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or
        'S3://') must be accessible via ``org.apache.flink.core.fs.FileSystem.get(URI)``.

        For a state backend targeting HDFS, this means that the URI must either specify the
        authority (host and port), or that the Hadoop configuration that describes that information
        must be in the classpath.

        Example:
        ::

            >>> state_backend = FsStateBackend("file://var/checkpoints/")


        :param checkpoint_directory_uri: The path to write checkpoint metadata to, required.
        :param default_savepoint_directory_uri: The path to write savepoints to. If none, the value
                                                from the runtime configuration will be used, or
                                                savepoint target locations need to be passed when
                                                triggering a savepoint.
        :param file_state_size_threshold: State below this size will be stored as part of the
                                          metadata, rather than in files. If none, the value
                                          configured in the runtime configuration will be used, or
                                          the default value (1KB) if nothing is configured.
        :param write_buffer_size: Write buffer size used to serialize state. If -1, the value
                                  configured in the runtime configuration will be used, or the
                                  default value (4KB) if nothing is configured.
        :param using_asynchronous_snapshots: Snapshots are now always asynchronous. This flag
                                             has no effect anymore in this version.
        :param j_fs_state_backend: For internal use, please keep none.
        """
        if j_fs_state_backend is None:
            gateway = get_gateway()
            JTernaryBoolean = gateway.jvm.org.apache.flink.util.TernaryBoolean
            JFsStateBackend = gateway.jvm.org.apache.flink.runtime.state.filesystem\
                .FsStateBackend
            JPath = gateway.jvm.org.apache.flink.core.fs.Path
            if checkpoint_directory_uri is None:
                raise ValueError("The parameter 'checkpoint_directory_uri' is required!")
            j_checkpoint_directory_uri = JPath(checkpoint_directory_uri).toUri()

            if default_savepoint_directory_uri is None:
                j_default_savepoint_directory_uri = None
            else:
                j_default_savepoint_directory_uri = JPath(default_savepoint_directory_uri).toUri()

            if file_state_size_threshold is None:
                file_state_size_threshold = -1

            if write_buffer_size is None:
                write_buffer_size = -1

            if using_asynchronous_snapshots is None:
                j_asynchronous_snapshots = JTernaryBoolean.UNDEFINED
            elif using_asynchronous_snapshots is True:
                j_asynchronous_snapshots = JTernaryBoolean.TRUE
            elif using_asynchronous_snapshots is False:
                j_asynchronous_snapshots = JTernaryBoolean.FALSE
            else:
                raise TypeError("Unsupported input for 'using_asynchronous_snapshots': %s, "
                                "the value of the parameter should be None or"
                                "True or False.")

            j_fs_state_backend = JFsStateBackend(j_checkpoint_directory_uri,
                                                 j_default_savepoint_directory_uri,
                                                 file_state_size_threshold,
                                                 write_buffer_size,
                                                 j_asynchronous_snapshots)

        self._j_fs_state_backend = j_fs_state_backend
        super(FsStateBackend, self).__init__(j_fs_state_backend)

    def get_checkpoint_path(self) -> str:
        """
        Gets the base directory where all the checkpoints are stored.
        The job-specific checkpoint directory is created inside this directory.

        :return: The base directory for checkpoints.
        """
        return self._j_fs_state_backend.getCheckpointPath().toString()

    def get_min_file_size_threshold(self) -> int:
        """
        Gets the threshold below which state is stored as part of the metadata, rather than in
        files. This threshold ensures that the backend does not create a large amount of very
        small files, where potentially the file pointers are larger than the state itself.

        If not explicitly configured, this is the default value of
        ``org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD``.

        :return: The file size threshold, in bytes.
        """
        return self._j_fs_state_backend.getMinFileSizeThreshold()

    def is_using_asynchronous_snapshots(self) -> bool:
        """
        Gets whether the key/value data structures are asynchronously snapshotted.

        If not explicitly configured, this is the default value of
        ``org.apache.flink.configuration.CheckpointingOptions.ASYNC_SNAPSHOTS``.

        :return: True if the key/value data structures are asynchronously snapshotted,
                 false otherwise.
        """
        return self._j_fs_state_backend.isUsingAsynchronousSnapshots()

    def get_write_buffer_size(self) -> int:
        """
        Gets the write buffer size for created checkpoint stream.

        If not explicitly configured, this is the default value of
        ``org.apache.flink.configuration.CheckpointingOptions.FS_WRITE_BUFFER_SIZE``.

        :return: The write buffer size, in bytes.
        """
        return self._j_fs_state_backend.getWriteBufferSize()


class RocksDBStateBackend(StateBackend):
    """
    **IMPORTANT** `RocksDBStateBackend` is deprecated in favor of `EmbeddedRocksDBStateBackend`
    and `FileSystemCheckpointStorage`. This change does not affect the runtime characteristics of
    your Jobs and is simply an API change to help better communicate the ways Flink separates
    local state storage from fault tolerance. Jobs can be upgraded without loss of state. If
    configuring your state backend via the `StreamExecutionEnvironment` please make the following
    changes.

    ::

        >> env.set_state_backend(EmbeddedRocksDBStateBackend())
        >> env.get_checkpoint_config().set_checkpoint_storage("hdfs://checkpoints")

    If you are configuring your state backend via the `flink-conf.yaml` no changes are required.

    A State Backend that stores its state in ``RocksDB``. This state backend can
    store very large state that exceeds memory and spills to disk.

    All key/value state (including windows) is stored in the key/value index of RocksDB.
    For persistence against loss of machines, checkpoints take a snapshot of the
    RocksDB database, and persist that snapshot in a file system (by default) or
    another configurable state backend.

    The behavior of the RocksDB instances can be parametrized by setting RocksDB Options
    using the methods :func:`set_predefined_options` and :func:`set_options`.
    """

    def __init__(self,
                 checkpoint_data_uri=None,
                 enable_incremental_checkpointing=None,
                 checkpoint_stream_backend=None,
                 j_rocks_db_state_backend=None):
        """
        Creates a new :class:`RocksDBStateBackend` that stores its checkpoint data in the given
        state backend or the location of given URI.

        If using state backend, typically, one would supply a filesystem or database state backend
        here where the snapshots from RocksDB would be stored.

        If using URI, a state backend that stores checkpoints in HDFS or S3 must specify the file
        system host and port in the URI, or have the Hadoop configuration that describes the file
        system (host / high-availability group / possibly credentials) either referenced from the
        Flink config, or included in the classpath.

        Example:
        ::

            >>> state_backend = RocksDBStateBackend("file://var/checkpoints/")

        :param checkpoint_data_uri: The URI describing the filesystem and path to the checkpoint
                                    data directory.
        :param enable_incremental_checkpointing: True if incremental checkpointing is enabled.
        :param checkpoint_stream_backend: The backend write the checkpoint streams to.
        :param j_rocks_db_state_backend: For internal use, please keep none.
        """
        if j_rocks_db_state_backend is None:
            gateway = get_gateway()
            JTernaryBoolean = gateway.jvm.org.apache.flink.util.TernaryBoolean
            JRocksDBStateBackend = gateway.jvm.org.apache.flink.contrib.streaming.state \
                .RocksDBStateBackend

            if enable_incremental_checkpointing not in (None, True, False):
                raise TypeError("Unsupported input for 'enable_incremental_checkpointing': %s, "
                                "the value of the parameter should be None or"
                                "True or False.")

            if checkpoint_data_uri is not None:
                if enable_incremental_checkpointing is None:
                    j_rocks_db_state_backend = JRocksDBStateBackend(checkpoint_data_uri)
                else:
                    j_rocks_db_state_backend = \
                        JRocksDBStateBackend(checkpoint_data_uri, enable_incremental_checkpointing)
            elif isinstance(checkpoint_stream_backend, StateBackend):
                if enable_incremental_checkpointing is None:
                    j_enable_incremental_checkpointing = JTernaryBoolean.UNDEFINED
                elif enable_incremental_checkpointing is True:
                    j_enable_incremental_checkpointing = JTernaryBoolean.TRUE
                else:
                    j_enable_incremental_checkpointing = JTernaryBoolean.FALSE

                j_rocks_db_state_backend = \
                    JRocksDBStateBackend(checkpoint_stream_backend._j_state_backend,
                                         j_enable_incremental_checkpointing)

        self._j_rocks_db_state_backend = j_rocks_db_state_backend
        super(RocksDBStateBackend, self).__init__(j_rocks_db_state_backend)

    def get_checkpoint_backend(self):
        """
        Gets the state backend that this RocksDB state backend uses to persist
        its bytes to.

        This RocksDB state backend only implements the RocksDB specific parts, it
        relies on the 'CheckpointBackend' to persist the checkpoint and savepoint bytes
        streams.

        :return: The state backend to persist the checkpoint and savepoint bytes streams.
        """
        j_state_backend = self._j_rocks_db_state_backend.getCheckpointBackend()
        return _from_j_state_backend(j_state_backend)

    def set_db_storage_paths(self, *paths: str):
        """
        Sets the directories in which the local RocksDB database puts its files (like SST and
        metadata files). These directories do not need to be persistent, they can be ephemeral,
        meaning that they are lost on a machine failure, because state in RocksDB is persisted
        in checkpoints.

        If nothing is configured, these directories default to the TaskManager's local
        temporary file directories.

        Each distinct state will be stored in one path, but when the state backend creates
        multiple states, they will store their files on different paths.

        Passing ``None`` to this function restores the default behavior, where the configured
        temp directories will be used.

        :param paths: The paths across which the local RocksDB database files will be spread. this
                      parameter is optional.
        """
        if len(paths) < 1:
            self._j_rocks_db_state_backend.setDbStoragePath(None)
        else:
            gateway = get_gateway()
            j_path_array = gateway.new_array(gateway.jvm.String, len(paths))
            for i in range(0, len(paths)):
                j_path_array[i] = paths[i]
            self._j_rocks_db_state_backend.setDbStoragePaths(j_path_array)

    def get_db_storage_paths(self) -> List[str]:
        """
        Gets the configured local DB storage paths, or null, if none were configured.

        Under these directories on the TaskManager, RocksDB stores its SST files and
        metadata files. These directories do not need to be persistent, they can be ephermeral,
        meaning that they are lost on a machine failure, because state in RocksDB is persisted
        in checkpoints.

        If nothing is configured, these directories default to the TaskManager's local
        temporary file directories.

        :return: The list of configured local DB storage paths.
        """
        return list(self._j_rocks_db_state_backend.getDbStoragePaths())

    def is_incremental_checkpoints_enabled(self) -> bool:
        """
        Gets whether incremental checkpoints are enabled for this state backend.

        :return: True if incremental checkpoints are enabled, false otherwise.
        """
        return self._j_rocks_db_state_backend.isIncrementalCheckpointsEnabled()

    def set_predefined_options(self, options: 'PredefinedOptions'):
        """
        Sets the predefined options for RocksDB.

        If user-configured options within ``RocksDBConfigurableOptions`` is set (through
        flink-conf.yaml) or a user-defined options factory is set (via :func:`setOptions`),
        then the options from the factory are applied on top of the here specified
        predefined options and customized options.

        Example:
        ::

            >>> state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED)

        :param options: The options to set (must not be null), see :class:`PredefinedOptions`.
        """
        self._j_rocks_db_state_backend.setPredefinedOptions(options._to_j_predefined_options())

    def get_predefined_options(self) -> 'PredefinedOptions':
        """
        Gets the current predefined options for RocksDB.
        The default options (if nothing was set via :func:`setPredefinedOptions`)
        are :data:`PredefinedOptions.DEFAULT`.

        If user-configured options within ``RocksDBConfigurableOptions`` is set (through
        flink-conf.yaml) or a user-defined options factory is set (via :func:`setOptions`),
        then the options from the factory are applied on top of the predefined and customized
        options.

        .. seealso:: :func:`set_predefined_options`

        :return: Current predefined options.
        """
        j_predefined_options = self._j_rocks_db_state_backend.getPredefinedOptions()
        return PredefinedOptions._from_j_predefined_options(j_predefined_options)

    def set_options(self, options_factory_class_name: str):
        """
        Sets ``org.rocksdb.Options`` for the RocksDB instances.
        Because the options are not serializable and hold native code references,
        they must be specified through a factory.

        The options created by the factory here are applied on top of the pre-defined
        options profile selected via :func:`set_predefined_options`.
        If the pre-defined options profile is the default (:data:`PredefinedOptions.DEFAULT`),
        then the factory fully controls the RocksDB options.

        :param options_factory_class_name: The fully-qualified class name of the options
                                           factory in Java that lazily creates the RocksDB options.
                                           The options factory must have a default constructor.
        """
        gateway = get_gateway()
        JOptionsFactory = gateway.jvm.org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory
        j_options_factory_clz = load_java_class(options_factory_class_name)
        if not get_java_class(JOptionsFactory).isAssignableFrom(j_options_factory_clz):
            raise ValueError("The input class does not implement RocksDBOptionsFactory.")
        self._j_rocks_db_state_backend.setRocksDBOptions(j_options_factory_clz.newInstance())

    def get_options(self) -> Optional[str]:
        """
        Gets the fully-qualified class name of the options factory in Java that lazily creates
        the RocksDB options.

        :return: The fully-qualified class name of the options factory in Java.
        """
        j_options_factory = self._j_rocks_db_state_backend.getRocksDBOptions()
        if j_options_factory is not None:
            return j_options_factory.getClass().getName()
        else:
            return None

    def get_number_of_transfering_threads(self) -> int:
        """
        Gets the number of threads used to transfer files while snapshotting/restoring.

        :return: The number of threads used to transfer files while snapshotting/restoring.
        """
        return self._j_rocks_db_state_backend.getNumberOfTransferingThreads()

    def set_number_of_transfering_threads(self, number_of_transfering_threads: int):
        """
        Sets the number of threads used to transfer files while snapshotting/restoring.

        :param number_of_transfering_threads: The number of threads used to transfer files while
                                              snapshotting/restoring.
        """
        self._j_rocks_db_state_backend.setNumberOfTransferingThreads(number_of_transfering_threads)

    def __str__(self):
        return self._j_rocks_db_state_backend.toString()


class PredefinedOptions(Enum):
    """
    The :class:`PredefinedOptions` are configuration settings for the :class:`RocksDBStateBackend`.
    The various pre-defined choices are configurations that have been empirically
    determined to be beneficial for performance under different settings.

    Some of these settings are based on experiments by the Flink community, some follow
    guides from the RocksDB project.

    :data:`DEFAULT`:

    Default options for all settings, except that writes are not forced to the
    disk.

    .. note::
        Because Flink does not rely on RocksDB data on disk for recovery,
        there is no need to sync data to stable storage.

    :data:`SPINNING_DISK_OPTIMIZED`:

    Pre-defined options for regular spinning hard disks.

    This constant configures RocksDB with some options that lead empirically
    to better performance when the machines executing the system use
    regular spinning hard disks.

    The following options are set:

    - setCompactionStyle(CompactionStyle.LEVEL)
    - setLevelCompactionDynamicLevelBytes(true)
    - setIncreaseParallelism(4)
    - setUseFsync(false)
    - setDisableDataSync(true)
    - setMaxOpenFiles(-1)

    .. note::
        Because Flink does not rely on RocksDB data on disk for recovery,
        there is no need to sync data to stable storage.

    :data:`SPINNING_DISK_OPTIMIZED_HIGH_MEM`:

    Pre-defined options for better performance on regular spinning hard disks,
    at the cost of a higher memory consumption.

    .. note::
        These settings will cause RocksDB to consume a lot of memory for
        block caching and compactions. If you experience out-of-memory problems related to,
        RocksDB, consider switching back to :data:`SPINNING_DISK_OPTIMIZED`.

    The following options are set:

    - setLevelCompactionDynamicLevelBytes(true)
    - setTargetFileSizeBase(256 MBytes)
    - setMaxBytesForLevelBase(1 GByte)
    - setWriteBufferSize(64 MBytes)
    - setIncreaseParallelism(4)
    - setMinWriteBufferNumberToMerge(3)
    - setMaxWriteBufferNumber(4)
    - setUseFsync(false)
    - setMaxOpenFiles(-1)
    - BlockBasedTableConfig.setBlockCacheSize(256 MBytes)
    - BlockBasedTableConfigsetBlockSize(128 KBytes)

    .. note::
        Because Flink does not rely on RocksDB data on disk for recovery,
        there is no need to sync data to stable storage.

    :data:`FLASH_SSD_OPTIMIZED`:

    Pre-defined options for Flash SSDs.

    This constant configures RocksDB with some options that lead empirically
    to better performance when the machines executing the system use SSDs.

    The following options are set:

    - setIncreaseParallelism(4)
    - setUseFsync(false)
    - setDisableDataSync(true)
    - setMaxOpenFiles(-1)

    .. note::
        Because Flink does not rely on RocksDB data on disk for recovery,
        there is no need to sync data to stable storage.
    """
    DEFAULT = 0
    SPINNING_DISK_OPTIMIZED = 1
    SPINNING_DISK_OPTIMIZED_HIGH_MEM = 2
    FLASH_SSD_OPTIMIZED = 3

    @staticmethod
    def _from_j_predefined_options(j_predefined_options) -> 'PredefinedOptions':
        return PredefinedOptions[j_predefined_options.name()]

    def _to_j_predefined_options(self):
        gateway = get_gateway()
        JPredefinedOptions = gateway.jvm.org.apache.flink.contrib.streaming.state.PredefinedOptions
        return getattr(JPredefinedOptions, self.name)


class CustomStateBackend(StateBackend):
    """
    A wrapper of customized java state backend.
    """

    def __init__(self, j_custom_state_backend):
        super(CustomStateBackend, self).__init__(j_custom_state_backend)
