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

from py4j.java_gateway import get_java_class
from typing import Optional

from pyflink.java_gateway import get_gateway

__all__ = [
    'CheckpointStorage',
    'JobManagerCheckpointStorage',
    'FileSystemCheckpointStorage',
    'CustomCheckpointStorage']


def _from_j_checkpoint_storage(j_checkpoint_storage):
    if j_checkpoint_storage is None:
        return None
    gateway = get_gateway()
    JCheckpointStorage = gateway.jvm.org.apache.flink.runtime.state.CheckpointStorage
    JJobManagerCheckpointStorage = gateway.jvm.org.apache.flink.runtime.state.storage \
        .JobManagerCheckpointStorage
    JFileSystemCheckpointStorage = gateway.jvm.org.apache.flink.runtime.state.storage \
        .FileSystemCheckpointStorage

    j_clz = j_checkpoint_storage.getClass()

    if not get_java_class(JCheckpointStorage).isAssignableFrom(j_clz):
        raise TypeError("%s is not an instance of CheckpointStorage." % j_checkpoint_storage)

    if get_java_class(JJobManagerCheckpointStorage).isAssignableFrom(j_clz):
        return JobManagerCheckpointStorage(j_jobmanager_checkpoint_storage=j_checkpoint_storage)
    elif get_java_class(JFileSystemCheckpointStorage).isAssignableFrom(j_clz):
        return FileSystemCheckpointStorage(j_filesystem_checkpoint_storage=j_checkpoint_storage)
    else:
        return CustomCheckpointStorage(j_checkpoint_storage)


class CheckpointStorage(object, metaclass=ABCMeta):
    """
    Checkpoint storage defines how :class:`StateBackend`'s store their state for fault-tolerance
    in streaming applications. Various implementations store their checkpoints in different fashions
    and have different requirements and availability guarantees.

    For example, :class:`JobManagerCheckpointStorage` stores checkpoints in the memory of the
    `JobManager`. It is lightweight and without additional dependencies but is not scalable
    and only supports small state sizes. This checkpoints storage policy is convenient for local
    testing and development.

    :class:`FileSystemCheckpointStorage` stores checkpoints in a filesystem. For systems like HDFS
    NFS drives, S3, and GCS, this storage policy supports large state size, in the magnitude of many
    terabytes while providing a highly available foundation for streaming applications. This
    checkpoint storage policy is recommended for most production deployments.

    **Raw Bytes Storage**

    The `CheckpointStorage` creates services for raw bytes storage.

    The raw bytes storage (through the CheckpointStreamFactory) is the fundamental service that
    simply stores bytes in a fault tolerant fashion. This service is used by the JobManager to
    store checkpoint and recovery metadata and is typically also used by the keyed- and operator-
    state backends to store checkpoint state.

    **Serializability**

    Implementations need to be serializable(`java.io.Serializable`), because they are distributed
    across parallel processes (for distributed execution) together with the streaming application
    code.

    Because of that `CheckpointStorage` implementations are meant to be like _factories_ that create
    the proper state stores that provide access to the persistent layer. That way, the storage
    policy can be very lightweight (contain only configurations) which makes it easier to be
    serializable.

    **Thread Safety**

    Checkpoint storage implementations have to be thread-safe. Multiple threads may be creating
    streams concurrently.
    """

    def __init__(self, j_checkpoint_storage):
        self._j_checkpoint_storage = j_checkpoint_storage


class JobManagerCheckpointStorage(CheckpointStorage):
    """
    The `CheckpointStorage` checkpoints state directly to the JobManager's memory (hence the
    name), but savepoints will be persisted to a file system.

    This checkpoint storage is primarily for experimentation, quick local setups, or for streaming
    applications that have very small state: Because it requires checkpoints to go through the
    JobManager's memory, larger state will occupy larger portions of the JobManager's main memory,
    reducing operational stability. For any other setup, the `FileSystemCheckpointStorage`
    should be used. The `FileSystemCheckpointStorage` but checkpoints state directly to files
    rather than to the JobManager's memory, thus supporting larger state sizes and more highly
    available recovery.

    **State Size Considerations**

    State checkpointing with this checkpoint storage is subject to the following conditions:

    - Each individual state must not exceed the configured maximum state size
      (see :func:`get_max_state_size`.

    - All state from one task (i.e., the sum of all operator states and keyed states from all
      chained operators of the task) must not exceed what the RPC system supports, which is
      be default < 10 MB. That limit can be configured up, but that is typically not advised.

    - The sum of all states in the application times all retained checkpoints must comfortably
      fit into the JobManager's JVM heap space.

    **Persistence Guarantees**

    For the use cases where the state sizes can be handled by this storage, it does
    guarantee persistence for savepoints, externalized checkpoints (of configured), and checkpoints
    (when high-availability is configured).

        **Configuration**

    As for all checkpoint storage, this type can either be configured within the application (by
    creating the storage with the respective constructor parameters and setting it on the execution
    environment) or by specifying it in the Flink configuration.

    If the storage was specified in the application, it may pick up additional configuration
    parameters from the Flink configuration. For example, if the backend if configured in the
    application without a default savepoint directory, it will pick up a default savepoint
    directory specified in the Flink configuration of the running job/cluster. That behavior is
    implemented via the :func:`configure` method.
    """

    # The default maximal size that the snapshotted memory state may have (5 MiBytes).
    DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024

    def __init__(self,
                 checkpoint_path=None,
                 max_state_size=None,
                 j_jobmanager_checkpoint_storage=None):
        """
        Creates a new JobManagerCheckpointStorage, setting optionally the paths to persist
        checkpoint metadata to, as well as configuring state thresholds.

        WARNING: Increasing the size of this value beyond the default value
        (:data:`DEFAULT_MAX_STATE_SIZE`) should be done with care.
        The checkpointed state needs to be send to the JobManager via limited size RPC messages,
        and there and the JobManager needs to be able to hold all aggregated state in its memory.

        Example:
        ::
            >>> checkpoint_storage = JobManagerCheckpointStorage()

        :param checkpoint_path: The path to write checkpoint metadata to. If none, the value from
                                the runtime configuration will be used.
        :param max_state_size: The maximal size of the serialized state. If none, the
                               :data:`DEFAULT_MAX_STATE_SIZE` will be used.
        :param j_jobmanager_checkpoint_storage: For internal use, please keep none.
        """
        if j_jobmanager_checkpoint_storage is None:
            gateway = get_gateway()
            JJobManagerCheckpointStorage = gateway.jvm.org.apache.flink.runtime.state.storage\
                .JobManagerCheckpointStorage
            JPath = gateway.jvm.org.apache.flink.core.fs.Path

            if checkpoint_path is not None:
                checkpoint_path = JPath(checkpoint_path)
            if max_state_size is None:
                max_state_size = JJobManagerCheckpointStorage.DEFAULT_MAX_STATE_SIZE
            j_jobmanager_checkpoint_storage = JJobManagerCheckpointStorage(checkpoint_path,
                                                                           max_state_size)

        super(JobManagerCheckpointStorage, self).__init__(j_jobmanager_checkpoint_storage)

    def get_checkpoint_path(self) -> Optional[str]:
        """
        Gets the base directory where all the checkpoints are stored.
        The job-specific checkpoint directory is created inside this directory.

        :return: The base directory for checkpoints.
        """
        j_path = self._j_checkpoint_storage.getCheckpointPath()
        if j_path is None:
            return None
        else:
            return j_path.toString()

    def get_max_state_size(self) -> int:
        """
        Gets the maximum size that an individual state can have, as configured in the
        constructor. By default :data:`DEFAULT_MAX_STATE_SIZE` will be used.
        """
        return self._j_checkpoint_storage.getMaxStateSize()

    def get_savepoint_path(self) -> Optional[str]:
        """
        Gets the base directory where all the savepoints are stored.
        The job-specific savepoint directory is created inside this directory.

        :return: The base directory for savepoints.
        """

        j_path = self._j_checkpoint_storage.getSavepointPath()
        if j_path is None:
            return None
        else:
            return j_path.toString()

    def __str__(self):
        return self._j_checkpoint_storage.toString()


class FileSystemCheckpointStorage(CheckpointStorage):
    """
    `FileSystemCheckpointStorage` checkpoints state as files to a filesystem.

    Each checkpoint will store all its files in a subdirectory that includes the
    checkpoints number, such as `hdfs://namenode:port/flink-checkpoints/chk-17/`.

    **State Size Considerations**

    This checkpoint storage stores small state chunks directly with the metadata, to avoid creating
    many small files. The threshold for that is configurable. When increasing this threshold, the
    size of the checkpoint metadata increases. The checkpoint metadata of all retained completed
    checkpoints needs to fit into the JobManager's heap memory. This is typically not a problem,
    unless the threashold `get_min_file_size_threshold` is increased significantly.

    **Persistence Guarantees**

    Checkpoints from this checkpoint storage are as persistent and available as the filesystem
    that it is written to. If the file system is a persistent distributed file system, this
    checkpoint storage supports highly available setups. The backend additionally supports
    savepoints and externalized checkpoints.

    **Configuration**

    As for all checkpoint storage policies, this backend can either be configured within the
    application (by creating the storage with the respective constructor parameters and setting
    it on the execution environment) or by specifying it in the Flink configuration.

    If the checkpoint storage was specified in the application, it may pick up additional
    configuration parameters from the Flink configuration. For example, if the storage is configured
    in the application without a default savepoint directory, it will pick up a default savepoint
    directory specified in the Flink configuration of the running job/cluster.
    """

    # Maximum size of state that is stored with the metadata, rather than in files (1 MiByte).
    MAX_FILE_STATE_THRESHOLD = 1024 * 1024

    def __init__(self,
                 checkpoint_path=None,
                 file_state_size_threshold=None,
                 write_buffer_size=-1,
                 j_filesystem_checkpoint_storage=None):
        """
        Creates a new FileSystemCheckpointStorage, setting the paths for the checkpoint data
        in a file system.

        All file systems for the file system scheme in the URI (e.g., `file://`, `hdfs://`, or
        `s3://`) must be accessible via `FileSystem#get`.

        For a Job targeting HDFS, this means that the URI must either specify the authority (host
        and port), of the Hadoop configuration that describes that information must be in the
        classpath.

        Example:
        ::
            >>> checkpoint_storage = FileSystemCheckpointStorage("hdfs://checkpoints")

        :param checkpoint_path: The path to write checkpoint metadata to. If none, the value from
                                the runtime configuration will be used.
        :param file_state_size_threshold: State below this size will be stored as part of the
                                        metadata, rather than in files. If -1, the value configured
                                        in the runtime configuration will be used, or the default
                                        value (1KB) if nothing is configured.
        :param write_buffer_size: Write buffer size used to serialize state. If -1, the value
                                    configured in the runtime configuration will be used, or the
                                    default value (4KB) if nothing is configured.
        :param j_filesystem_checkpoint_storage: For internal use, please keep none.
        """
        if j_filesystem_checkpoint_storage is None:
            gateway = get_gateway()
            JFileSystemCheckpointStorage = gateway.jvm.org.apache.flink.runtime.state.storage\
                .FileSystemCheckpointStorage
            JPath = gateway.jvm.org.apache.flink.core.fs.Path

            if checkpoint_path is None:
                raise ValueError("checkpoint_path must not be None")
            else:
                checkpoint_path = JPath(checkpoint_path)

            if file_state_size_threshold is None:
                file_state_size_threshold = -1

            j_filesystem_checkpoint_storage = JFileSystemCheckpointStorage(
                checkpoint_path,
                file_state_size_threshold,
                write_buffer_size)

        super(FileSystemCheckpointStorage, self).__init__(j_filesystem_checkpoint_storage)

    def get_checkpoint_path(self) -> str:
        """
        Gets the base directory where all the checkpoints are stored.
        The job-specific checkpoint directory is created inside this directory.

        :return: The base directory for checkpoints.
        """
        return self._j_checkpoint_storage.getCheckpointPath().toString()

    def get_savepoint_path(self) -> Optional[str]:
        """
        Gets the base directory where all the savepoints are stored.
        The job-specific savepoint directory is created inside this directory.

        :return: The base directory for savepoints.
        """

        j_path = self._j_checkpoint_storage.getSavepointPath()
        if j_path is None:
            return None
        else:
            return j_path.toString()

    def get_min_file_size_threshold(self) -> int:
        """
        Gets the threshold below which state is stored as part of the metadata, rather than in
        file. This threshold ensures the backend does not create a large amount of small files,
        where potentially the file pointers are larget than the state itself.
        """
        return self._j_checkpoint_storage.getMinFileSizeThreshold()

    def get_write_buffer_size(self) -> int:
        """
        Gets the write buffer size for created checkpoint streams.
        """
        return self._j_checkpoint_storage.getWriteBufferSize()

    def __str__(self):
        return self._j_checkpoint_storage.toString()


class CustomCheckpointStorage(CheckpointStorage):
    """
    A wrapper of customized java checkpoint storage.
    """

    def __init__(self, j_custom_checkpoint_storage):
        super(CustomCheckpointStorage, self).__init__(j_custom_checkpoint_storage)
