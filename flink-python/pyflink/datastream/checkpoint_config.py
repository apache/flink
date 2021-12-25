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
from typing import Optional

from pyflink.common import Duration
from pyflink.datastream.checkpoint_storage import CheckpointStorage, _from_j_checkpoint_storage
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.java_gateway import get_gateway

__all__ = ['CheckpointConfig', 'ExternalizedCheckpointCleanup']


class CheckpointConfig(object):
    """
    Configuration that captures all checkpointing related settings.

    :data:`DEFAULT_MODE`:

    The default checkpoint mode: exactly once.

    :data:`DEFAULT_TIMEOUT`:

    The default timeout of a checkpoint attempt: 10 minutes.

    :data:`DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS`:

    The default minimum pause to be made between checkpoints: none.

    :data:`DEFAULT_MAX_CONCURRENT_CHECKPOINTS`:

    The default limit of concurrently happening checkpoints: one.
    """

    DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE

    DEFAULT_TIMEOUT = 10 * 60 * 1000

    DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0

    DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1

    def __init__(self, j_checkpoint_config):
        self._j_checkpoint_config = j_checkpoint_config

    def is_checkpointing_enabled(self) -> bool:
        """
        Checks whether checkpointing is enabled.

        :return: True if checkpointing is enables, false otherwise.
        """
        return self._j_checkpoint_config.isCheckpointingEnabled()

    def get_checkpointing_mode(self) -> CheckpointingMode:
        """
        Gets the checkpointing mode (exactly-once vs. at-least-once).

        .. seealso:: :func:`set_checkpointing_mode`

        :return: The :class:`CheckpointingMode`.
        """
        return CheckpointingMode._from_j_checkpointing_mode(
            self._j_checkpoint_config.getCheckpointingMode())

    def set_checkpointing_mode(self, checkpointing_mode: CheckpointingMode) -> 'CheckpointConfig':
        """
        Sets the checkpointing mode (:data:`CheckpointingMode.EXACTLY_ONCE` vs.
        :data:`CheckpointingMode.AT_LEAST_ONCE`).

        Example:
        ::

            >>> config.set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)

        :param checkpointing_mode: The :class:`CheckpointingMode`.
        """
        self._j_checkpoint_config.setCheckpointingMode(
            CheckpointingMode._to_j_checkpointing_mode(checkpointing_mode))
        return self

    def get_checkpoint_interval(self) -> int:
        """
        Gets the interval in which checkpoints are periodically scheduled.

        This setting defines the base interval. Checkpoint triggering may be delayed by the settings
        :func:`get_max_concurrent_checkpoints` and :func:`get_min_pause_between_checkpoints`.

        :return: The checkpoint interval, in milliseconds.
        """
        return self._j_checkpoint_config.getCheckpointInterval()

    def set_checkpoint_interval(self, checkpoint_interval: int) -> 'CheckpointConfig':
        """
        Sets the interval in which checkpoints are periodically scheduled.

        This setting defines the base interval. Checkpoint triggering may be delayed by the settings
        :func:`set_max_concurrent_checkpoints` and :func:`set_min_pause_between_checkpoints`.

        :param checkpoint_interval: The checkpoint interval, in milliseconds.
        """
        self._j_checkpoint_config.setCheckpointInterval(checkpoint_interval)
        return self

    def get_checkpoint_timeout(self) -> int:
        """
        Gets the maximum time that a checkpoint may take before being discarded.

        :return: The checkpoint timeout, in milliseconds.
        """
        return self._j_checkpoint_config.getCheckpointTimeout()

    def set_checkpoint_timeout(self, checkpoint_timeout: int) -> 'CheckpointConfig':
        """
        Sets the maximum time that a checkpoint may take before being discarded.

        :param checkpoint_timeout: The checkpoint timeout, in milliseconds.
        """
        self._j_checkpoint_config.setCheckpointTimeout(checkpoint_timeout)
        return self

    def get_min_pause_between_checkpoints(self) -> int:
        """
        Gets the minimal pause between checkpointing attempts. This setting defines how soon the
        checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
        another checkpoint with respect to the maximum number of concurrent checkpoints
        (see :func:`get_max_concurrent_checkpoints`).

        :return: The minimal pause before the next checkpoint is triggered.
        """
        return self._j_checkpoint_config.getMinPauseBetweenCheckpoints()

    def set_min_pause_between_checkpoints(self,
                                          min_pause_between_checkpoints: int) -> 'CheckpointConfig':
        """
        Sets the minimal pause between checkpointing attempts. This setting defines how soon the
        checkpoint coordinator may trigger another checkpoint after it becomes possible to trigger
        another checkpoint with respect to the maximum number of concurrent checkpoints
        (see :func:`set_max_concurrent_checkpoints`).

        If the maximum number of concurrent checkpoints is set to one, this setting makes
        effectively sure that a minimum amount of time passes where no checkpoint is in progress
        at all.

        :param min_pause_between_checkpoints: The minimal pause before the next checkpoint is
                                              triggered.
        """
        self._j_checkpoint_config.setMinPauseBetweenCheckpoints(min_pause_between_checkpoints)
        return self

    def get_max_concurrent_checkpoints(self) -> int:
        """
        Gets the maximum number of checkpoint attempts that may be in progress at the same time.
        If this value is *n*, then no checkpoints will be triggered while *n* checkpoint attempts
        are currently in flight. For the next checkpoint to be triggered, one checkpoint attempt
        would need to finish or expire.

        :return: The maximum number of concurrent checkpoint attempts.
        """
        return self._j_checkpoint_config.getMaxConcurrentCheckpoints()

    def set_max_concurrent_checkpoints(self, max_concurrent_checkpoints: int) -> 'CheckpointConfig':
        """
        Sets the maximum number of checkpoint attempts that may be in progress at the same time.
        If this value is *n*, then no checkpoints will be triggered while *n* checkpoint attempts
        are currently in flight. For the next checkpoint to be triggered, one checkpoint attempt
        would need to finish or expire.

        :param max_concurrent_checkpoints: The maximum number of concurrent checkpoint attempts.
        """
        self._j_checkpoint_config.setMaxConcurrentCheckpoints(max_concurrent_checkpoints)
        return self

    def is_fail_on_checkpointing_errors(self) -> bool:
        """
        This determines the behaviour of tasks if there is an error in their local checkpointing.
        If this returns true, tasks will fail as a reaction. If this returns false, task will only
        decline the failed checkpoint.

        :return: ``True`` if failing on checkpointing errors, false otherwise.
        """
        return self._j_checkpoint_config.isFailOnCheckpointingErrors()

    def set_fail_on_checkpointing_errors(self,
                                         fail_on_checkpointing_errors: bool) -> 'CheckpointConfig':
        """
        Sets the expected behaviour for tasks in case that they encounter an error in their
        checkpointing procedure. If this is set to true, the task will fail on checkpointing error.
        If this is set to false, the task will only decline a the checkpoint and continue running.
        The default is true.

        Example:
        ::

            >>> config.set_fail_on_checkpointing_errors(False)

        :param fail_on_checkpointing_errors: ``True`` if failing on checkpointing errors,
                                             false otherwise.
        """
        self._j_checkpoint_config.setFailOnCheckpointingErrors(fail_on_checkpointing_errors)
        return self

    def get_tolerable_checkpoint_failure_number(self) -> int:
        """
        Get the defined number of consecutive checkpoint failures that will be tolerated, before the
        whole job is failed over.

        :return: The maximum number of tolerated checkpoint failures.
        """
        return self._j_checkpoint_config.getTolerableCheckpointFailureNumber()

    def set_tolerable_checkpoint_failure_number(self,
                                                tolerable_checkpoint_failure_number: int
                                                ) -> 'CheckpointConfig':
        """
        This defines how many consecutive checkpoint failures will be tolerated, before the whole
        job is failed over. The default value is `0`, which means no checkpoint failures will be
        tolerated, and the job will fail on first reported checkpoint failure.

        Example:
        ::

            >>> config.set_tolerable_checkpoint_failure_number(2)

        :param tolerable_checkpoint_failure_number: The maximum number of tolerated checkpoint
                                                    failures.
        """
        self._j_checkpoint_config.setTolerableCheckpointFailureNumber(
            tolerable_checkpoint_failure_number)
        return self

    def enable_externalized_checkpoints(
            self,
            cleanup_mode: 'ExternalizedCheckpointCleanup') -> 'CheckpointConfig':
        """
        Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
        automatically unless the mode is set to
        :data:`ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS`.

        Externalized checkpoints write their meta data out to persistent storage and are **not**
        automatically cleaned up when the owning job fails or is suspended (terminating with job
        status ``FAILED`` or ``SUSPENDED``). In this case, you have to manually clean up the
        checkpoint state, both the meta data and actual program state.

        The :class:`ExternalizedCheckpointCleanup` mode defines how an externalized checkpoint
        should be cleaned up on job cancellation. If you choose to retain externalized checkpoints
        on cancellation you have to handle checkpoint clean-up manually when you cancel the job as
        well (terminating with job status ``CANCELED``).

        The target directory for externalized checkpoints is configured via
        ``org.apache.flink.configuration.CheckpointingOptions#CHECKPOINTS_DIRECTORY``.

        Example:
        ::

            >>> config.enable_externalized_checkpoints(
            ...     ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        :param cleanup_mode: Externalized checkpoint clean-up behaviour, the mode could be
                             :data:`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`,
                             :data:`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION` or
                             :data:`ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS`

        .. note:: Deprecated in 1.15. Use :func:`set_externalized_checkpoint_cleanup` instead.
        """
        self._j_checkpoint_config.enableExternalizedCheckpoints(
            ExternalizedCheckpointCleanup._to_j_externalized_checkpoint_cleanup(cleanup_mode))
        return self

    def set_externalized_checkpoint_cleanup(
            self,
            cleanup_mode: 'ExternalizedCheckpointCleanup') -> 'CheckpointConfig':
        """
        Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
        automatically unless the mode is set to
        :data:`ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS`.

        Externalized checkpoints write their meta data out to persistent storage and are **not**
        automatically cleaned up when the owning job fails or is suspended (terminating with job
        status ``FAILED`` or ``SUSPENDED``). In this case, you have to manually clean up the
        checkpoint state, both the meta data and actual program state.

        The :class:`ExternalizedCheckpointCleanup` mode defines how an externalized checkpoint
        should be cleaned up on job cancellation. If you choose to retain externalized checkpoints
        on cancellation you have to handle checkpoint clean-up manually when you cancel the job as
        well (terminating with job status ``CANCELED``).

        The target directory for externalized checkpoints is configured via
        ``org.apache.flink.configuration.CheckpointingOptions#CHECKPOINTS_DIRECTORY``.

        Example:
        ::

            >>> config.set_externalized_checkpoint_cleanup(
            ...     ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        :param cleanup_mode: Externalized checkpoint clean-up behaviour, the mode could be
                             :data:`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`,
                             :data:`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION` or
                             :data:`ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS`
        """
        self._j_checkpoint_config.setExternalizedCheckpointCleanup(
            ExternalizedCheckpointCleanup._to_j_externalized_checkpoint_cleanup(cleanup_mode))
        return self

    def is_externalized_checkpoints_enabled(self) -> bool:
        """
        Returns whether checkpoints should be persisted externally.

        :return: ``True`` if checkpoints should be externalized, false otherwise.
        """
        return self._j_checkpoint_config.isExternalizedCheckpointsEnabled()

    def get_externalized_checkpoint_cleanup(self) -> Optional['ExternalizedCheckpointCleanup']:
        """
        Returns the cleanup behaviour for externalized checkpoints.

        :return: The cleanup behaviour for externalized checkpoints or ``None`` if none is
                 configured.
        """
        cleanup_mode = self._j_checkpoint_config.getExternalizedCheckpointCleanup()
        if cleanup_mode is None:
            return None
        else:
            return ExternalizedCheckpointCleanup._from_j_externalized_checkpoint_cleanup(
                cleanup_mode)

    def is_unaligned_checkpoints_enabled(self) -> bool:
        """
        Returns whether unaligned checkpoints are enabled.

        :return: ``True`` if unaligned checkpoints are enabled.
        """
        return self._j_checkpoint_config.isUnalignedCheckpointsEnabled()

    def enable_unaligned_checkpoints(self, enabled: bool = True) -> 'CheckpointConfig':
        """
        Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure.

        Unaligned checkpoints contain data stored in buffers as part of the checkpoint state, which
        allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes
        independent of the current throughput as checkpoint barriers are effectively not embedded
        into the stream of data anymore.

        Unaligned checkpoints can only be enabled if :func:`get_checkpointing_mode` is
        :data:`CheckpointingMode.EXACTLY_ONCE`.

        :param enabled: ``True`` if a checkpoints should be taken in unaligned mode.
        """
        self._j_checkpoint_config.enableUnalignedCheckpoints(enabled)
        return self

    def disable_unaligned_checkpoints(self) -> 'CheckpointConfig':
        """
        Enables unaligned checkpoints, which greatly reduce checkpointing times under backpressure
        (experimental).

        Unaligned checkpoints contain data stored in buffers as part of the checkpoint state, which
        allows checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes
        independent of the current throughput as checkpoint barriers are effectively not embedded
        into the stream of data anymore.

        Unaligned checkpoints can only be enabled if :func:`get_checkpointing_mode` is
        :data:`CheckpointingMode.EXACTLY_ONCE`.
        """
        self.enable_unaligned_checkpoints(False)
        return self

    def set_alignment_timeout(self, alignment_timeout: Duration) -> 'CheckpointConfig':
        """
        Only relevant if :func:`enable_unaligned_checkpoints` is enabled.

        If ``alignment_timeout`` has value equal to ``0``, checkpoints will always start unaligned.
        If ``alignment_timeout`` has value greater then ``0``, checkpoints will start aligned. If
        during checkpointing, checkpoint start delay exceeds this ``alignment_timeout``, alignment
        will timeout and checkpoint will start working as unaligned checkpoint.

        :param alignment_timeout: The duration until the aligned checkpoint will be converted into
                                  an unaligned checkpoint.
        """
        self._j_checkpoint_config.setAlignmentTimeout(alignment_timeout._j_duration)
        return self

    def get_alignment_timeout(self) -> 'Duration':
        """
        Returns the alignment timeout, as configured via :func:`set_alignment_timeout` or
        ``org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions#ALIGNMENT_TIMEOUT``.

        :return: the alignment timeout.
        """
        return Duration(self._j_checkpoint_config.getAlignmentTimeout())

    def set_force_unaligned_checkpoints(
            self,
            force_unaligned_checkpoints: bool = True) -> 'CheckpointConfig':
        """
        Checks whether unaligned checkpoints are forced, despite currently non-checkpointable
        iteration feedback or custom partitioners.

        :param force_unaligned_checkpoints: The flag to force unaligned checkpoints.
        """
        self._j_checkpoint_config.setForceUnalignedCheckpoints(force_unaligned_checkpoints)
        return self

    def is_force_unaligned_checkpoints(self) -> 'bool':
        """
        Checks whether unaligned checkpoints are forced, despite iteration feedback or custom
        partitioners.

        :return: True, if unaligned checkpoints are forced, false otherwise.
        """
        return self._j_checkpoint_config.isForceUnalignedCheckpoints()

    def set_checkpoint_storage(self, storage: CheckpointStorage) -> 'CheckpointConfig':
        """
        Checkpoint storage defines how stat backends checkpoint their state for fault
        tolerance in streaming applications. Various implementations store their checkpoints
        in different fashions and have different requirements and availability guarantees.

        For example, `JobManagerCheckpointStorage` stores checkpoints in the memory of the
        JobManager. It is lightweight and without additional dependencies but is not highly
        available and only supports small state sizes. This checkpoint storage policy is convenient
        for local testing and development.

        The `FileSystemCheckpointStorage` stores checkpoints in a filesystem. For systems like
        HDFS, NFS Drivs, S3, and GCS, this storage policy supports large state size, in the
        magnitude of many terabytes while providing a highly available foundation for stateful
        applications. This checkpoint storage policy is recommended for most production deployments.
        """
        self._j_checkpoint_config.setCheckpointStorage(storage._j_checkpoint_storage)
        return self

    def set_checkpoint_storage_dir(self, checkpoint_path: str) -> 'CheckpointConfig':
        """
        Configures the application to write out checkpoint snapshots to the configured directory.
        See `FileSystemCheckpointStorage` for more details on checkpointing to a file system.
        """
        self._j_checkpoint_config.setCheckpointStorage(checkpoint_path)
        return self

    def get_checkpoint_storage(self) -> Optional[CheckpointStorage]:
        """
        The checkpoint storage that has been configured for the Job, or None if
        none has been set.
        """
        j_storage = self._j_checkpoint_config.getCheckpointStorage()
        if j_storage is None:
            return None
        else:
            return _from_j_checkpoint_storage(j_storage)


class ExternalizedCheckpointCleanup(Enum):
    """
    Cleanup behaviour for externalized checkpoints when the job is cancelled.

    :data:`DELETE_ON_CANCELLATION`:

    Delete externalized checkpoints on job cancellation.

    All checkpoint state will be deleted when you cancel the owning
    job, both the meta data and actual program state. Therefore, you
    cannot resume from externalized checkpoints after the job has been
    cancelled.

    Note that checkpoint state is always kept if the job terminates
    with state ``FAILED``.

    :data:`RETAIN_ON_CANCELLATION`:

    Retain externalized checkpoints on job cancellation.

    All checkpoint state is kept when you cancel the owning job. You
    have to manually delete both the checkpoint meta data and actual
    program state after cancelling the job.

    Note that checkpoint state is always kept if the job terminates
    with state ``FAILED``.

    :data:`NO_EXTERNALIZED_CHECKPOINTS`:

    Externalized checkpoints are disabled completely.
    """

    DELETE_ON_CANCELLATION = 0

    RETAIN_ON_CANCELLATION = 1

    NO_EXTERNALIZED_CHECKPOINTS = 2

    @staticmethod
    def _from_j_externalized_checkpoint_cleanup(j_cleanup_mode) \
            -> 'ExternalizedCheckpointCleanup':
        return ExternalizedCheckpointCleanup[j_cleanup_mode.name()]

    def _to_j_externalized_checkpoint_cleanup(self):
        gateway = get_gateway()
        JExternalizedCheckpointCleanup = \
            gateway.jvm.org.apache.flink.streaming.api.environment.CheckpointConfig \
            .ExternalizedCheckpointCleanup
        return getattr(JExternalizedCheckpointCleanup, self.name)
