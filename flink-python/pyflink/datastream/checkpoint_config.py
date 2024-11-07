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
from typing import Optional

from pyflink.common import Duration
from pyflink.datastream.externalized_checkpoint_retention import ExternalizedCheckpointRetention
from pyflink.datastream.checkpointing_mode import CheckpointingMode

__all__ = ['CheckpointConfig']


class CheckpointConfig(object):
    """
    Configuration that captures all checkpointing related settings.
    """

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
            self._j_checkpoint_config.getCheckpointingConsistencyMode())

    def set_checkpointing_mode(self, checkpointing_mode: CheckpointingMode) -> 'CheckpointConfig':
        """
        Sets the checkpointing mode (:data:`CheckpointingMode.EXACTLY_ONCE` vs.
        :data:`CheckpointingMode.AT_LEAST_ONCE`).

        Example:
        ::

            >>> config.set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)

        :param checkpointing_mode: The :class:`CheckpointingMode`.
        """
        self._j_checkpoint_config.setCheckpointingConsistencyMode(
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

        .. note:: Deprecated in 2.0. Use :func:`get_tolerable_checkpoint_failure_number` instead.
        """
        return self.get_tolerable_checkpoint_failure_number() == 0

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

        .. note:: Deprecated in 2.0. Use :func:`set_tolerable_checkpoint_failure_number` instead.
        """
        if fail_on_checkpointing_errors:
            self.set_tolerable_checkpoint_failure_number(0)
        else:
            self.set_tolerable_checkpoint_failure_number(2147483647)
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

    def set_externalized_checkpoint_retention(
            self,
            retention_mode: 'ExternalizedCheckpointRetention') -> 'CheckpointConfig':
        """
        Sets the mode for externalized checkpoint clean-up. Externalized checkpoints will be enabled
        automatically unless the mode is set to
        :data:`ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS`.

        Externalized checkpoints write their meta data out to persistent storage and are **not**
        automatically cleaned up when the owning job fails or is suspended (terminating with job
        status ``FAILED`` or ``SUSPENDED``). In this case, you have to manually clean up the
        checkpoint state, both the meta data and actual program state.

        The :class:`ExternalizedCheckpointRetention` mode defines how an externalized checkpoint
        should be cleaned up on job cancellation. If you choose to retain externalized checkpoints
        on cancellation you have to handle checkpoint clean-up manually when you cancel the job as
        well (terminating with job status ``CANCELED``).

        The target directory for externalized checkpoints is configured via
        ``org.apache.flink.configuration.CheckpointingOptions#CHECKPOINTS_DIRECTORY``.

        Example:
        ::

            >>> config.set_externalized_checkpoint_retention(
            ...     ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)

        :param retention_mode: Externalized checkpoint clean-up behaviour, the mode could be
                             :data:`ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION`,
                             :data:`ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION` or
                             :data:`ExternalizedCheckpointRetention.NO_EXTERNALIZED_CHECKPOINTS`
        """
        self._j_checkpoint_config.setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention._to_j_externalized_checkpoint_retention(retention_mode))
        return self

    def is_externalized_checkpoints_enabled(self) -> bool:
        """
        Returns whether checkpoints should be persisted externally.

        :return: ``True`` if checkpoints should be externalized, false otherwise.
        """
        return self._j_checkpoint_config.isExternalizedCheckpointsEnabled()

    def get_externalized_checkpoint_retention(self) -> Optional['ExternalizedCheckpointRetention']:
        """
        Returns the cleanup behaviour for externalized checkpoints.

        :return: The cleanup behaviour for externalized checkpoints or ``None`` if none is
                 configured.
        """
        retention_mode = self._j_checkpoint_config.getExternalizedCheckpointRetention()
        if retention_mode is None:
            return None
        else:
            return ExternalizedCheckpointRetention._from_j_externalized_checkpoint_retention(
                retention_mode)

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

    def set_aligned_checkpoint_timeout(self, alignment_timeout: Duration) -> 'CheckpointConfig':
        """
        Only relevant if :func:`enable_unaligned_checkpoints` is enabled.

        If ``alignment_timeout`` has value equal to ``0``, checkpoints will always start unaligned.
        If ``alignment_timeout`` has value greater then ``0``, checkpoints will start aligned. If
        during checkpointing, checkpoint start delay exceeds this ``alignment_timeout``, alignment
        will timeout and checkpoint will start working as unaligned checkpoint.

        :param alignment_timeout: The duration until the aligned checkpoint will be converted into
                                  an unaligned checkpoint.
        """
        self._j_checkpoint_config.setAlignedCheckpointTimeout(alignment_timeout._j_duration)
        return self

    def get_aligned_checkpoint_timeout(self) -> 'Duration':
        """
        Returns the alignment timeout, as configured via :func:`set_alignment_timeout` or
        ``org.apache.flink.configuration.CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT``.

        :return: the alignment timeout.
        """
        return Duration(self._j_checkpoint_config.getAlignedCheckpointTimeout())

    def set_alignment_timeout(self, alignment_timeout: Duration) -> 'CheckpointConfig':
        """
        Only relevant if :func:`enable_unaligned_checkpoints` is enabled.

        If ``alignment_timeout`` has value equal to ``0``, checkpoints will always start unaligned.
        If ``alignment_timeout`` has value greater then ``0``, checkpoints will start aligned. If
        during checkpointing, checkpoint start delay exceeds this ``alignment_timeout``, alignment
        will timeout and checkpoint will start working as unaligned checkpoint.

        :param alignment_timeout: The duration until the aligned checkpoint will be converted into
                                  an unaligned checkpoint.

        .. note:: Deprecated in 2.0. Use :func:`set_aligned_checkpoint_timeout` instead.
        """
        self.set_aligned_checkpoint_timeout(alignment_timeout)
        return self

    def get_alignment_timeout(self) -> 'Duration':
        """
        Returns the alignment timeout, as configured via :func:`set_alignment_timeout` or
        ``org.apache.flink.configuration.CheckpointingOptions#ALIGNED_CHECKPOINT_TIMEOUT``.

        :return: the alignment timeout.

        .. note:: Deprecated in 2.0. Use :func:`get_aligned_checkpoint_timeout` instead.
        """
        return self.get_aligned_checkpoint_timeout()

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
