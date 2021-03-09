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

from pyflink.java_gateway import get_gateway

__all__ = ['JobStatus']


class JobStatus(Enum):
    """
    Possible states of a job once it has been accepted by the job manager.

    :data:`CREATED`:

    Job is newly created, no task has started to run.

    :data:`RUNNING`:

    Some tasks are scheduled or running, some may be pending, some may be finished.

    :data:`FAILING`:

    The job has failed and is currently waiting for the cleanup to complete.

    :data:`FAILED`:

    The job has failed with a non-recoverable task failure.

    :data:`CANCELLING`:

    Job is being cancelled.

    :data:`CANCELED`:

    Job has been cancelled.

    :data:`FINISHED`:

    All of the job's tasks have successfully finished.

    :data:`RESTARTING`:

    The job is currently undergoing a reset and total restart.

    :data:`SUSPENDED`:

    The job has been suspended which means that it has been stopped but not been removed from a
    potential HA job store.

    :data:`RECONCILING`:

    The job is currently reconciling and waits for task execution report to recover state.

    .. versionadded:: 1.11.0
    """

    CREATED = 0
    RUNNING = 1
    FAILING = 2
    FAILED = 3
    CANCELLING = 4
    CANCELED = 5
    FINISHED = 6
    RESTARTING = 7
    SUSPENDED = 8
    RECONCILING = 9

    def __init__(self, j_job_status) -> None:
        self._j_job_status = j_job_status

    def is_globally_terminal_state(self) -> bool:
        """
        Checks whether this state is <i>globally terminal</i>. A globally terminal job
        is complete and cannot fail any more and will not be restarted or recovered by another
        standby master node.

        When a globally terminal state has been reached, all recovery data for the job is
        dropped from the high-availability services.

        :return: ``True`` if this job status is globally terminal, ``False`` otherwise.

        .. versionadded:: 1.11.0
        """
        return self._j_job_status.isGloballyTerminalState()

    def is_terminal_state(self) -> bool:
        """
        Checks whether this state is locally terminal. Locally terminal refers to the
        state of a job's execution graph within an executing JobManager. If the execution graph
        is locally terminal, the JobManager will not continue executing or recovering the job.

        The only state that is locally terminal, but not globally terminal is SUSPENDED,
        which is typically entered when the executing JobManager looses its leader status.

        :return: ``True`` if this job status is terminal, ``False`` otherwise.

        .. versionadded:: 1.11.0
        """
        return self._j_job_status.isTerminalState()

    @staticmethod
    def _from_j_job_status(j_job_status) -> 'JobStatus':
        return JobStatus[j_job_status.name()]

    def _to_j_job_status(self):
        gateway = get_gateway()
        JJobStatus = gateway.jvm.org.apache.flink.api.common.JobStatus
        return getattr(JJobStatus, self.name)
