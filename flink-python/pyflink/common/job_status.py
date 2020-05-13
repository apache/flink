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
from pyflink.java_gateway import get_gateway

__all__ = ['JobStatus']


class JobStatus(object):
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
        super().__init__()
        self._j_job_status = j_job_status

    def is_globally_terminal_state(self):
        """
        Checks whether this state is <i>globally terminal</i>. A globally terminal job
        is complete and cannot fail any more and will not be restarted or recovered by another
        standby master node.

        When a globally terminal state has been reached, all recovery data for the job is
        dropped from the high-availability services.

        :return: ``True`` if this job status is globally terminal, ``False`` otherwise.
        """
        return self._j_job_status.isGloballyTerminalState()

    def is_terminal_state(self):
        """
        Checks whether this state is locally terminal. Locally terminal refers to the
        state of a job's execution graph within an executing JobManager. If the execution graph
        is locally terminal, the JobManager will not continue executing or recovering the job.

        The only state that is locally terminal, but not globally terminal is SUSPENDED,
        which is typically entered when the executing JobManager looses its leader status.

        :return: ``True`` if this job status is terminal, ``False`` otherwise.
        """
        return self._j_job_status.isTerminalState()

    @staticmethod
    def _from_j_job_status(j_job_status):
        gateway = get_gateway()
        JJobStatus = gateway.jvm.org.apache.flink.api.common.JobStatus
        if j_job_status == JJobStatus.CREATED:
            return JobStatus.CREATED
        elif j_job_status == JJobStatus.RUNNING:
            return JobStatus.RUNNING
        elif j_job_status == JJobStatus.FAILING:
            return JobStatus.FAILING
        elif j_job_status == JJobStatus.FAILED:
            return JobStatus.FAILED
        elif j_job_status == JJobStatus.CANCELLING:
            return JobStatus.CANCELLING
        elif j_job_status == JJobStatus.CANCELED:
            return JobStatus.CANCELED
        elif j_job_status == JJobStatus.FINISHED:
            return JobStatus.FINISHED
        elif j_job_status == JJobStatus.RESTARTING:
            return JobStatus.RESTARTING
        elif j_job_status == JJobStatus.SUSPENDED:
            return JobStatus.SUSPENDED
        elif j_job_status == JJobStatus.RECONCILING:
            return JobStatus.RECONCILING
        else:
            raise Exception("Unsupported java job status: %s" % j_job_status)

    @staticmethod
    def _to_j_job_status(job_status):
        gateway = get_gateway()
        JJobStatus = gateway.jvm.org.apache.flink.api.common.JobStatus
        if job_status == JobStatus.CREATED:
            return JJobStatus.CREATED
        elif job_status == JobStatus.RUNNING:
            return JJobStatus.RUNNING
        elif job_status == JobStatus.FAILING:
            return JJobStatus.FAILING
        elif job_status == JobStatus.FAILED:
            return JJobStatus.FAILED
        elif job_status == JobStatus.CANCELLING:
            return JJobStatus.CANCELLING
        elif job_status == JobStatus.CANCELED:
            return JJobStatus.CANCELED
        elif job_status == JobStatus.FINISHED:
            return JJobStatus.FINISHED
        elif job_status == JobStatus.RESTARTING:
            return JJobStatus.RESTARTING
        elif job_status == JobStatus.SUSPENDED:
            return JJobStatus.SUSPENDED
        elif job_status == JobStatus.RECONCILING:
            return JJobStatus.RECONCILING
        else:
            raise TypeError("Unsupported job status: %s, supported job statuses are: "
                            "JobStatus.CREATED, JobStatus.RUNNING, "
                            "JobStatus.FAILING, JobStatus.FAILED, "
                            "JobStatus.CANCELLING, JobStatus.CANCELED, "
                            "JobStatus.FINISHED, JobStatus.RESTARTING, "
                            "JobStatus.SUSPENDED and JobStatus.RECONCILING." % job_status)
