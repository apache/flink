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
from pyflink.common.completable_future import CompletableFuture
from pyflink.common.job_execution_result import JobExecutionResult
from pyflink.common.job_id import JobID
from pyflink.common.job_status import JobStatus

__all__ = ['JobClient']


class JobClient(object):
    """
    A client that is scoped to a specific job.

    .. versionadded:: 1.11.0
    """

    def __init__(self, j_job_client):
        self._j_job_client = j_job_client

    def get_job_id(self) -> JobID:
        """
        Returns the JobID that uniquely identifies the job this client is scoped to.

        :return: JobID, or null if the job has been executed on a runtime without JobIDs
                 or if the execution failed.

        .. versionadded:: 1.11.0
        """
        return JobID(self._j_job_client.getJobID())

    def get_job_status(self) -> CompletableFuture:
        """
        Requests the JobStatus of the associated job.

        :return: A CompletableFuture containing the JobStatus of the associated job.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(self._j_job_client.getJobStatus(), JobStatus)

    def cancel(self) -> CompletableFuture:
        """
        Cancels the associated job.

        :return: A CompletableFuture for canceling the associated job.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(self._j_job_client.cancel())

    def stop_with_savepoint(self, advance_to_end_of_event_time: bool,
                            savepoint_directory: str = None) -> CompletableFuture:
        """
        Stops the associated job on Flink cluster.

        Stopping works only for streaming programs. Be aware, that the job might continue to run
        for a while after sending the stop command, because after sources stopped to emit data all
        operators need to finish processing.

        :param advance_to_end_of_event_time: Flag indicating if the source should inject a
                                             MAX_WATERMARK in the pipeline.
        :param savepoint_directory: Directory the savepoint should be written to.
        :return: A CompletableFuture containing the path where the savepoint is located.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(
            self._j_job_client.stopWithSavepoint(advance_to_end_of_event_time, savepoint_directory),
            str)

    def trigger_savepoint(self, savepoint_directory: str = None) -> CompletableFuture:
        """
        Triggers a savepoint for the associated job. The savepoint will be written to the given
        savepoint directory.

        :param savepoint_directory: Directory the savepoint should be written to.
        :return: A CompletableFuture containing the path where the savepoint is located.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(self._j_job_client.triggerSavepoint(savepoint_directory), str)

    def get_accumulators(self) -> CompletableFuture:
        """
        Requests the accumulators of the associated job. Accumulators can be requested while it
        is running or after it has finished. The class loader is used to deserialize the incoming
        accumulator results.

        :param class_loader: Class loader used to deserialize the incoming accumulator results.
        :return: A CompletableFuture containing the accumulators of the associated job.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(self._j_job_client.getAccumulators(), dict)

    def get_job_execution_result(self) -> CompletableFuture:
        """
        Returns the JobExecutionResult result of the job execution of the submitted job.

        :return: A CompletableFuture containing the JobExecutionResult result of the job execution.

        .. versionadded:: 1.11.0
        """
        return CompletableFuture(self._j_job_client.getJobExecutionResult(),
                                 JobExecutionResult)
