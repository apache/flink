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
    """

    def __init__(self, j_job_client):
        self._j_job_client = j_job_client

    def get_job_id(self):
        """
        Returns the JobID that uniquely identifies the job this client is scoped to.

        :return: JobID, or null if the job has been executed on a runtime without JobIDs
                 or if the execution failed.
        """
        return JobID(self._j_job_client.getJobID())

    def get_job_status(self):
        """
        Requests the JobStatus of the associated job.

        :return: A CompletableFuture containing the JobStatus of the associated job.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(self._j_job_client.getJobStatus(), JobStatus)

    def cancel(self):
        """
        Cancels the associated job.

        :return: A CompletableFuture for canceling the associated job.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(self._j_job_client.cancel())

    def stop_with_savepoint(self, advance_to_end_of_event_time, savepoint_directory):
        """
        Stops the associated job on Flink cluster.

        Stopping works only for streaming programs. Be aware, that the job might continue to run
        for a while after sending the stop command, because after sources stopped to emit data all
        operators need to finish processing.

        :param advance_to_end_of_event_time: Flag indicating if the source should inject a
                                             MAX_WATERMARK in the pipeline.
        :type advance_to_end_of_event_time: bool
        :param savepoint_directory: Directory the savepoint should be written to.
        :type savepoint_directory: str
        :return: A CompletableFuture containing the path where the savepoint is located.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(
            self._j_job_client.stopWithSavepoint(advance_to_end_of_event_time, savepoint_directory),
            str)

    def trigger_savepoint(self, savepoint_directory):
        """
        Triggers a savepoint for the associated job. The savepoint will be written to the given
        savepoint directory.

        :param savepoint_directory: Directory the savepoint should be written to.
        :type savepoint_directory: str
        :return: A CompletableFuture containing the path where the savepoint is located.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(self._j_job_client.triggerSavepoint(savepoint_directory), str)

    def get_accumulators(self, class_loader):
        """
        Requests the accumulators of the associated job. Accumulators can be requested while it
        is running or after it has finished. The class loader is used to deserialize the incoming
        accumulator results.

        :param class_loader: Class loader used to deserialize the incoming accumulator results.
        :return: A CompletableFuture containing the accumulators of the associated job.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(self._j_job_client.getAccumulators(class_loader), dict)

    def get_job_execution_result(self, user_class_loader):
        """
        Returns the JobExecutionResult result of the job execution of the submitted job.

        :param user_class_loader: Class loader used to deserialize the accumulators of the job.
        :return: A CompletableFuture containing the JobExecutionResult result of the job execution.
        :rtype: pyflink.common.CompletableFuture
        """
        return CompletableFuture(self._j_job_client.getJobExecutionResult(user_class_loader),
                                 JobExecutionResult)
