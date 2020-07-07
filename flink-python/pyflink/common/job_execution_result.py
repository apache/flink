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
from pyflink.common.job_id import JobID

__all__ = ['JobExecutionResult']


class JobExecutionResult(object):
    """
    The result of a job execution. Gives access to the execution time of the job,
    and to all accumulators created by this job.

    .. versionadded:: 1.11.0
    """

    def __init__(self, j_job_execution_result):
        self._j_job_execution_result = j_job_execution_result

    def get_job_id(self):
        """
        Returns the JobID assigned to the job by the Flink runtime.

        :return: JobID, or null if the job has been executed on a runtime without JobIDs
                 or if the execution failed.

        .. versionadded:: 1.11.0
        """
        return JobID(self._j_job_execution_result.getJobID())

    def get_net_runtime(self):
        """
        Gets the net execution time of the job, i.e., the execution time in the parallel system,
        without the pre-flight steps like the optimizer.

        :return: The net execution time in milliseconds.

        .. versionadded:: 1.11.0
        """
        return self._j_job_execution_result.getNetRuntime()

    def get_accumulator_result(self, accumulator_name):
        """
        Gets the accumulator with the given name. Returns None, if no accumulator with
        that name was produced.

        :param accumulator_name: The name of the accumulator.
        :return: The value of the accumulator with the given name.

        .. versionadded:: 1.11.0
        """
        return self.get_all_accumulator_results().get(accumulator_name)

    def get_all_accumulator_results(self):
        """
        Gets all accumulators produced by the job. The map contains the accumulators as
        mappings from the accumulator name to the accumulator value.

        :return: The dict which the keys are names of the accumulator and the values
                 are values of the accumulator produced by the job.

        .. versionadded:: 1.11.0
        """
        j_result_map = self._j_job_execution_result.getAllAccumulatorResults()
        accumulators = {}
        for key in j_result_map:
            accumulators[key] = j_result_map[key]
        return accumulators

    def __str__(self):
        """
        Convert JobExecutionResult to a string, if possible.

        .. versionadded:: 1.11.0
        """
        return self._j_job_execution_result.toString()
