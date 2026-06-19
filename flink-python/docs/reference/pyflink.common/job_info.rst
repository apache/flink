.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


===============
Job Information
===============

Job Client
----------

.. currentmodule:: pyflink.common.job_client

.. autosummary::
    :toctree: api/

    JobClient.get_job_id
    JobClient.get_job_status
    JobClient.cancel
    JobClient.stop_with_savepoint
    JobClient.trigger_savepoint
    JobClient.get_accumulators
    JobClient.get_job_execution_result


JobExecution Result
-------------------

.. currentmodule:: pyflink.common.job_execution_result

.. autosummary::
    :toctree: api/

    JobExecutionResult.get_job_id
    JobExecutionResult.get_net_runtime
    JobExecutionResult.get_accumulator_result
    JobExecutionResult.get_all_accumulator_results


Job Status
----------

.. currentmodule:: pyflink.common.job_status

.. autosummary::
    :toctree: api/

    JobStatus
