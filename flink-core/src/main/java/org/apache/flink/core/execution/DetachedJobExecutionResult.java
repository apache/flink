/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;

import java.util.Map;

/**
 * The {@link JobExecutionResult} returned by a context environment when executing a job in detached
 * mode.
 */
@Internal
public final class DetachedJobExecutionResult extends JobExecutionResult {

    public static final String DETACHED_MESSAGE = "Job was submitted in detached mode. ";

    public static final String EAGER_FUNCTION_MESSAGE =
            "Please make sure your program doesn't call "
                    + "an eager execution function [collect, print, printToErr, count]. ";

    public static final String JOB_RESULT_MESSAGE =
            "Results of job execution, such as accumulators,"
                    + " runtime, etc. are not available. ";

    public DetachedJobExecutionResult(final JobID jobID) {
        super(jobID, -1, null);
    }

    @Override
    public long getNetRuntime() {
        throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
    }

    @Override
    public <T> T getAccumulatorResult(String accumulatorName) {
        throw new InvalidProgramException(
                DETACHED_MESSAGE + JOB_RESULT_MESSAGE + EAGER_FUNCTION_MESSAGE);
    }

    @Override
    public Map<String, Object> getAllAccumulatorResults() {
        throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
    }

    @Override
    public Integer getIntCounterResult(String accumulatorName) {
        throw new InvalidProgramException(DETACHED_MESSAGE + JOB_RESULT_MESSAGE);
    }

    @Override
    public JobID getJobID() {
        return super.getJobID();
    }

    @Override
    public boolean isJobExecutionResult() {
        return false;
    }

    @Override
    public JobExecutionResult getJobExecutionResult() {
        return this;
    }

    @Override
    public String toString() {
        return "Job has been submitted with JobID " + getJobID();
    }
}
