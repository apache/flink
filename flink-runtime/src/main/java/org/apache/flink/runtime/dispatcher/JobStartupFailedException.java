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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Exception signalling that an exception occurred during the execution of the jar's main method.
 *
 * <p>The job will transition to FAILED state, and it will not be recovered.
 */
@Internal
public class JobStartupFailedException extends Exception {

    /** Serial version UID for serialization interoperability. */
    private static final long serialVersionUID = -2417524218857151612L;

    private final Collection<JobValidationError> errors;
    private final JobID jobId;
    private final String jobName;

    public JobStartupFailedException(
            JobID jobId, String jobName, Collection<JobValidationError> errors) {
        this.jobId = checkNotNull(jobId);
        this.jobName = checkNotNull(jobName);
        this.errors = checkNotNull(errors);
    }

    public Collection<JobValidationError> getErrors() {
        return errors;
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public String toString() {
        return "JobStartupFailedException{"
                + "errors="
                + errors
                + ", jobId="
                + jobId
                + ", jobName='"
                + jobName
                + '\''
                + '}';
    }
}
