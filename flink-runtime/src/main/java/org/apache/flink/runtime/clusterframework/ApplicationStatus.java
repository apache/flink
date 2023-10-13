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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.api.common.JobStatus;

import org.apache.flink.shaded.guava31.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.EnumBiMap;

/** The status of an application. */
public enum ApplicationStatus {

    /** Application finished successfully. */
    SUCCEEDED(0),

    /** Application encountered an unrecoverable failure or error. */
    FAILED(1443),

    /** Application was canceled or killed on request. */
    CANCELED(0),

    /** Application status is not known. */
    UNKNOWN(1445);

    // ------------------------------------------------------------------------

    private static final BiMap<JobStatus, ApplicationStatus> JOB_STATUS_APPLICATION_STATUS_BI_MAP =
            EnumBiMap.create(JobStatus.class, ApplicationStatus.class);

    static {
        // only globally-terminated JobStatus have a corresponding ApplicationStatus
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FAILED, ApplicationStatus.FAILED);
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.CANCELED, ApplicationStatus.CANCELED);
        JOB_STATUS_APPLICATION_STATUS_BI_MAP.put(JobStatus.FINISHED, ApplicationStatus.SUCCEEDED);
    }

    /** The associated process exit code. */
    private final int processExitCode;

    ApplicationStatus(int exitCode) {
        this.processExitCode = exitCode;
    }

    /**
     * Gets the process exit code associated with this status.
     *
     * @return The associated process exit code.
     */
    public int processExitCode() {
        return processExitCode;
    }

    /**
     * Derives the ApplicationStatus that should be used for a job that resulted in the given job
     * status. If the job is not yet in a globally terminal state, this method returns {@link
     * #UNKNOWN}.
     */
    public static ApplicationStatus fromJobStatus(JobStatus jobStatus) {
        return JOB_STATUS_APPLICATION_STATUS_BI_MAP.getOrDefault(jobStatus, UNKNOWN);
    }

    /**
     * Derives the {@link JobStatus} from the {@code ApplicationStatus}.
     *
     * @return The corresponding {@code JobStatus}.
     * @throws UnsupportedOperationException for {@link #UNKNOWN}.
     */
    public JobStatus deriveJobStatus() {
        if (!JOB_STATUS_APPLICATION_STATUS_BI_MAP.inverse().containsKey(this)) {
            throw new UnsupportedOperationException(
                    this.name() + " cannot be mapped to a JobStatus.");
        }

        return JOB_STATUS_APPLICATION_STATUS_BI_MAP.inverse().get(this);
    }
}
