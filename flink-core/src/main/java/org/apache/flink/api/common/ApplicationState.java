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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DeploymentOptions;

import java.util.HashMap;
import java.util.Map;

/** Possible states of an application. */
@PublicEvolving
public enum ApplicationState {

    /** The application is newly created and has not started running. */
    CREATED(false),

    /** The application has started running. */
    RUNNING(false),

    /** The application has encountered a failure and is waiting for the cleanup to complete. */
    FAILING(false),

    /** The application has failed due to an exception. */
    FAILED(true),

    /** The application is being canceled. */
    CANCELING(false),

    /** The application has been canceled. */
    CANCELED(true),

    /**
     * All jobs in the application have completed, See {@link
     * DeploymentOptions#TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION} for more information.
     */
    FINISHED(true);

    // --------------------------------------------------------------------------------------------

    private final boolean terminalState;

    ApplicationState(boolean terminalState) {
        this.terminalState = terminalState;
    }

    public boolean isTerminalState() {
        return terminalState;
    }

    private static final Map<JobStatus, ApplicationState> JOB_STATUS_APPLICATION_STATE_MAP =
            new HashMap<>();

    static {
        // only globally terminal JobStatus can have a corresponding ApplicationState
        JOB_STATUS_APPLICATION_STATE_MAP.put(JobStatus.FAILED, ApplicationState.FAILED);
        JOB_STATUS_APPLICATION_STATE_MAP.put(JobStatus.CANCELED, ApplicationState.CANCELED);
        JOB_STATUS_APPLICATION_STATE_MAP.put(JobStatus.FINISHED, ApplicationState.FINISHED);
    }

    /**
     * Derives the ApplicationState that corresponds to the given JobStatus. This method only
     * accepts globally terminal JobStatus. If the job status is not globally terminal, this method
     * throws an IllegalArgumentException.
     */
    public static ApplicationState fromJobStatus(JobStatus jobStatus) {
        if (!JOB_STATUS_APPLICATION_STATE_MAP.containsKey(jobStatus)) {
            throw new IllegalArgumentException(
                    "JobStatus " + jobStatus + " does not have a corresponding ApplicationState.");
        }

        return JOB_STATUS_APPLICATION_STATE_MAP.get(jobStatus);
    }
}
