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

    /** The application is being cancelled. */
    CANCELLING(false),

    /** The application has been cancelled. */
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
}
