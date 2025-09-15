/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.timeline;

/** The enum to represent the reason why a rescale event is terminated. */
public enum TerminatedReason {
    SUCCEEDED(TerminalState.COMPLETED, "The rescale was completed successfully."),
    EXCEPTION_OCCURRED(
            TerminalState.FAILED,
            "The rescale was failed due to some exceptions about no resources enough."),
    RESOURCE_REQUIREMENTS_UPDATED(
            TerminalState.IGNORED, "The rescale was ignored due to the new resource requirements."),
    NO_RESOURCES_OR_PARALLELISMS_CHANGE(
            TerminalState.IGNORED,
            "The rescale was ignored due to no available resources change or parallelism change."),
    JOB_FINISHED(TerminalState.IGNORED, "The rescale was ignored due to the job finished."),
    JOB_FAILING(TerminalState.IGNORED, "The rescale was ignored due to the job failing."),
    JOB_FAILOVER_RESTARTING(
            TerminalState.IGNORED, "The rescale was ignored due to the job failover restarting."),
    JOB_CANCELING(TerminalState.IGNORED, "The rescale was ignored due to the job canceling.");

    private final TerminalState terminalState;
    private final String description;

    TerminatedReason(TerminalState terminalState, String description) {
        this.terminalState = terminalState;
        this.description = description;
    }

    public TerminalState getTerminalState() {
        return terminalState;
    }

    public String getDescription() {
        return description;
    }
}
