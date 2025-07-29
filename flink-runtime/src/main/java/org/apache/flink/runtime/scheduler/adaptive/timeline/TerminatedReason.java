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
    SUCCEEDED(TerminalState.COMPLETED),
    EXCEPTION_OCCURRED(TerminalState.FAILED),
    RESOURCE_REQUIREMENTS_UPDATED(TerminalState.IGNORED),
    NO_RESOURCES_OR_PARALLELISMS_CHANGE(TerminalState.IGNORED),
    JOB_FINISHED(TerminalState.IGNORED),
    JOB_FAILING(TerminalState.IGNORED),
    JOB_FAILOVER_RESTARTING(TerminalState.IGNORED),
    JOB_CANCELING(TerminalState.IGNORED);

    private final TerminalState terminalState;

    TerminatedReason(TerminalState terminalState) {
        this.terminalState = terminalState;
    }

    public TerminalState getTerminalState() {
        return terminalState;
    }
}
