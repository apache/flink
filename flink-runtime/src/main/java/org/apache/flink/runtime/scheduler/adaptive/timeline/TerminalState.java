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

/** The enum to represent the terminal state of a rescale. */
public enum TerminalState {
    COMPLETED("It represents the rescale was completed successfully"),
    FAILED(
            "It represents the rescale was failed due to some exceptions about lack of condition resources."),
    IGNORED(
            "It represents the rescale was ignored by some new conditions that could trigger a new rescale.\n"
                    + "For example,\n"
                    + "   The scheduler has received a new resource request,\n"
                    + "   A job restart is triggered during rescale due to any exception,\n"
                    + "   Available resources or parallelism do not change during rescale,\n"
                    + "   The job reaches a terminal state during rescale: FINISHED, FAILING, CANCELING.");

    private final String description;

    TerminalState(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public static boolean isTerminated(TerminalState terminalState) {
        return terminalState != null;
    }
}
