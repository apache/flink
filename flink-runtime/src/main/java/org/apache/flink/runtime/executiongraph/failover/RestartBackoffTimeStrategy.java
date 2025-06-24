/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

/** Strategy to decide whether to restart failed tasks and the delay to do the restarting. */
public interface RestartBackoffTimeStrategy {

    /**
     * Returns whether a restart should be conducted.
     *
     * @return whether a restart should be conducted
     */
    boolean canRestart();

    /**
     * Returns the delay to do the restarting.
     *
     * @return the delay to do the restarting
     */
    long getBackoffTime();

    /**
     * Notify the strategy about the task failure cause.
     *
     * @param cause of the task failure
     * @return True means that the current failure is the first one after the most-recent failure
     *     handling happened, false means that there has been a failure before that was not handled,
     *     yet, and the current failure will be considered in a combined failure handling effort.
     */
    boolean notifyFailure(Throwable cause);

    // ------------------------------------------------------------------------
    //  factory
    // ------------------------------------------------------------------------

    /** The factory to instantiate {@link RestartBackoffTimeStrategy}. */
    interface Factory {

        /**
         * Instantiates the {@link RestartBackoffTimeStrategy}.
         *
         * @return The instantiated restart strategy.
         */
        RestartBackoffTimeStrategy create();
    }
}
