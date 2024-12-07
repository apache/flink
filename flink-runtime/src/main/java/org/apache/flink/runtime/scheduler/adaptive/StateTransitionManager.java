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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/**
 * The {@code StateTransitionManager} decides on whether {@link AdaptiveScheduler} state transition
 * should happen or not.
 */
public interface StateTransitionManager {

    /**
     * Is called if the environment changed in a way that a state transition could be considered.
     */
    void onChange();

    /**
     * Is called when any previous observed environment changes shall be verified possibly
     * triggering a state transition operation.
     */
    void onTrigger();

    /** Is called when the state transition manager should be closed. */
    default void close() {}

    /**
     * The interface that can be used by the {@code StateTransitionManager} to communicate with the
     * underlying system.
     */
    interface Context {

        /**
         * Returns {@code true} if the available resources are sufficient enough for a state
         * transition; otherwise {@code false}.
         */
        boolean hasSufficientResources();

        /**
         * Returns {@code true} if the available resources meet the desired resources for the job;
         * otherwise {@code false}.
         */
        boolean hasDesiredResources();

        /** Triggers the transition to the subsequent state of the {@link AdaptiveScheduler}. */
        void transitionToSubsequentState();

        /**
         * Runs operation with a given delay in the underlying main thread.
         *
         * @return a ScheduledFuture representing pending completion of the operation.
         */
        ScheduledFuture<?> scheduleOperation(Runnable callback, Duration delay);

        /**
         * Gets the {@link JobID} of the job.
         *
         * @return the {@link JobID} of the job
         */
        JobID getJobId();
    }
}
