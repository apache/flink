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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;

import javax.annotation.Nullable;

/**
 * The rescale timeline information updating interface. When rescale history is enabled for a job,
 * this class is used to perform fast operations on rescaling and to keep historical records. That
 * is, a timeline used to record changes in rescaling.
 */
@Internal
public interface RescaleTimeline {

    /** Get the job information of current job. */
    @Nullable
    JobInformation getJobInformation();

    /**
     * Judge whether the current rescale in timeline is terminated or null, which represent no
     * rescales in the current phase.
     *
     * @return <code>true</code> if there are no rescales to perform. <code>false</code> else.
     */
    boolean isIdling();

    /**
     * Create a new rescale and assign it as current rescale. Note, the {@link #isIdling()} must be
     * true when creating a new current rescale. When there's a rescale in transition phase, we must
     * seal the current resale before creating a new rescale.
     *
     * @param newRescaleEpoch It represents whether the rescale resource requirements is in the new
     *     epoch.
     * @return the {@link RescaleIdInfo} of the new created {@link Rescale}.
     */
    RescaleIdInfo newRescale(boolean newRescaleEpoch);

    /** Get the latest rescale for the specified terminal state. */
    @Nullable
    Rescale getLatestRescale(TerminalState terminalState);

    /**
     * Update the current rescale. It only makes sense to update a rescale when there is an ongoing
     * rescale that is in the process of transition states.
     *
     * @param rescaleUpdater The action to update the current rescale.
     * @return <code>true</code> if update successfully <code>false</code> else.
     */
    boolean updateRescale(RescaleUpdater rescaleUpdater);

    /** Rescale operation interface. */
    interface RescaleUpdater {
        void update(Rescale rescaleToUpdate);
    }

    /** No-op implementation of {@link RescaleTimeline}. */
    enum NoOpRescaleTimeline implements RescaleTimeline {
        INSTANCE;

        @Override
        public RescaleIdInfo newRescale(boolean newRescaleEpoch) {
            return null;
        }

        @Override
        public boolean updateRescale(RescaleUpdater rescaleUpdater) {
            return false;
        }

        @Nullable
        @Override
        public Rescale getLatestRescale(TerminalState terminalState) {
            return null;
        }

        @Nullable
        @Override
        public JobInformation getJobInformation() {
            return null;
        }

        @Override
        public boolean isIdling() {
            return false;
        }
    }
}
