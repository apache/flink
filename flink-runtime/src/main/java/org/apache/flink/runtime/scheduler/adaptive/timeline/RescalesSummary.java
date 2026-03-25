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

import org.apache.flink.runtime.util.stats.StatsSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/** Statistics summary of rescales. */
public class RescalesSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RescalesSummary.class);

    private final StatsSummary allTerminatedSummary;
    private final StatsSummary completedRescalesSummary;
    private final StatsSummary ignoredRescalesSummary;
    private final StatsSummary failedRescalesSummary;

    // Total terminated count
    private long totalRescalesCount = 0L;
    private long inProgressRescalesCount = 0L;

    public RescalesSummary(int maxHistorySize) {
        this.allTerminatedSummary = new StatsSummary(maxHistorySize);
        this.completedRescalesSummary = new StatsSummary(maxHistorySize);
        this.ignoredRescalesSummary = new StatsSummary(maxHistorySize);
        this.failedRescalesSummary = new StatsSummary(maxHistorySize);
    }

    /**
     * Add a terminated rescale in. Note, The method could be called after calling {@link
     * #addInProgress(Rescale)}.
     *
     * @param rescale the target terminated rescale.
     */
    public void addTerminated(Rescale rescale) {
        if (!Rescale.isTerminated(rescale)) {
            LOG.warn(
                    "Unexpected rescale: {}, which will be ignored when computing statistics.",
                    rescale);
            return;
        }

        this.allTerminatedSummary.add(rescale.getDuration().toMillis());
        this.inProgressRescalesCount = 0;

        if (rescale.getTerminalState() == null) {
            return;
        }

        switch (rescale.getTerminalState()) {
            case FAILED:
                failedRescalesSummary.add(rescale.getDuration().toMillis());
                break;
            case COMPLETED:
                completedRescalesSummary.add(rescale.getDuration().toMillis());
                break;
            case IGNORED:
                ignoredRescalesSummary.add(rescale.getDuration().toMillis());
                break;
            default:
                break;
        }
    }

    /**
     * Add an in-progress rescale in. Note, The method could be called before {@link
     * #addTerminated(Rescale)}.
     *
     * @param rescale the target non-terminated rescale.
     */
    public void addInProgress(Rescale rescale) {
        if (Rescale.isTerminated(rescale)) {
            LOG.warn("Unexpected rescale: {}, which will be ignored.", rescale);
        } else {
            inProgressRescalesCount++;
            totalRescalesCount++;
        }
    }

    public long getTotalRescalesCount() {
        return totalRescalesCount;
    }

    public long getInProgressRescalesCount() {
        return inProgressRescalesCount;
    }

    public long getCompletedRescalesCount() {
        return completedRescalesSummary.getCount();
    }

    public long getIgnoredRescalesCount() {
        return ignoredRescalesSummary.getCount();
    }

    public long getFailedRescalesCount() {
        return failedRescalesSummary.getCount();
    }

    public StatsSummary getAllTerminatedSummary() {
        return allTerminatedSummary;
    }

    public StatsSummary getCompletedRescalesSummary() {
        return completedRescalesSummary;
    }

    public StatsSummary getIgnoredRescalesSummary() {
        return ignoredRescalesSummary;
    }

    public StatsSummary getFailedRescalesSummary() {
        return failedRescalesSummary;
    }
}
