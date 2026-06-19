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

import org.apache.flink.runtime.util.stats.StatsSummarySnapshot;

import java.io.Serializable;

public class RescalesSummarySnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final StatsSummarySnapshot allTerminatedSummarySnapshot;

    private final StatsSummarySnapshot completedRescalesSummarySnapshot;
    private final StatsSummarySnapshot ignoredRescalesSummarySnapshot;
    private final StatsSummarySnapshot failedRescalesSummarySnapshot;

    private long totalRescalesCount = 0L;
    private long inProgressRescaleCount = 0L;

    public RescalesSummarySnapshot(
            StatsSummarySnapshot allTerminatedSummarySnapshot,
            StatsSummarySnapshot completedRescalesSummarySnapshot,
            StatsSummarySnapshot ignoredRescalesSummarySnapshot,
            StatsSummarySnapshot failedRescalesSummarySnapshot,
            long totalRescalesCount,
            long inProgressRescaleCount) {
        this.allTerminatedSummarySnapshot = allTerminatedSummarySnapshot;
        this.completedRescalesSummarySnapshot = completedRescalesSummarySnapshot;
        this.ignoredRescalesSummarySnapshot = ignoredRescalesSummarySnapshot;
        this.failedRescalesSummarySnapshot = failedRescalesSummarySnapshot;
        this.totalRescalesCount = totalRescalesCount;
        this.inProgressRescaleCount = inProgressRescaleCount;
    }

    public StatsSummarySnapshot getAllTerminatedSummarySnapshot() {
        return allTerminatedSummarySnapshot;
    }

    public StatsSummarySnapshot getCompletedRescalesSummarySnapshot() {
        return completedRescalesSummarySnapshot;
    }

    public StatsSummarySnapshot getIgnoredRescalesSummarySnapshot() {
        return ignoredRescalesSummarySnapshot;
    }

    public StatsSummarySnapshot getFailedRescalesSummarySnapshot() {
        return failedRescalesSummarySnapshot;
    }

    public long getTotalRescalesCount() {
        return totalRescalesCount;
    }

    public long getCompletedRescalesCount() {
        return completedRescalesSummarySnapshot.getCount();
    }

    public long getIgnoredRescalesCount() {
        return ignoredRescalesSummarySnapshot.getCount();
    }

    public long getFailedRescalesCount() {
        return failedRescalesSummarySnapshot.getCount();
    }

    public long getInProgressRescaleCount() {
        return inProgressRescaleCount;
    }
}
