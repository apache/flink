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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * A snapshot of one source subtask's rate limit state, handed to a {@link RateAllocations} so it
 * can decide how to split the global target rate across subtasks.
 */
@PublicEvolving
public final class SubtaskRateInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final double allocatedRecordsPerSecond;
    private final double observedRecordsPerSecond;

    public SubtaskRateInfo(
            int subtaskId, double allocatedRecordsPerSecond, double observedRecordsPerSecond) {
        this.subtaskId = subtaskId;
        this.allocatedRecordsPerSecond = allocatedRecordsPerSecond;
        this.observedRecordsPerSecond = observedRecordsPerSecond;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    /** The rate most recently allocated to this subtask, or {@code NaN} if none was allocated. */
    public double getAllocatedRecordsPerSecond() {
        return allocatedRecordsPerSecond;
    }

    /** The throughput this subtask last reported, or {@code NaN} if it has not reported yet. */
    public double getObservedRecordsPerSecond() {
        return observedRecordsPerSecond;
    }

    public boolean hasAllocation() {
        return !Double.isNaN(allocatedRecordsPerSecond);
    }

    public boolean hasObservation() {
        return !Double.isNaN(observedRecordsPerSecond);
    }

    @Override
    public String toString() {
        return "SubtaskRateInfo{subtaskId="
                + subtaskId
                + ", allocatedRecordsPerSecond="
                + allocatedRecordsPerSecond
                + ", observedRecordsPerSecond="
                + observedRecordsPerSecond
                + '}';
    }
}
