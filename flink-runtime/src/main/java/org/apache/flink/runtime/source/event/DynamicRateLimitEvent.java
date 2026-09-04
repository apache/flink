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

package org.apache.flink.runtime.source.event;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.Objects;

/**
 * Signals a source operator subtask its current, coordinator-computed target rate limit (records
 * per second, for that subtask only). The coordinator derives the global rate from checkpoint
 * completion times and splits it across subtasks with a {@code RateAllocations}.
 */
public class DynamicRateLimitEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final double recordsPerSecond;

    public DynamicRateLimitEvent(double recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
    }

    public double getRecordsPerSecond() {
        return recordsPerSecond;
    }

    /**
     * @return true, because even if lost, {@link DynamicRateLimitEvent} will be re-sent again after
     *     the next checkpoint completes, and it doesn't hold any critical information that could
     *     lead to a data loss.
     */
    @Override
    public boolean isLossTolerant() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DynamicRateLimitEvent that = (DynamicRateLimitEvent) o;
        return Double.compare(that.recordsPerSecond, recordsPerSecond) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordsPerSecond);
    }

    @Override
    public String toString() {
        return "DynamicRateLimitEvent{" + "recordsPerSecond=" + recordsPerSecond + '}';
    }
}
