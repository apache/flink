/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import java.util.Map;
import java.util.Objects;

class JobInitializationMetrics {
    public static final long UNSET = -1;

    private final long checkpointId;
    private final long stateSize;
    private final InitializationStatus status;
    private final long initializationStartTs;
    private final long initializationEndTs;
    private final Map<String, SumMaxDuration> durationMetrics;

    public JobInitializationMetrics(
            long checkpointId,
            long stateSize,
            InitializationStatus status,
            long initializationStartTs,
            long initializationEndTs,
            Map<String, SumMaxDuration> durationMetrics) {
        this.checkpointId = checkpointId;
        this.stateSize = stateSize;
        this.status = status;

        this.initializationStartTs = initializationStartTs;
        this.initializationEndTs = initializationEndTs;
        this.durationMetrics = durationMetrics;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getStateSize() {
        return stateSize;
    }

    public InitializationStatus getStatus() {
        return status;
    }

    public long getStartTs() {
        return initializationStartTs;
    }

    public long getEndTs() {
        return initializationEndTs;
    }

    public Map<String, SumMaxDuration> getDurationMetrics() {
        return durationMetrics;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                + "{"
                + "checkpointId="
                + checkpointId
                + ", stateSize="
                + stateSize
                + ", status="
                + status
                + ", initializationStartTs="
                + initializationStartTs
                + ", "
                + durationMetrics
                + "}";
    }

    static class SumMaxDuration {
        private final String name;
        private long sumDuration = 0;
        private long maxDuration = 0;

        SumMaxDuration(String name) {
            this.name = name;
        }

        SumMaxDuration addDuration(long duration) {
            if (duration == SubTaskInitializationMetrics.UNSET) {
                return this;
            }
            sumDuration += duration;
            maxDuration = Math.max(maxDuration, duration);
            return this;
        }

        public String getName() {
            return name;
        }

        public long getSum() {
            return sumDuration;
        }

        public long getMax() {
            return maxDuration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, sumDuration, maxDuration);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SumMaxDuration that = (SumMaxDuration) o;

            return name == that.name
                    && sumDuration == that.sumDuration
                    && maxDuration == that.maxDuration;
        }

        @Override
        public String toString() {
            return String.format("%s(max=%d, sum=%d)", name, maxDuration, sumDuration);
        }
    }
}
