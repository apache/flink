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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A collection of simple metrics, around the triggering of a checkpoint. */
public class SubTaskInitializationMetrics implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final long UNSET = -1L;

    /**
     * WARNING! When adding new fields make sure that the math to calculate various durations in
     * this class's getters is still correct.
     */
    private final long startTs;

    private final long endTs;
    private final Map<String, Long> durationMetrics;
    private final InitializationStatus status;

    public SubTaskInitializationMetrics(
            long startTs,
            long endTs,
            Map<String, Long> durationMetrics,
            InitializationStatus status) {
        checkArgument(startTs >= 0);
        checkArgument(endTs >= startTs);
        this.startTs = startTs;
        this.endTs = endTs;
        this.durationMetrics = durationMetrics;
        this.status = status;
    }

    public long getStartTs() {
        return startTs;
    }

    public long getEndTs() {
        return endTs;
    }

    public Map<String, Long> getDurationMetrics() {
        return durationMetrics;
    }

    public InitializationStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubTaskInitializationMetrics that = (SubTaskInitializationMetrics) o;

        return startTs == that.startTs
                && endTs == that.endTs
                && Objects.equals(durationMetrics, that.durationMetrics)
                && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTs, endTs, durationMetrics, status);
    }

    @Override
    public String toString() {
        return SubTaskInitializationMetrics.class.getSimpleName()
                + "{"
                + "initializationStartTs="
                + startTs
                + "initializationEndTs="
                + endTs
                + "durationMetrics="
                + durationMetrics
                + "status="
                + status
                + '}';
    }
}
