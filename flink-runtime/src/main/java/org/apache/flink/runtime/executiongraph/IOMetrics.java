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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An instance of this class represents a snapshot of the io-related metrics of a single task. */
public class IOMetrics implements Serializable {

    private static final long serialVersionUID = -7208093607556457183L;

    protected long numRecordsIn;
    protected long numRecordsOut;

    protected long numBytesIn;
    protected long numBytesOut;

    protected long accumulateBackPressuredTime;
    protected double accumulateBusyTime;
    protected long accumulateIdleTime;

    @Nullable
    protected Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes;

    /**
     * Per-downstream-target breakdown of {@link #numRecordsOut}, keyed by target {@code
     * JobVertexID} (hex string). May be empty when the task has no outputs for which a target was
     * registered (e.g. sinks, or the broadcast fan-out path). Never {@code null}.
     */
    protected Map<String, Long> numRecordsOutPerTarget;

    public IOMetrics(
            Meter recordsIn,
            Meter recordsOut,
            Meter bytesIn,
            Meter bytesOut,
            Gauge<Long> accumulatedBackPressuredTime,
            Gauge<Long> accumulatedIdleTime,
            Gauge<Double> accumulatedBusyTime,
            Map<IntermediateResultPartitionID, ResultPartitionBytesCounter>
                    resultPartitionBytesCounters,
            Map<String, Long> numRecordsOutPerTarget) {
        this.numRecordsIn = recordsIn.getCount();
        this.numRecordsOut = recordsOut.getCount();
        this.numBytesIn = bytesIn.getCount();
        this.numBytesOut = bytesOut.getCount();
        this.accumulateBackPressuredTime = accumulatedBackPressuredTime.getValue();
        this.accumulateBusyTime = accumulatedBusyTime.getValue();
        this.accumulateIdleTime = accumulatedIdleTime.getValue();
        this.resultPartitionBytes =
                resultPartitionBytesCounters.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> entry.getValue().createSnapshot()));
        this.numRecordsOutPerTarget =
                numRecordsOutPerTarget == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(numRecordsOutPerTarget);
    }

    public IOMetrics(
            long numBytesIn,
            long numBytesOut,
            long numRecordsIn,
            long numRecordsOut,
            long accumulateIdleTime,
            double accumulateBusyTime,
            long accumulateBackPressuredTime) {
        this(
                numBytesIn,
                numBytesOut,
                numRecordsIn,
                numRecordsOut,
                accumulateIdleTime,
                accumulateBusyTime,
                accumulateBackPressuredTime,
                null);
    }

    @VisibleForTesting
    public IOMetrics(
            long numBytesIn,
            long numBytesOut,
            long numRecordsIn,
            long numRecordsOut,
            long accumulateIdleTime,
            double accumulateBusyTime,
            long accumulateBackPressuredTime,
            @Nullable
                    Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
        this(
                numBytesIn,
                numBytesOut,
                numRecordsIn,
                numRecordsOut,
                accumulateIdleTime,
                accumulateBusyTime,
                accumulateBackPressuredTime,
                resultPartitionBytes,
                null);
    }

    @VisibleForTesting
    public IOMetrics(
            long numBytesIn,
            long numBytesOut,
            long numRecordsIn,
            long numRecordsOut,
            long accumulateIdleTime,
            double accumulateBusyTime,
            long accumulateBackPressuredTime,
            @Nullable Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes,
            @Nullable Map<String, Long> numRecordsOutPerTarget) {
        this.numBytesIn = numBytesIn;
        this.numBytesOut = numBytesOut;
        this.numRecordsIn = numRecordsIn;
        this.numRecordsOut = numRecordsOut;
        this.accumulateIdleTime = accumulateIdleTime;
        this.accumulateBusyTime = accumulateBusyTime;
        this.accumulateBackPressuredTime = accumulateBackPressuredTime;
        this.resultPartitionBytes = resultPartitionBytes;
        this.numRecordsOutPerTarget =
                numRecordsOutPerTarget == null
                        ? Collections.emptyMap()
                        : Collections.unmodifiableMap(numRecordsOutPerTarget);
    }

    public long getNumRecordsIn() {
        return numRecordsIn;
    }

    public long getNumRecordsOut() {
        return numRecordsOut;
    }

    public long getNumBytesIn() {
        return numBytesIn;
    }

    public long getNumBytesOut() {
        return numBytesOut;
    }

    public double getAccumulateBusyTime() {
        return accumulateBusyTime;
    }

    public long getAccumulateBackPressuredTime() {
        return accumulateBackPressuredTime;
    }

    public long getAccumulateIdleTime() {
        return accumulateIdleTime;
    }

    public Map<IntermediateResultPartitionID, ResultPartitionBytes> getResultPartitionBytes() {
        return Collections.unmodifiableMap(checkNotNull(resultPartitionBytes));
    }

    /**
     * Returns the per-downstream-target {@code numRecordsOut} breakdown, keyed by target {@code
     * JobVertexID} (hex string). Never {@code null}; returns an empty map when no per-target
     * counters were registered.
     */
    public Map<String, Long> getNumRecordsOutPerTarget() {
        return numRecordsOutPerTarget == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(numRecordsOutPerTarget);
    }
}
