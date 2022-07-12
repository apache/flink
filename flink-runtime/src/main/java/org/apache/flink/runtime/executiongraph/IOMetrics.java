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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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

    protected final Map<IntermediateResultPartitionID, Long> numBytesProducedOfPartitions =
            new HashMap<>();

    public IOMetrics(
            Meter recordsIn,
            Meter recordsOut,
            Meter bytesIn,
            Meter bytesOut,
            Map<IntermediateResultPartitionID, Counter> numBytesProducedCounters,
            Gauge<Long> accumulatedBackPressuredTime,
            Gauge<Long> accumulatedIdleTime,
            Gauge<Double> accumulatedBusyTime) {
        this.numRecordsIn = recordsIn.getCount();
        this.numRecordsOut = recordsOut.getCount();
        this.numBytesIn = bytesIn.getCount();
        this.numBytesOut = bytesOut.getCount();
        this.accumulateBackPressuredTime = accumulatedBackPressuredTime.getValue();
        this.accumulateBusyTime = accumulatedBusyTime.getValue();
        this.accumulateIdleTime = accumulatedIdleTime.getValue();

        for (Map.Entry<IntermediateResultPartitionID, Counter> counter :
                numBytesProducedCounters.entrySet()) {
            numBytesProducedOfPartitions.put(counter.getKey(), counter.getValue().getCount());
        }
    }

    public IOMetrics(
            long numBytesIn,
            long numBytesOut,
            long numRecordsIn,
            long numRecordsOut,
            long accumulateIdleTime,
            long accumulateBusyTime,
            long accumulateBackPressuredTime) {
        this.numBytesIn = numBytesIn;
        this.numBytesOut = numBytesOut;
        this.numRecordsIn = numRecordsIn;
        this.numRecordsOut = numRecordsOut;
        this.accumulateIdleTime = accumulateIdleTime;
        this.accumulateBusyTime = accumulateBusyTime;
        this.accumulateBackPressuredTime = accumulateBackPressuredTime;
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

    public Map<IntermediateResultPartitionID, Long> getNumBytesProducedOfPartitions() {
        return numBytesProducedOfPartitions;
    }
}
