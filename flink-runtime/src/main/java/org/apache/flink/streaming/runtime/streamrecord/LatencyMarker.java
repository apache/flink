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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.jobgraph.OperatorID;

/**
 * Special record type carrying a timestamp of its creation time at a source operator and the
 * vertexId and subtask index of the operator.
 *
 * <p>At sinks, the marker can be used to approximate the time a record needs to travel through the
 * dataflow.
 */
@PublicEvolving
public final class LatencyMarker extends StreamElement {

    // ------------------------------------------------------------------------

    /** The time the latency mark is denoting. */
    private final long markedTime;

    private final OperatorID operatorId;

    private final int subtaskIndex;

    /** Creates a latency mark with the given timestamp. */
    public LatencyMarker(long markedTime, OperatorID operatorId, int subtaskIndex) {
        this.markedTime = markedTime;
        this.operatorId = operatorId;
        this.subtaskIndex = subtaskIndex;
    }

    /** Returns the timestamp marked by the LatencyMarker. */
    public long getMarkedTime() {
        return markedTime;
    }

    public OperatorID getOperatorId() {
        return operatorId;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LatencyMarker that = (LatencyMarker) o;

        if (markedTime != that.markedTime) {
            return false;
        }
        if (!operatorId.equals(that.operatorId)) {
            return false;
        }
        return subtaskIndex == that.subtaskIndex;
    }

    @Override
    public int hashCode() {
        int result = (int) (markedTime ^ (markedTime >>> 32));
        result = 31 * result + operatorId.hashCode();
        result = 31 * result + subtaskIndex;
        return result;
    }

    @Override
    public String toString() {
        return "LatencyMarker{"
                + "markedTime="
                + markedTime
                + ", operatorId="
                + operatorId
                + ", subtaskIndex="
                + subtaskIndex
                + '}';
    }
}
