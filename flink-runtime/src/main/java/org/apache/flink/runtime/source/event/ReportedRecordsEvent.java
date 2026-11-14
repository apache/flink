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
 * Reports the number of records a subtask has produced since its last snapshot, and the time that
 * took, to the {@link org.apache.flink.runtime.source.coordinator.SourceCoordinator}. It divides
 * one by the other to get the actually observed throughput, rather than relying on the last
 * broadcast target rate, as the basis for computing the next dynamic rate limit.
 */
public class ReportedRecordsEvent implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;
    private final long numRecords;
    private final long elapsedMillis;

    /**
     * @param checkpointId the checkpoint whose trigger ends the reported period.
     * @param numRecords the records produced since the last snapshot.
     * @param elapsedMillis the time from the trigger of the checkpoint of the last snapshot (or
     *     from the start of the subtask if there is none) to the trigger of this checkpoint.
     */
    public ReportedRecordsEvent(long checkpointId, long numRecords, long elapsedMillis) {
        this.checkpointId = checkpointId;
        this.numRecords = numRecords;
        this.elapsedMillis = elapsedMillis;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    /**
     * @return true, because a lost report is superseded by the one sent for the next checkpoint and
     *     doesn't hold any critical information.
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
        ReportedRecordsEvent that = (ReportedRecordsEvent) o;
        return checkpointId == that.checkpointId
                && numRecords == that.numRecords
                && elapsedMillis == that.elapsedMillis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointId, numRecords, elapsedMillis);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{checkpointId="
                + checkpointId
                + ", numRecords="
                + numRecords
                + ", elapsedMillis="
                + elapsedMillis
                + '}';
    }
}
