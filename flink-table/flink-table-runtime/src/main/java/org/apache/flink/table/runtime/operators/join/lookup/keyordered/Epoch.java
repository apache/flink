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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * All inputs are segment into distinct epochs, marked by the arrival of non-record inputs. Records
 * are assigned to a unique epoch based on their arrival.
 */
public class Epoch<OUT> {

    /** The completed ones in this epoch which is not the active epoch. */
    private final Deque<StreamElementQueueEntry<OUT>> outputQueue;

    /** The watermark binding to the epoch. */
    private final Watermark watermark;

    /** The number of records that are still ongoing in this epoch. */
    private int ongoingRecordCount;

    @Nullable private Consumer<StreamElementQueueEntry<OUT>> output;

    /** The action associated with non-record of this epoch(e.g. advance watermark). */
    @Nullable private Runnable advanceWatermark;

    private EpochStatus status;

    public Epoch(Watermark watermark) {
        this.status = EpochStatus.OPEN;
        this.ongoingRecordCount = 0;
        this.advanceWatermark = null;
        this.output = null;
        this.outputQueue = new LinkedList<>();
        this.watermark = watermark;
    }

    /** Add resultFuture to the output queue. */
    public void collect(StreamElementQueueEntry<OUT> resultFuture) {
        outputQueue.add(resultFuture);
    }

    public void setOutput(Consumer<StreamElementQueueEntry<OUT>> outputConsumer) {
        if (output == null) {
            this.output = outputConsumer;
        }
    }

    public void decrementCount() {
        Preconditions.checkState(ongoingRecordCount > 0);
        ongoingRecordCount--;
    }

    public void incrementCount() {
        ongoingRecordCount++;
    }

    public Watermark getWatermark() {
        return watermark;
    }

    /**
     * Try to finish this epoch.
     *
     * @return whether this epoch has been finished.
     */
    boolean tryFinish() {
        if (this.status == EpochStatus.FINISHED) {
            return true;
        }
        while (!outputQueue.isEmpty()) {
            assert output != null;
            output.accept(outputQueue.poll());
        }
        if (ongoingRecordCount == 0 && this.status == EpochStatus.CLOSED) {
            this.status = EpochStatus.FINISHED;
            if (advanceWatermark != null) {
                advanceWatermark.run();
            }
            return true;
        }
        return false;
    }

    /** Close this epoch. */
    public void close(Runnable advanceWatermark) {
        this.advanceWatermark = advanceWatermark;
        this.status = EpochStatus.CLOSED;
    }

    public void free() {
        this.outputQueue.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Epoch<?> other = (Epoch<?>) obj;
        return ongoingRecordCount == other.ongoingRecordCount
                && Objects.equals(watermark, other.watermark)
                && status == other.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(watermark.hashCode(), ongoingRecordCount, status);
    }

    @Override
    public String toString() {
        return String.format(
                "Epoch{watermark=%s, ongoingRecord=%d}", watermark, ongoingRecordCount);
    }

    @VisibleForTesting
    public int getOngoingRecordCount() {
        return ongoingRecordCount;
    }

    /** The status of an epoch. */
    enum EpochStatus {
        /**
         * The subsequent non-record input has not arrived. So arriving records will be collected
         * into current epoch.
         */
        OPEN,
        /**
         * The records belong to this epoch is settled since the following non-record input has
         * arrived, the newly arriving records would be collected into the next epoch.
         */
        CLOSED,
        /**
         * One epoch can only be finished when it meets the following three conditions. 1. The
         * records of this epoch have finished execution. 2. The epoch is closed. 3. The epoch is in
         * the front of outputQueue.
         */
        FINISHED
    }
}
