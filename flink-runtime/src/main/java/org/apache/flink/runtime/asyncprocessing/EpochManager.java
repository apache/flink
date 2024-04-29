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

package org.apache.flink.runtime.asyncprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.LinkedList;

/**
 * Epoch manager segments inputs into distinct epochs, marked by the arrival of non-records(e.g.
 * watermark, record attributes). Records are assigned to a unique epoch based on their arrival,
 * records within an epoch are allowed to be parallelized, while the non-record of an epoch can only
 * be executed when all records in this epoch have finished.
 *
 * <p>For more details please refer to FLIP-425.
 */
public class EpochManager {
    private static final Logger LOG = LoggerFactory.getLogger(EpochManager.class);

    /**
     * This enum defines whether parallel execution between epochs is allowed. We should keep this
     * internal and away from API module for now, until we could see the concrete need for {@link
     * #PARALLEL_BETWEEN_EPOCH} from average users.
     */
    public enum ParallelMode {
        /**
         * Subsequent epochs must wait until the previous epoch is completed before they can start.
         */
        SERIAL_BETWEEN_EPOCH,
        /**
         * Subsequent epochs can begin execution even if the previous epoch has not yet completed.
         * Usually performs better than {@link #SERIAL_BETWEEN_EPOCH}.
         */
        PARALLEL_BETWEEN_EPOCH
    }

    /**
     * The reference to the {@link AsyncExecutionController}, used for {@link
     * ParallelMode#SERIAL_BETWEEN_EPOCH}. Can be null when testing.
     */
    final AsyncExecutionController<?> asyncExecutionController;

    /** The number of epochs that have arrived. */
    long epochNum;

    /** The output queue to hold ongoing epochs. */
    LinkedList<Epoch> outputQueue;

    /** Current active epoch, only one active epoch at the same time. */
    Epoch activeEpoch;

    public EpochManager(AsyncExecutionController<?> aec) {
        this.epochNum = 0;
        this.outputQueue = new LinkedList<>();
        this.asyncExecutionController = aec;
        // init an empty epoch, the epoch action will be updated when non-record is received.
        this.activeEpoch = new Epoch(epochNum++);
    }

    /**
     * Add a record to the current epoch and return the current open epoch, the epoch will be
     * associated with the {@link RecordContext} of this record. Must be invoked within task thread.
     *
     * @return the current open epoch.
     */
    public Epoch onRecord() {
        activeEpoch.ongoingRecordCount++;
        return activeEpoch;
    }

    /**
     * Add a non-record to the current epoch, close current epoch and open a new epoch. Must be
     * invoked within task thread.
     *
     * @param action the action associated with this non-record.
     * @param parallelMode the parallel mode for this epoch.
     */
    public void onNonRecord(Runnable action, ParallelMode parallelMode) {
        LOG.trace(
                "on NonRecord, old epoch: {}, outputQueue size: {}",
                activeEpoch,
                outputQueue.size());
        switchActiveEpoch(action);
        if (parallelMode == ParallelMode.SERIAL_BETWEEN_EPOCH) {
            asyncExecutionController.drainInflightRecords(0);
        }
    }

    /**
     * Complete one record in the specific epoch. Must be invoked within task thread.
     *
     * @param epoch the specific epoch
     */
    public void completeOneRecord(Epoch epoch) {
        if (--epoch.ongoingRecordCount == 0) {
            tryFinishInQueue();
        }
    }

    private void tryFinishInQueue() {
        // If one epoch has been closed before and all records in
        // this epoch have finished, the epoch will be removed from the output queue.
        while (!outputQueue.isEmpty() && outputQueue.peek().tryFinish()) {
            LOG.trace(
                    "Finish epoch: {}, outputQueue size: {}",
                    outputQueue.peek(),
                    outputQueue.size());
            outputQueue.pop();
        }
    }

    private void switchActiveEpoch(Runnable action) {
        activeEpoch.close(action);
        outputQueue.offer(activeEpoch);
        this.activeEpoch = new Epoch(epochNum++);
        tryFinishInQueue();
    }

    /** The status of an epoch, see Fig.6 in FLIP-425 for details. */
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

    /**
     * All inputs are segment into distinct epochs, marked by the arrival of non-record inputs.
     * Records are assigned to a unique epoch based on their arrival.
     */
    public static class Epoch {
        /** The id of this epoch for easy debugging. */
        long id;
        /** The number of records that are still ongoing in this epoch. */
        int ongoingRecordCount;

        /** The action associated with non-record of this epoch(e.g. advance watermark). */
        @Nullable Runnable action;

        EpochStatus status;

        public Epoch(long id) {
            this.id = id;
            this.ongoingRecordCount = 0;
            this.status = EpochStatus.OPEN;
            this.action = null;
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
            if (ongoingRecordCount == 0 && this.status == EpochStatus.CLOSED) {
                this.status = EpochStatus.FINISHED;
                if (action != null) {
                    action.run();
                }
                return true;
            }
            return false;
        }

        /** Close this epoch. */
        void close(Runnable action) {
            this.action = action;
            this.status = EpochStatus.CLOSED;
        }

        public String toString() {
            return String.format(
                    "Epoch{id=%d, ongoingRecord=%d, status=%s}", id, ongoingRecordCount, status);
        }
    }
}
