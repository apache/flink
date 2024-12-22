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

    /**
     * The epoch that is possibly in finishing status, the following new records should share this
     * epoch.
     */
    @Nullable Epoch finishingEpoch;

    /** The flag that prevent the recursive call of {@link #tryFinishInQueue()}. */
    boolean recursiveFlag;

    public EpochManager(AsyncExecutionController<?> aec) {
        this.epochNum = 0;
        this.outputQueue = new LinkedList<>();
        this.asyncExecutionController = aec;
        // init an empty epoch, the epoch action will be updated when non-record is received.
        this.activeEpoch = new Epoch(epochNum++);
        this.finishingEpoch = null;
        this.recursiveFlag = false;
    }

    /**
     * Add a record to the current epoch and return the current open epoch, the epoch will be
     * associated with the {@link RecordContext} of this record. Must be invoked within task thread.
     *
     * @return the current open epoch.
     */
    public Epoch onRecord() {
        if (finishingEpoch != null) {
            finishingEpoch.ongoingRecordCount++;
            return finishingEpoch;
        } else {
            activeEpoch.ongoingRecordCount++;
            return activeEpoch;
        }
    }

    /**
     * Add a record to a specified epoch.
     *
     * @param epoch the specified epoch.
     * @return the specified epoch itself.
     */
    public Epoch onEpoch(Epoch epoch) {
        epoch.ongoingRecordCount++;
        return epoch;
    }

    /**
     * Add a non-record to the current epoch, close current epoch and open a new epoch. Must be
     * invoked within task thread.
     *
     * @param triggerAction the action associated with this non-record.
     * @param parallelMode the parallel mode for this epoch.
     */
    public void onNonRecord(
            @Nullable Runnable triggerAction,
            @Nullable Runnable finalAction,
            ParallelMode parallelMode) {
        LOG.trace(
                "on NonRecord, old epoch: {}, outputQueue size: {}",
                activeEpoch,
                outputQueue.size());
        switchActiveEpoch(triggerAction, finalAction);
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
        // Only the epoch that is not active can trigger finish.
        if (--epoch.ongoingRecordCount == 0 && epoch != activeEpoch) {
            tryFinishInQueue();
        }
    }

    private void tryFinishInQueue() {
        // We don't permit recursive call of this method.
        if (recursiveFlag) {
            return;
        }
        recursiveFlag = true;
        // If one epoch has been closed before and all records in
        // this epoch have finished, the epoch will be removed from the output queue.
        while (!outputQueue.isEmpty()) {
            Epoch epoch = outputQueue.peek();
            // The epoch is set for inheritance during possible trigger action.
            finishingEpoch = epoch;
            try {
                if (epoch.tryFinish()) {
                    outputQueue.pop();
                } else {
                    break;
                }
            } finally {
                // Clear the override
                finishingEpoch = null;
            }
        }
        recursiveFlag = false;
    }

    private void switchActiveEpoch(
            @Nullable Runnable triggerAction, @Nullable Runnable finalAction) {
        activeEpoch.close(triggerAction, finalAction);
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
         * the front of outputQueue. When the status transit from {@link #CLOSED} to {@link
         * #FINISHING}, a trigger action will go.
         */
        FINISHING,

        /**
         * After the action is triggered, there might be more async process bind to this epoch.
         * After all these process finished, a final action will go and the epoch will fall into
         * {@link #FINISHED} status.
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

        /** The action associated with non-record of this epoch(e.g. triggering timer). */
        @Nullable Runnable triggerAction;

        /**
         * The action when we finish this epoch and the triggerAction as well as any async
         * processing.
         */
        @Nullable Runnable finalAction;

        EpochStatus status;

        public Epoch(long id) {
            this.id = id;
            this.ongoingRecordCount = 0;
            this.status = EpochStatus.OPEN;
            this.triggerAction = null;
            this.finalAction = null;
        }

        /**
         * Try to finish this epoch. This is the core logic of triggering actions. The state machine
         * and timeline are as follows:
         *
         * <pre>
         * Action:     close()       triggerAction       wait             finalAction
         * Statue:  OPEN ----- CLOSED ----------FINISHING -------- FINISHED -----------
         * </pre>
         *
         * @return whether this epoch has been normally finished.
         */
        boolean tryFinish() {
            if (ongoingRecordCount == 0) {
                if (status == EpochStatus.CLOSED) {
                    // CLOSED -> FINISHING
                    transition(EpochStatus.FINISHING);
                    if (triggerAction != null) {
                        // trigger action will use {@link overrideEpoch}.
                        triggerAction.run();
                    }
                }
                // After the triggerAction run, if there is no new async process, the
                // ongoingRecordCount remains 0, then the status should transit to FINISHED.
                // Otherwise, we will reach here when ongoingRecordCount reaches 0 again.
                if (ongoingRecordCount == 0 && status == EpochStatus.FINISHING) {
                    // FINISHING -> FINISHED
                    transition(EpochStatus.FINISHED);
                    if (finalAction != null) {
                        finalAction.run();
                    }
                }
                return status == EpochStatus.FINISHED;
            }
            return false;
        }

        void transition(EpochStatus newStatus) {
            if (status != newStatus) {
                LOG.trace("Epoch {} transit from {} to {}", this, status, newStatus);
                status = newStatus;
            }
        }

        /** Close this epoch with defined triggerAction and finalAction. */
        void close(@Nullable Runnable triggerAction, @Nullable Runnable finalAction) {
            this.triggerAction = triggerAction;
            this.finalAction = finalAction;
            transition(EpochStatus.CLOSED);
        }

        public String toString() {
            return String.format(
                    "Epoch{id=%d, ongoingRecord=%d, status=%s}", id, ongoingRecordCount, status);
        }
    }
}
