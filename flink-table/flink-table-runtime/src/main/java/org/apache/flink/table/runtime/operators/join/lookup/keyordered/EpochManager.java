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
import org.apache.flink.streaming.api.watermark.Watermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Optional;

/**
 * Epoch manager segments inputs into distinct epochs, marked by the arrival of non-records(e.g.
 * watermark). Records are assigned to a unique epoch based on their arrival.
 */
public class EpochManager<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(EpochManager.class);

    /** The output queue to hold ongoing epochs. */
    private final LinkedList<Epoch<OUT>> outputQueue;

    /** Current active epoch, only one active epoch at the same time. */
    private Epoch<OUT> activeEpoch;

    /** Current watermark in active epoch. */
    private Watermark currentWatermark;

    public EpochManager() {
        this.currentWatermark = new Watermark(Long.MIN_VALUE);
        this.outputQueue = new LinkedList<>();
        this.activeEpoch = new Epoch<>(currentWatermark);
        this.outputQueue.offer(activeEpoch);
    }

    /**
     * Add a record to the current epoch and return the current open epoch.
     *
     * @return the current open epoch.
     */
    public Epoch<OUT> onRecord() {
        activeEpoch.incrementCount();
        return activeEpoch;
    }

    public Optional<Epoch<OUT>> getProperEpoch(Watermark watermark) {
        for (Epoch<OUT> epoch : outputQueue) {
            if (epoch.getWatermark().equals(watermark)) {
                return Optional.of(epoch);
            }
        }
        return Optional.empty();
    }

    /**
     * Add a non-record to the current epoch, close current epoch and open a new epoch.
     *
     * @param watermark the new watermark.
     */
    public void onNonRecord(Watermark watermark, Runnable action) {
        activeEpoch.close(action);
        activeEpoch = new Epoch<>(watermark);
        outputQueue.offer(activeEpoch);
        currentWatermark = watermark;
        tryFinishInQueue();
    }

    /**
     * Complete one record in the specific epoch. Must be invoked within task thread.
     *
     * @param epoch the specific epoch
     */
    public void completeOneRecord(Epoch<OUT> epoch) {
        epoch.decrementCount();
        tryFinishInQueue();
    }

    public void close() {
        for (Epoch<OUT> epoch : outputQueue) {
            epoch.free();
        }
        outputQueue.clear();
    }

    @VisibleForTesting
    public Epoch<OUT> getActiveEpoch() {
        return activeEpoch;
    }

    private void tryFinishInQueue() {
        // If one epoch has been closed before and all records in
        // this epoch have finished, the epoch will be removed from the output queue.
        while (!outputQueue.isEmpty() && outputQueue.peek().tryFinish()) {
            LOG.debug(
                    "Finish epoch: {}, outputQueue size: {}",
                    outputQueue.peek(),
                    outputQueue.size());
            outputQueue.pop();
        }
    }
}
