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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.core.testutils.CheckedThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class waits for {@code expectedRepetitionsOfExpectedRecord} number of occurrences of the
 * {@code expectedRecord}. {@code expectedRepetitionsOfExpectedRecord} is correlated with number of
 * input channels.
 */
public abstract class ReceiverThread extends CheckedThread {
    protected static final Logger LOG = LoggerFactory.getLogger(ReceiverThread.class);

    private final State state;

    ReceiverThread(State state) {
        super(
                () -> {
                    try {
                        while (state.running) {
                            state.readRecords(state.getExpectedRecord().get());
                            state.finishProcessingExpectedRecords();
                        }
                    } catch (InterruptedException e) {
                        if (state.running) {
                            throw e;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

        setName(this.getClass().getName());
        this.state = state;
    }

    public CompletableFuture<?> setExpectedRecord(long record) {
        return state.setExpectedRecord(record);
    }

    public void shutdown() {
        state.running = false;
        interrupt();
        state.expectedRecord.complete(0L);
    }

    /** Maintain the states of ReceiverThread. */
    protected abstract static class State {
        protected final int expectedRepetitionsOfExpectedRecord;

        protected int expectedRecordCounter;
        protected CompletableFuture<Long> expectedRecord = new CompletableFuture<>();
        protected CompletableFuture<?> recordsProcessed = new CompletableFuture<>();

        protected volatile boolean running;

        protected State(int expectedRepetitionsOfExpectedRecord) {
            this.expectedRepetitionsOfExpectedRecord = expectedRepetitionsOfExpectedRecord;
            this.running = true;
        }

        public synchronized CompletableFuture<?> setExpectedRecord(long record) {
            checkState(!expectedRecord.isDone());
            checkState(!recordsProcessed.isDone());
            expectedRecord.complete(record);
            expectedRecordCounter = 0;
            return recordsProcessed;
        }

        private synchronized CompletableFuture<Long> getExpectedRecord() {
            return expectedRecord;
        }

        protected abstract void readRecords(long lastExpectedRecord) throws Exception;

        private synchronized void finishProcessingExpectedRecords() {
            checkState(expectedRecord.isDone());
            checkState(!recordsProcessed.isDone());

            recordsProcessed.complete(null);
            expectedRecord = new CompletableFuture<>();
            recordsProcessed = new CompletableFuture<>();
        }
    }
}
