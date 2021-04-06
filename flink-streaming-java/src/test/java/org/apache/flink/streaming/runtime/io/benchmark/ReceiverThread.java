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

    protected final int expectedRepetitionsOfExpectedRecord;

    protected int expectedRecordCounter;
    protected CompletableFuture<Long> expectedRecord = new CompletableFuture<>();
    protected CompletableFuture<?> recordsProcessed = new CompletableFuture<>();

    protected volatile boolean running;

    ReceiverThread(int expectedRepetitionsOfExpectedRecord) {
        setName(this.getClass().getName());

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

    private synchronized void finishProcessingExpectedRecords() {
        checkState(expectedRecord.isDone());
        checkState(!recordsProcessed.isDone());

        recordsProcessed.complete(null);
        expectedRecord = new CompletableFuture<>();
        recordsProcessed = new CompletableFuture<>();
    }

    @Override
    public void go() throws Exception {
        try {
            while (running) {
                readRecords(getExpectedRecord().get());
                finishProcessingExpectedRecords();
            }
        } catch (InterruptedException e) {
            if (running) {
                throw e;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract void readRecords(long lastExpectedRecord) throws Exception;

    public void shutdown() {
        running = false;
        interrupt();
        expectedRecord.complete(0L);
    }
}
