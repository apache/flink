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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.Function;

/**
 * Each of the source operators outputs records in given number of checkpoints. Number of records
 * per checkpoint is constant between checkpoints, and defined by user. When all records are
 * emitted, the source waits for two more checkpoints until it finishes.
 *
 * <p>Main credits for this implementation should go to <b>Grzegorz
 * Kolakowski/https://github.com/grzegorz8</b> who implemented the original version of this class
 * for Delta-Flink connector.
 */
public class CheckpointCountingSource<OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointListener, CheckpointedFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointCountingSource.class);

    private final int numberOfCheckpoints;
    private final int recordsPerCheckpoint;
    private final Function<Integer, OUT> elementProducer;

    private ListState<Integer> nextValueState;
    private int nextValue;
    private volatile boolean isCanceled;
    private volatile boolean waitingForCheckpoint;

    public CheckpointCountingSource(
            int recordsPerCheckpoint,
            int numberOfCheckpoints,
            Function<Integer, OUT> elementProducer) {
        this.numberOfCheckpoints = numberOfCheckpoints;
        this.recordsPerCheckpoint = recordsPerCheckpoint;
        this.elementProducer = elementProducer;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        nextValueState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

        if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
            nextValue = nextValueState.get().iterator().next();
        }
        waitingForCheckpoint = false;
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        LOGGER.info(
                "Run subtask={}; attempt={}.",
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getAttemptNumber());

        sendRecordsUntil(numberOfCheckpoints, ctx);
        idleUntilNextCheckpoint(ctx);
        LOGGER.info("Source task done; subtask={}.", getRuntimeContext().getIndexOfThisSubtask());
    }

    private void sendRecordsUntil(int targetCheckpoints, SourceContext<OUT> ctx)
            throws InterruptedException {
        while (!isCanceled && nextValue < targetCheckpoints * recordsPerCheckpoint) {
            synchronized (ctx.getCheckpointLock()) {
                emitRecordsBatch(recordsPerCheckpoint, ctx);
                waitingForCheckpoint = true;
            }
            LOGGER.info(
                    "Waiting for checkpoint to complete; subtask={}.",
                    getRuntimeContext().getIndexOfThisSubtask());
            while (waitingForCheckpoint) {
                Thread.sleep(100);
            }
        }
    }

    private void emitRecordsBatch(int batchSize, SourceContext<OUT> ctx) {
        for (int i = 0; i < batchSize; ++i) {
            OUT row = elementProducer.apply(nextValue++);
            ctx.collect(row);
        }

        LOGGER.info(
                "Emitted {} records (total {}); subtask={}.",
                batchSize,
                nextValue,
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void idleUntilNextCheckpoint(SourceContext<OUT> ctx) throws InterruptedException {
        // Idle until the next checkpoint completes to avoid any premature job termination and
        // race conditions.
        LOGGER.info(
                "Waiting for an additional checkpoint to complete; subtask={}.",
                getRuntimeContext().getIndexOfThisSubtask());
        synchronized (ctx.getCheckpointLock()) {
            waitingForCheckpoint = true;
        }
        while (waitingForCheckpoint) {
            Thread.sleep(100);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        nextValueState.update(Collections.singletonList(nextValue));
        LOGGER.info(
                "state snapshot done; checkpointId={}; subtask={}.",
                context.getCheckpointId(),
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        waitingForCheckpoint = false;
        LOGGER.info(
                "Checkpoint {} complete; subtask={}.",
                checkpointId,
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        LOGGER.info(
                "Checkpoint {} aborted; subtask={}.",
                checkpointId,
                getRuntimeContext().getIndexOfThisSubtask());
        CheckpointListener.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void cancel() {
        isCanceled = true;
    }
}
