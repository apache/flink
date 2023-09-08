/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/** Source for window checkpointing IT cases that can introduce artificial failures. */
public class FailingSource extends RichSourceFunction<Tuple2<Long, IntType>>
        implements ListCheckpointed<Integer>, CheckpointListener {

    /** Function to generate and emit the test events (and watermarks if required). */
    @FunctionalInterface
    public interface EventEmittingGenerator extends Serializable {
        void emitEvent(SourceContext<Tuple2<Long, IntType>> ctx, int eventSequenceNo);
    }

    private static final long INITIAL = Long.MIN_VALUE;
    private static final long STATEFUL_CHECKPOINT_COMPLETED = Long.MIN_VALUE;

    @Nonnull private final EventEmittingGenerator eventEmittingGenerator;
    private final int expectedEmitCalls;
    private final int failureAfterNumElements;
    private final boolean usingProcessingTime;
    private final AtomicLong checkpointStatus;

    private int emitCallCount;
    private volatile boolean running;

    public FailingSource(
            @Nonnull EventEmittingGenerator eventEmittingGenerator,
            @Nonnegative int numberOfGeneratorInvocations) {
        this(eventEmittingGenerator, numberOfGeneratorInvocations, false);
    }

    public FailingSource(
            @Nonnull EventEmittingGenerator eventEmittingGenerator,
            @Nonnegative int numberOfGeneratorInvocations,
            boolean usingProcessingTime) {
        this.eventEmittingGenerator = eventEmittingGenerator;
        this.running = true;
        this.emitCallCount = 0;
        this.expectedEmitCalls = numberOfGeneratorInvocations;
        this.failureAfterNumElements = numberOfGeneratorInvocations / 2;
        this.checkpointStatus = new AtomicLong(INITIAL);
        this.usingProcessingTime = usingProcessingTime;
    }

    @Override
    public void open(OpenContext openContext) {
        // non-parallel source
        assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public void run(SourceContext<Tuple2<Long, IntType>> ctx) throws Exception {

        final RuntimeContext runtimeContext = getRuntimeContext();
        // detect if this task is "the chosen one" and should fail (via subtaskidx), if it did not
        // fail before (via attempt)
        final boolean failThisTask =
                runtimeContext.getAttemptNumber() == 0
                        && runtimeContext.getIndexOfThisSubtask() == 0;

        // we loop longer than we have elements, to permit delayed checkpoints
        // to still cause a failure
        while (running && emitCallCount < expectedEmitCalls) {

            // the function failed before, or we are in the elements before the failure
            synchronized (ctx.getCheckpointLock()) {
                eventEmittingGenerator.emitEvent(ctx, emitCallCount++);
            }

            if (emitCallCount < failureAfterNumElements) {
                Thread.sleep(1);
            } else if (failThisTask && emitCallCount == failureAfterNumElements) {
                // wait for a pending checkpoint that fulfills our requirements if needed
                while (checkpointStatus.get() != STATEFUL_CHECKPOINT_COMPLETED) {
                    Thread.sleep(1);
                }
                throw new Exception("Artificial Failure");
            }
        }

        if (usingProcessingTime) {
            while (running) {
                Thread.sleep(10);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // This will unblock the task for failing, if this is the checkpoint we are waiting for
        checkpointStatus.compareAndSet(checkpointId, STATEFUL_CHECKPOINT_COMPLETED);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        // We accept a checkpoint as basis if it should have a "decent amount" of state
        if (emitCallCount > failureAfterNumElements / 2) {
            // This means we are waiting for notification of this checkpoint to completed now.
            checkpointStatus.compareAndSet(INITIAL, checkpointId);
        }
        return Collections.singletonList(this.emitCallCount);
    }

    @Override
    public void restoreState(List<Integer> state) throws Exception {
        if (state.isEmpty() || state.size() > 1) {
            throw new RuntimeException(
                    "Test failed due to unexpected recovered state size " + state.size());
        }
        this.emitCallCount = state.get(0);
    }
}
