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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * A stateful streaming source that emits each number from a given interval exactly once, possibly
 * in parallel.
 *
 * <p>For the source to be re-scalable, the first time the job is run, we precompute all the
 * elements that each of the tasks should emit and upon checkpointing, each element constitutes its
 * own partition. When rescaling, these partitions will be randomly re-assigned to the new tasks.
 *
 * <p>This strategy guarantees that each element will be emitted exactly-once, but elements will not
 * necessarily be emitted in ascending order, even for the same tasks.
 *
 * <p>NOTE: this source will be removed together with the deprecated
 * StreamExecutionEnvironmetn#generateSequence() method.
 *
 * @deprecated This class is based on the {@link
 *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
 *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
 */
@Deprecated
@PublicEvolving
public class StatefulSequenceSource extends RichParallelSourceFunction<Long>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;

    private volatile boolean isRunning = true;

    private transient Deque<Long> valuesToEmit;

    private transient ListState<Long> checkpointedState;

    /**
     * Creates a source that emits all numbers from the given interval exactly once.
     *
     * @param start Start of the range of numbers to emit.
     * @param end End of the range of numbers to emit.
     */
    public StatefulSequenceSource(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "stateful-sequence-source-state", LongSerializer.INSTANCE));

        this.valuesToEmit = new ArrayDeque<>();
        if (context.isRestored()) {
            // upon restoring

            for (Long v : this.checkpointedState.get()) {
                this.valuesToEmit.add(v);
            }
        } else {
            // the first time the job is executed

            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            final long congruence = start + taskIdx;

            long totalNoOfElements = Math.abs(end - start + 1);
            final int baseSize = safeDivide(totalNoOfElements, stepSize);
            final int toCollect =
                    (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;

            for (long collected = 0; collected < toCollect; collected++) {
                this.valuesToEmit.add(collected * stepSize + congruence);
            }
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && !this.valuesToEmit.isEmpty()) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(this.valuesToEmit.poll());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.update(new ArrayList<>(valuesToEmit));
    }

    private static int safeDivide(long left, long right) {
        Preconditions.checkArgument(right > 0);
        Preconditions.checkArgument(left >= 0);
        Preconditions.checkArgument(left <= Integer.MAX_VALUE * right);
        return (int) (left / right);
    }
}
