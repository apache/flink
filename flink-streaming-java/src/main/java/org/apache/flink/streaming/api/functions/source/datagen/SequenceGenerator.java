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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.Queues;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A stateful, re-scalable {@link DataGenerator} that emits each number from a given interval
 * exactly once, possibly in parallel.
 *
 * <p>It maintains a state internally to record the position of the current subtask sending
 * sequence. When the task resumes, it will continue to send the sequence value according to the
 * position sent by the state, until all the sequences have been sent.
 *
 * <p><b>IMPORTANT NOTE: </b> When the degree of parallelism increases, there may be cases where
 * subtasks are running empty. When the degree of parallelism decreases, there may be cases where
 * one subtask handles multiple states.
 */
@Experimental
public abstract class SequenceGenerator<T> implements DataGenerator<T> {

    private final long start;
    private final long end;
    /**
     * Save the intermediate state of the data to be sent by the current subtask, when the state
     * restore, the sequence values continue to be sent based on the intermediate state.
     */
    private transient Queue<SubTaskState> subTaskStates;

    private transient ListState<SubTaskState> checkpointedState;

    /**
     * Creates a DataGenerator that emits all numbers from the given interval exactly once.
     *
     * @param inclStart Start of the range of numbers to emit.
     * @param inclEnd End of the range of numbers to emit.
     */
    public SequenceGenerator(long inclStart, long inclEnd) {
        Preconditions.checkArgument(
                inclEnd > inclStart,
                "The start value (%s) cannot be greater than the end value (%s).",
                inclStart,
                inclEnd);
        Preconditions.checkArgument(
                inclEnd - inclStart <= Long.MAX_VALUE - 1,
                "The total size of range (%s, %s) exceeds the maximum limit: Long.MAX_VALUE - 1.",
                inclStart,
                inclEnd);
        this.start = inclStart;
        this.end = inclEnd;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        final ListStateDescriptor<SubTaskState> stateDescriptor =
                new ListStateDescriptor<>(
                        name + "-sequence-state", TypeInformation.of(SubTaskState.class));
        this.checkpointedState = context.getOperatorStateStore().getListState(stateDescriptor);

        this.subTaskStates = Queues.newPriorityQueue();

        if (context.isRestored()) {
            checkpointedState.get().forEach(subTaskStates::offer);
        } else {
            // The first time the job is executed.
            final int startOffset = runtimeContext.getTaskInfo().getIndexOfThisSubtask();
            final long stepSize = runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();
            final SubTaskState state = new SubTaskState(stepSize, start + startOffset);
            subTaskStates.offer(state);
        }
    }

    public Long nextValue() {
        if (subTaskStates.isEmpty()) {
            throw new NoSuchElementException(
                    "SequenceGenerator.nextValue() was called with no remaining values.");
        }

        final SubTaskState state = subTaskStates.poll();
        final long currentValue = state.nextValue;

        try {
            state.nextValue = Math.addExact(currentValue, state.stepSize);
            // All sequence values are cleared from the state after they are sent.
            if (state.nextValue <= this.end) {
                subTaskStates.offer(state);
            }
        } catch (ArithmeticException e) {
            // When it overflows, it means that all the data has been sent and needs to be
            // cleared from the state.
        }

        return currentValue;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.update(new ArrayList<>(subTaskStates));
    }

    @Override
    public boolean hasNext() {
        return !subTaskStates.isEmpty();
    }

    public static SequenceGenerator<Long> longGenerator(long start, long end) {
        return new SequenceGenerator<Long>(start, end) {
            @Override
            public Long next() {
                return nextValue();
            }
        };
    }

    public static SequenceGenerator<Integer> intGenerator(int start, int end) {
        return new SequenceGenerator<Integer>(start, end) {
            @Override
            public Integer next() {
                return nextValue().intValue();
            }
        };
    }

    public static SequenceGenerator<Short> shortGenerator(short start, short end) {
        return new SequenceGenerator<Short>(start, end) {
            @Override
            public Short next() {
                return nextValue().shortValue();
            }
        };
    }

    public static SequenceGenerator<Byte> byteGenerator(byte start, byte end) {
        return new SequenceGenerator<Byte>(start, end) {
            @Override
            public Byte next() {
                return nextValue().byteValue();
            }
        };
    }

    public static SequenceGenerator<Float> floatGenerator(short start, short end) {
        return new SequenceGenerator<Float>(start, end) {
            @Override
            public Float next() {
                return nextValue().floatValue();
            }
        };
    }

    public static SequenceGenerator<Double> doubleGenerator(int start, int end) {
        return new SequenceGenerator<Double>(start, end) {
            @Override
            public Double next() {
                return nextValue().doubleValue();
            }
        };
    }

    public static SequenceGenerator<BigDecimal> bigDecimalGenerator(
            int start, int end, int precision, int scale) {
        return new SequenceGenerator<BigDecimal>(start, end) {
            @Override
            public BigDecimal next() {
                BigDecimal decimal =
                        new BigDecimal(nextValue().doubleValue(), new MathContext(precision));
                return decimal.setScale(scale, RoundingMode.DOWN);
            }
        };
    }

    public static SequenceGenerator<String> stringGenerator(long start, long end) {
        return new SequenceGenerator<String>(start, end) {
            @Override
            public String next() {
                return nextValue().toString();
            }
        };
    }

    /**
     * The internal state of the sequence generator's subtask(s), which is used to record the latest
     * state of the sequence value sent by the current sequence generator. When recovering from the
     * state, it is guaranteed to continue sending the sequence value from the latest state.
     */
    private static class SubTaskState implements Comparable<SubTaskState> {
        long stepSize;
        long nextValue;

        public SubTaskState(long stepSize, long nextValue) {
            this.stepSize = stepSize;
            this.nextValue = nextValue;
        }

        @Override
        public int compareTo(SubTaskState subTaskState) {
            return Long.compare(nextValue, subTaskState.nextValue);
        }
    }
}
