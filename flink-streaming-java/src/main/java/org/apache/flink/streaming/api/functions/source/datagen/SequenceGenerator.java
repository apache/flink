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

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A stateful, re-scalable {@link DataGenerator} that emits each number from a given interval
 * exactly once, possibly in parallel.
 */
@Experimental
public abstract class SequenceGenerator<T> implements DataGenerator<T> {

    private final long start;
    private final long end;
    private long totalNoOfElements;
    /**
     * Save the intermediate state of the data to be sent by the current subtask,When the state
     * returns, the sequence values continue to be sent based on the intermediate state
     */
    private ArrayList<InternalState> internalStates;

    private transient ListState<InternalState> checkpointedState;

    /**
     * Creates a DataGenerator that emits all numbers from the given interval exactly once.
     *
     * @param start Start of the range of numbers to emit.
     * @param end End of the range of numbers to emit.
     */
    public SequenceGenerator(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        Preconditions.checkState(
                this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        ListStateDescriptor<InternalState> stateDescriptor =
                new ListStateDescriptor<>(
                        name + "-sequence-state", TypeInformation.of(InternalState.class));
        this.checkpointedState = context.getOperatorStateStore().getListState(stateDescriptor);
        this.internalStates = Lists.newArrayList();

        totalNoOfElements = Math.abs(end - start + 1);
        if (context.isRestored()) {
            checkpointedState.get().forEach(state -> internalStates.add(state));
        } else {
            // the first time the job is executed
            final int taskIdx = runtimeContext.getTaskInfo().getIndexOfThisSubtask();
            final long stepSize = runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();
            internalStates.add(new InternalState(0, taskIdx, stepSize));
        }
    }

    private long toCollect(long baseSize, long stepSize, int taskIdx) {
        return (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;
    }

    public Long nextValue() {
        Iterator<InternalState> iterator = internalStates.iterator();
        if (iterator.hasNext()) {
            InternalState state = iterator.next();
            long nextSequence = state.collected * state.stepSize + (start + state.taskId);
            state.collected++;
            // All sequence values are cleared from the stateList after they have been sent
            if (state.collected
                    >= toCollect(
                            safeDivide(totalNoOfElements, state.stepSize),
                            state.stepSize,
                            state.taskId)) {
                iterator.remove();
            }
            return nextSequence;
        }

        // Before calling this method, you should call hasNext to check
        throw new IllegalStateException();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(
                this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.addAll(internalStates);
    }

    @Override
    public boolean hasNext() {
        return !internalStates.isEmpty();
    }

    private static long safeDivide(long left, long right) {
        Preconditions.checkArgument(right > 0);
        Preconditions.checkArgument(left >= 0);
        Preconditions.checkArgument(left <= Integer.MAX_VALUE * right);
        return left / right;
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

    private static class InternalState {
        long collected;
        int taskId;
        long stepSize;

        public InternalState(long collected, int taskId, long stepSize) {
            this.collected = collected;
            this.taskId = taskId;
            this.stepSize = stepSize;
        }
    }
}
