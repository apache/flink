/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** A utility class containing common functions/classes used by multiple migration tests. */
public class MigrationTestUtils {

    /** A non-parallel source with list state used for testing. */
    public static class CheckpointingNonParallelSourceWithListState
            implements SourceFunction<Tuple2<Long, Long>>, CheckpointedFunction {

        static final ListStateDescriptor<String> STATE_DESCRIPTOR =
                new ListStateDescriptor<>("source-state", StringSerializer.INSTANCE);

        static final String CHECKPOINTED_STRING = "Here be dragons!";
        static final String CHECKPOINTED_STRING_1 = "Here be more dragons!";
        static final String CHECKPOINTED_STRING_2 = "Here be yet more dragons!";
        static final String CHECKPOINTED_STRING_3 = "Here be the mostest dragons!";

        private static final long serialVersionUID = 1L;

        private volatile boolean isRunning = true;

        private final int numElements;

        private transient ListState<String> unionListState;

        public CheckpointingNonParallelSourceWithListState(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            unionListState.clear();
            unionListState.add(CHECKPOINTED_STRING);
            unionListState.add(CHECKPOINTED_STRING_1);
            unionListState.add(CHECKPOINTED_STRING_2);
            unionListState.add(CHECKPOINTED_STRING_3);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            unionListState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            ctx.emitWatermark(new Watermark(0));

            synchronized (ctx.getCheckpointLock()) {
                for (long i = 0; i < numElements; i++) {
                    ctx.collect(new Tuple2<>(i, i));
                }
            }

            // don't emit a final watermark so that we don't trigger the registered event-time
            // timers
            while (isRunning) {
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * A non-parallel source with union state used to verify the restored state of {@link
     * CheckpointingNonParallelSourceWithListState}.
     */
    public static class CheckingNonParallelSourceWithListState
            extends RichSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR =
                CheckingNonParallelSourceWithListState.class + "_RESTORE_CHECK";

        private volatile boolean isRunning = true;

        private final int numElements;

        public CheckingNonParallelSourceWithListState(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListState<String> unionListState =
                    context.getOperatorStateStore()
                            .getListState(
                                    CheckpointingNonParallelSourceWithListState.STATE_DESCRIPTOR);

            if (context.isRestored()) {
                assertThat(
                        unionListState.get(),
                        containsInAnyOrder(
                                CheckpointingNonParallelSourceWithListState.CHECKPOINTED_STRING,
                                CheckpointingNonParallelSourceWithListState.CHECKPOINTED_STRING_1,
                                CheckpointingNonParallelSourceWithListState.CHECKPOINTED_STRING_2,
                                CheckpointingNonParallelSourceWithListState.CHECKPOINTED_STRING_3));

                getRuntimeContext()
                        .addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
                getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
            } else {
                throw new RuntimeException(
                        "This source should always be restored because it's only used when restoring from a savepoint.");
            }
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            // immediately trigger any set timers
            ctx.emitWatermark(new Watermark(1000));

            synchronized (ctx.getCheckpointLock()) {
                for (long i = 0; i < numElements; i++) {
                    ctx.collect(new Tuple2<>(i, i));
                }
            }

            while (isRunning) {
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /** A parallel source with union state used for testing. */
    public static class CheckpointingParallelSourceWithUnionListState
            extends RichSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

        static final ListStateDescriptor<String> STATE_DESCRIPTOR =
                new ListStateDescriptor<>("source-state", StringSerializer.INSTANCE);

        static final String[] CHECKPOINTED_STRINGS = {
            "Here be dragons!",
            "Here be more dragons!",
            "Here be yet more dragons!",
            "Here be the mostest dragons!"
        };

        private static final long serialVersionUID = 1L;

        private volatile boolean isRunning = true;

        private final int numElements;

        private transient ListState<String> unionListState;

        public CheckpointingParallelSourceWithUnionListState(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            unionListState.clear();

            for (String s : CHECKPOINTED_STRINGS) {
                if (s.hashCode() % getRuntimeContext().getNumberOfParallelSubtasks()
                        == getRuntimeContext().getIndexOfThisSubtask()) {
                    unionListState.add(s);
                }
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            unionListState = context.getOperatorStateStore().getUnionListState(STATE_DESCRIPTOR);
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            ctx.emitWatermark(new Watermark(0));

            synchronized (ctx.getCheckpointLock()) {
                for (long i = 0; i < numElements; i++) {
                    if (i % getRuntimeContext().getNumberOfParallelSubtasks()
                            == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(new Tuple2<>(i, i));
                    }
                }
            }

            // don't emit a final watermark so that we don't trigger the registered event-time
            // timers
            while (isRunning) {
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * A parallel source with union state used to verify the restored state of {@link
     * CheckpointingParallelSourceWithUnionListState}.
     */
    public static class CheckingParallelSourceWithUnionListState
            extends RichParallelSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR =
                CheckingParallelSourceWithUnionListState.class + "_RESTORE_CHECK";

        private volatile boolean isRunning = true;

        private final int numElements;

        public CheckingParallelSourceWithUnionListState(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListState<String> unionListState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    CheckpointingNonParallelSourceWithListState.STATE_DESCRIPTOR);

            if (context.isRestored()) {
                assertThat(
                        unionListState.get(),
                        containsInAnyOrder(
                                CheckpointingParallelSourceWithUnionListState
                                        .CHECKPOINTED_STRINGS));

                getRuntimeContext()
                        .addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
                getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
            } else {
                throw new RuntimeException(
                        "This source should always be restored because it's only used when restoring from a savepoint.");
            }
        }

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

            // immediately trigger any set timers
            ctx.emitWatermark(new Watermark(1000));

            synchronized (ctx.getCheckpointLock()) {
                for (long i = 0; i < numElements; i++) {
                    if (i % getRuntimeContext().getNumberOfParallelSubtasks()
                            == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(new Tuple2<>(i, i));
                    }
                }
            }

            while (isRunning) {
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /** A sink which counts the elements it sees in an accumulator. */
    public static class AccumulatorCountingSink<T> extends RichSinkFunction<T> {
        private static final long serialVersionUID = 1L;

        public static final String NUM_ELEMENTS_ACCUMULATOR =
                AccumulatorCountingSink.class + "_NUM_ELEMENTS";

        int count = 0;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            getRuntimeContext().addAccumulator(NUM_ELEMENTS_ACCUMULATOR, new IntCounter());
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            count++;
            getRuntimeContext().getAccumulator(NUM_ELEMENTS_ACCUMULATOR).add(1);
        }
    }
}
