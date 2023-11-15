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

package org.apache.flink.deployment;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * End-to-end test for heavy deployment descriptors. This test creates a heavy deployment by
 * producing inflated meta data for the source's operator state. The state is registered as union
 * state and will be multiplied in deployment.
 */
public class HeavyDeploymentStressTestProgram {

    private static final ConfigOption<Integer> NUM_LIST_STATES_PER_OP =
            ConfigOptions.key("heavy_deployment_test.num_list_states_per_op")
                    .intType()
                    .defaultValue(100);

    private static final ConfigOption<Integer> NUM_PARTITIONS_PER_LIST_STATE =
            ConfigOptions.key("heavy_deployment_test.num_partitions_per_list_state")
                    .intType()
                    .defaultValue(100);

    public static void main(String[] args) throws Exception {

        final ParameterTool pt = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupEnvironment(env, pt);

        final int numStates =
                pt.getInt(NUM_LIST_STATES_PER_OP.key(), NUM_LIST_STATES_PER_OP.defaultValue());
        final int numPartitionsPerState =
                pt.getInt(
                        NUM_PARTITIONS_PER_LIST_STATE.key(),
                        NUM_PARTITIONS_PER_LIST_STATE.defaultValue());

        Preconditions.checkState(
                env.getCheckpointInterval() > 0L, "Checkpointing must be enabled for this test!");

        env.addSource(new SimpleEndlessSourceWithBloatedState(numStates, numPartitionsPerState))
                .setParallelism(env.getParallelism())
                .sinkTo(new DiscardingSink<>())
                .setParallelism(1);

        env.execute("HeavyDeploymentStressTestProgram");
    }

    /**
     * Source with dummy operator state that results in inflated meta data.
     *
     * @deprecated This class is based on the {@link
     *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
     *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
     */
    @Deprecated
    static class SimpleEndlessSourceWithBloatedState extends RichParallelSourceFunction<String>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        private final int numListStates;
        private final int numPartitionsPerListState;

        private transient volatile boolean isRunning;

        /** Flag to induce failure after we have a valid checkpoint. */
        private transient volatile boolean readyToFail;

        SimpleEndlessSourceWithBloatedState(int numListStates, int numPartitionsPerListState) {
            this.numListStates = numListStates;
            this.numPartitionsPerListState = numPartitionsPerListState;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            readyToFail = false;

            if (context.isRestored()) {
                isRunning = false;
            } else {
                isRunning = true;

                OperatorStateStore operatorStateStore = context.getOperatorStateStore();
                for (int i = 0; i < numListStates; ++i) {

                    ListStateDescriptor<String> listStateDescriptor =
                            new ListStateDescriptor<>("test-list-state-" + i, String.class);

                    ListState<String> unionListState =
                            operatorStateStore.getUnionListState(listStateDescriptor);

                    for (int j = 0; j < numPartitionsPerListState; ++j) {
                        unionListState.add(String.valueOf(j));
                    }
                }
            }
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {

                if (readyToFail && getRuntimeContext().getIndexOfThisSubtask() == 0) {
                    throw new Exception("Artificial failure.");
                }

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect("test-element");
                }

                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            readyToFail = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }
}
