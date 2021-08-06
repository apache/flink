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

package org.apache.flink.test.checkpointing.finishing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;

public class CheckpointingWithFinishingTasksITCase extends AbstractTestBase {

    @Test
    public void test() throws Exception {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.milliseconds(500)));

        DataStream<String> branch1 = createOneInputOperatorsBranch(env, 0);
        DataStream<String> branch2 = createOneInputOperatorsBranch(env, 6);

        final SingleOutputStreamOperator<String> job =
                branch1.connect(branch2)
                        .transform(
                                "two-input-operator",
                                BasicTypeInfo.STRING_TYPE_INFO,
                                new FinishingLifecycleTwoInputOperator<>());

        try (CloseableIterator<String> iterator = job.executeAndCollect()) {
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        }
    }

    private SingleOutputStreamOperator<String> createOneInputOperatorsBranch(
            StreamExecutionEnvironment env, long checkpointStoppingOffset) {
        return env.addSource(
                        new IndexGeneratingOldSource(checkpointStoppingOffset, Boundedness.BOUNDED))
                .startNewChain()
                .transform(
                        "operator-after-network-exchange",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new FinishingLifecycleOneInputOperator<>())
                .keyBy(value -> value)
                .transform(
                        "operator-after-key-by",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new FinishingLifecycleOneInputOperator<>());
    }

    private static class IndexGeneratingOldSource extends RichParallelSourceFunction<String>
            implements CheckpointedFunction {

        private volatile boolean isRunning = true;
        private ListState<String> listState;
        private final Boundedness bounded;
        private final long checkpointStoppingOffset;

        private IndexGeneratingOldSource(long checkpointStoppingOffset, Boundedness bounded) {
            this.bounded = bounded;
            this.checkpointStoppingOffset = checkpointStoppingOffset;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            if (isListStateEmpty()) {
                return;
            }

            while (isRunning) {
                Thread.sleep(100);
            }
            for (String record : listState.get()) {
                ctx.collect(record);
            }
            listState.clear();
        }

        private boolean isListStateEmpty() throws Exception {
            return !listState.get().iterator().hasNext();
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // finish odd tasks
            if (context.getCheckpointId() == checkpointStoppingOffset + 2 && isEvenSubtask()) {
                this.isRunning = false;
            }

            // trigger failover with partially finished tasks
            if (context.getCheckpointId() == checkpointStoppingOffset + 4) {
                throw new ExpectedTestException("Fail and recover");
            }

            // finish all tasks
            if (context.getCheckpointId() >= checkpointStoppingOffset + 6
                    && bounded == Boundedness.BOUNDED) {
                this.isRunning = false;
            }
        }

        private boolean isEvenSubtask() {
            return getRuntimeContext().getIndexOfThisSubtask() % 2 == 0;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "indices-to-emit", StringSerializer.INSTANCE));
            if (!context.isRestored()) {
                listState.add(
                        String.format(
                                "(offset %d) index: %d",
                                checkpointStoppingOffset,
                                getRuntimeContext().getIndexOfThisSubtask()));
            }
        }
    }
}
