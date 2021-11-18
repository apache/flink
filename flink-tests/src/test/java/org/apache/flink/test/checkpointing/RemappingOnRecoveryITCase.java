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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.junit.Assert.assertEquals;

/** Test of ignoring in-flight data during recovery. */
public class RemappingOnRecoveryITCase extends TestLogger {
    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(3)
                            .setNumberSlotsPerTaskManager(2)
                            .build());

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private static final int PARALLELISM = 3;

    private SharedReference<Map<Integer, Integer>> subtaskBeforeRecovery;
    private SharedReference<Map<Integer, Integer>> subtaskAfterRecovery;
    private SharedReference<AtomicBoolean> afterRecovery;

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
        return config;
    }

    public void setupSharedObjects() {
        subtaskBeforeRecovery = sharedObjects.add(new ConcurrentHashMap<>());
        subtaskAfterRecovery = sharedObjects.add(new ConcurrentHashMap<>());
        afterRecovery = sharedObjects.add(new AtomicBoolean());
    }

    @Test
    public void test() throws Exception {
        setupSharedObjects();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setMaxParallelism(13);
        env.enableCheckpointing(150);
        env.disableOperatorChaining();
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setAlignmentTimeout(Duration.ZERO);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(fixedDelayRestart(1, 0));

        env.addSource(new NumberSource(afterRecovery))
                .keyBy(e -> e)
                .map(new SlowMap(subtaskBeforeRecovery, subtaskAfterRecovery, afterRecovery))
                .addSink(new DiscardingSink<>());

        // when: Job is executed.
        env.execute("Total sum");

        System.out.println(subtaskBeforeRecovery.get());
        System.out.println(subtaskAfterRecovery.get());
    }

    private static class NumberSource implements SourceFunction<Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        private final SharedReference<AtomicBoolean> afterRecovery;
        private ListState<Integer> valueState;

        public NumberSource(SharedReference<AtomicBoolean> afterRecovery) {
            this.afterRecovery = afterRecovery;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int next = 0;
            synchronized (ctx.getCheckpointLock()) {
                do {
                    next++;
                    valueState.update(singletonList(next));
                    ctx.collect(next);
                } while (next < 50);
            }

            if (!afterRecovery.get().get()) {
                Thread.sleep(10_000);
            }
        }

        @Override
        public void cancel() {}

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            if (!afterRecovery.get().get() && context.getCheckpointId() == 2) {
                throw new ExpectedTestException("The planned fail on the second checkpoint");
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.valueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("state", Types.INT));

            if (valueState.get().iterator().hasNext()) {
                afterRecovery.get().set(true);
            }
        }
    }

    private static class SlowMap extends RichMapFunction<Integer, Integer>
            implements CheckpointedFunction {

        private SharedReference<Map<Integer, Integer>> subtaskBeforeRecovery;
        private SharedReference<Map<Integer, Integer>> subtaskAfterRecovery;
        private SharedReference<AtomicBoolean> afterRecovery;
        private ValueState<Integer> valueState;

        public SlowMap(
                SharedReference<Map<Integer, Integer>> subtaskBeforeRecovery,
                SharedReference<Map<Integer, Integer>> subtaskAfterRecovery,
                SharedReference<AtomicBoolean> afterRecovery) {
            this.subtaskAfterRecovery = subtaskAfterRecovery;
            this.subtaskBeforeRecovery = subtaskBeforeRecovery;
            this.afterRecovery = afterRecovery;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            if (!afterRecovery.get().get()) {
                valueState.update(value);
                subtaskBeforeRecovery.get().put(value, getRuntimeContext().getIndexOfThisSubtask());
            } else {
                assertEquals(value, valueState.value());
                subtaskAfterRecovery.get().put(value, getRuntimeContext().getIndexOfThisSubtask());
            }

            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            valueState =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("value", Integer.class));
        }
    }
}
