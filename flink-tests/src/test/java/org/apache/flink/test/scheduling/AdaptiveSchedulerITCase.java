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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assume.assumeTrue;

/** Integration tests for the adaptive scheduler. */
public class AdaptiveSchedulerITCase extends TestLogger {

    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;

    private static final Configuration configuration = getConfiguration();

    private static Configuration getConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);

        return configuration;
    }

    @ClassRule
    public static final MiniClusterResource MINI_CLUSTER_WITH_CLIENT_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    /** Tests that the adaptive scheduler can recover stateful operators. */
    @Test
    public void testGlobalFailoverCanRecoverState() throws Exception {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

        env.enableCheckpointing(20L, CheckpointingMode.EXACTLY_ONCE);
        final DataStreamSource<Integer> input = env.addSource(new SimpleSource());

        input.addSink(new DiscardingSink<>());

        env.execute();
    }

    /**
     * Simple source which fails once after a successful checkpoint has been taken. Upon recovery
     * the source will immediately terminate.
     */
    public static final class SimpleSource extends RichParallelSourceFunction<Integer>
            implements CheckpointListener, CheckpointedFunction {

        private static final ListStateDescriptor<Boolean> unionStateListDescriptor =
                new ListStateDescriptor<>("state", Boolean.class);

        private volatile boolean running = true;

        @Nullable private ListState<Boolean> unionListState = null;

        private boolean hasFailedBefore = false;

        private boolean fail = false;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running && !hasFailedBefore) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(getRuntimeContext().getIndexOfThisSubtask());

                    Thread.sleep(5L);
                }

                if (fail) {
                    throw new FlinkException("Test failure.");
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            fail = true;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            unionListState =
                    context.getOperatorStateStore().getUnionListState(unionStateListDescriptor);

            for (Boolean previousState : unionListState.get()) {
                hasFailedBefore |= previousState;
            }

            unionListState.clear();
            unionListState.add(true);
        }
    }
}
