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

import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.InfiniteIntegerSource;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckpointSyncPhaseTimeoutITCase extends TestLogger {

    private static final long SYNC_PHASE_TIMEOUT_MILLIS = 50L;
    private static final StreamExecutionEnvironment env = envSetup();

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static StreamExecutionEnvironment envSetup() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10);
        env.getCheckpointConfig()
                .setCheckpointSyncPhaseTimeout(Duration.ofMillis(SYNC_PHASE_TIMEOUT_MILLIS));
        RestartStrategyUtils.configureNoRestartStrategy(env);
        return env;
    }

    @Test
    void testStuckSyncPhaseFailsJob(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        env.addSource(new BlockingSnapshotSource()).sinkTo(new DiscardingSink<>());
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

        assertThatThrownBy(() -> miniCluster.executeJobBlocking(jobGraph))
                .isInstanceOf(JobExecutionException.class)
                .hasRootCauseInstanceOf(TimeoutException.class)
                .hasRootCauseMessage(
                        "Task Source: Custom Source (1/1)#0 did not complete the synchronous phase of checkpoint 1 within "
                                + SYNC_PHASE_TIMEOUT_MILLIS
                                + " ms.");
    }

    private static class BlockingSnapshotSource extends InfiniteIntegerSource
            implements CheckpointedFunction {
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            new CountDownLatch(1).await(2 * SYNC_PHASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}
    }
}
