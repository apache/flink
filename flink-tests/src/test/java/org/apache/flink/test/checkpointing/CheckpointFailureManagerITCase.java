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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.TestingCheckpointStorageAccessCoordinatorView;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureManager.EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE;
import static org.junit.Assert.fail;

/** Tests to verify end-to-end logic of checkpoint failure manager. */
public class CheckpointFailureManagerITCase extends TestLogger {

    /**
     * Test that checkpoint finalization failure is counted by {@link CheckpointFailureManager} and
     * eventually fails the job. In this test, finalization is failed by throwing an exception from
     * {@link org.apache.flink.runtime.state.CheckpointStorageLocation#createMetadataOutputStream}
     * which should fail the job.
     */
    @Test
    public void testFinalizationFailureCounted() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        env.getCheckpointConfig().setCheckpointStorage(new FailingFinalizationCheckpointStorage());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE).sinkTo(new DiscardingSink<>());
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        try {
            TestUtils.submitJobAndWaitForResult(
                    cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
            fail("The job should fail");
        } catch (JobExecutionException jobException) {
            if (!isCheckpointFailure(jobException)) {
                throw jobException;
            }
        }
    }

    @ClassRule
    public static MiniClusterWithClientResource cluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder().build());

    @Test(timeout = 20_000)
    public void testAsyncCheckpointFailureTriggerJobFailed() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new AsyncFailureStateBackend());
        env.addSource(new StringGeneratingSourceFunction()).sinkTo(new DiscardingSink<>());
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        try {
            // assert that the job only execute checkpoint once and only failed once.
            TestUtils.submitJobAndWaitForResult(
                    cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
        } catch (JobExecutionException jobException) {
            if (!isCheckpointFailure(jobException)) {
                throw jobException;
            }
        }
        // assert that the job only failed once.
        Assert.assertEquals(1, StringGeneratingSourceFunction.INITIALIZE_TIMES.get());
    }

    private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<String>
            implements CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private static final ListStateDescriptor<Long> stateDescriptor =
                new ListStateDescriptor<>("emitted", Long.class);

        private final byte[] randomBytes = new byte[10];

        private ListState<Long> listState;
        private long emitted = 0L;

        private volatile boolean isRunning = true;

        public static final AtomicInteger INITIALIZE_TIMES = new AtomicInteger(0);

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.update(Collections.singletonList(emitted));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore().getListState(stateDescriptor);
            INITIALIZE_TIMES.addAndGet(1);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                ThreadLocalRandom.current().nextBytes(randomBytes);
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(new String(randomBytes));
                    emitted += 1;
                }
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class AsyncFailureStateBackend extends MemoryStateBackend {
        private static final long serialVersionUID = 1L;
        private static final SnapshotStrategy<OperatorStateHandle, SnapshotResources>
                ASYNC_DECLINING_SNAPSHOT_STRATEGY =
                        new SnapshotStrategy<OperatorStateHandle, SnapshotResources>() {
                            @Override
                            public SnapshotResources syncPrepareResources(long checkpointId)
                                    throws Exception {
                                return null;
                            }

                            @Override
                            public SnapshotResultSupplier<OperatorStateHandle> asyncSnapshot(
                                    SnapshotResources syncPartResource,
                                    long checkpointId,
                                    long timestamp,
                                    @Nonnull CheckpointStreamFactory streamFactory,
                                    @Nonnull CheckpointOptions checkpointOptions) {
                                return (closeableRegistry) -> {
                                    throw new Exception("Expected async snapshot exception.");
                                };
                            }
                        };

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry) {
            return new DefaultOperatorStateBackendBuilder(
                    env.getUserCodeClassLoader().asClassLoader(),
                    env.getExecutionConfig(),
                    true,
                    stateHandles,
                    cancelStreamRegistry) {
                @Override
                @SuppressWarnings("unchecked")
                public DefaultOperatorStateBackend build() {
                    CloseableRegistry closeableRegistry = new CloseableRegistry();
                    return new DefaultOperatorStateBackend(
                            executionConfig,
                            closeableRegistry,
                            new HashMap<>(),
                            new HashMap<>(),
                            new HashMap<>(),
                            new HashMap<>(),
                            new SnapshotStrategyRunner(
                                    "Async Failure State Backend",
                                    ASYNC_DECLINING_SNAPSHOT_STRATEGY,
                                    closeableRegistry,
                                    SnapshotExecutionType.ASYNCHRONOUS));
                }
            }.build();
        }

        @Override
        public AsyncFailureStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
            return this;
        }
    }

    private static class FailingFinalizationCheckpointStorage implements CheckpointStorage {
        private static final long serialVersionUID = 8134582566514272546L;

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) {
            return new TestCompletedCheckpointStorageLocation();
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) {
            return new TestingCheckpointStorageAccessCoordinatorView() {
                @Override
                public CheckpointStorageLocation initializeLocationForCheckpoint(
                        long checkpointId) {
                    return new NonPersistentMetadataCheckpointStorageLocation(Integer.MAX_VALUE) {
                        @Override
                        public CheckpointMetadataOutputStream createMetadataOutputStream() {
                            throw new RuntimeException("finalization failure");
                        }
                    };
                }
            };
        }
    }

    private boolean isCheckpointFailure(JobExecutionException jobException) {
        return ExceptionUtils.findThrowable(jobException, FlinkRuntimeException.class)
                .filter(
                        ex ->
                                ex.getMessage()
                                        .startsWith(EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE))
                .isPresent();
    }
}
