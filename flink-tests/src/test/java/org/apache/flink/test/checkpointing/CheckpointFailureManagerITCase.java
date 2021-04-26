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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

/** Tests to verify end-to-end logic of checkpoint failure manager. */
public class CheckpointFailureManagerITCase extends TestLogger {
    private static MiniClusterWithClientResource cluster;

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .build());
        cluster.before();
    }

    @AfterClass
    public static void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test(timeout = 10000)
    public void testAsyncCheckpointFailureTriggerJobFailed() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend(new AsyncFailureStateBackend());
        env.addSource(new StringGeneratingSourceFunction()).addSink(new DiscardingSink<>());
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
        try {
            // assert that the job only execute checkpoint once and only failed once.
            TestUtils.submitJobAndWaitForResult(
                    cluster.getClusterClient(), jobGraph, getClass().getClassLoader());
        } catch (JobExecutionException jobException) {
            Optional<FlinkRuntimeException> throwable =
                    ExceptionUtils.findThrowable(jobException, FlinkRuntimeException.class);
            Assert.assertTrue(throwable.isPresent());
            Assert.assertEquals(
                    CheckpointFailureManager.EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE,
                    throwable.get().getMessage());
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
            listState.clear();
            listState.add(emitted);
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
                    return new DefaultOperatorStateBackend(
                            executionConfig,
                            cancelStreamRegistry,
                            new HashMap<>(),
                            new HashMap<>(),
                            new HashMap<>(),
                            new HashMap<>(),
                            mock(AbstractSnapshotStrategy.class)) {
                        @Nonnull
                        @Override
                        public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
                                long checkpointId,
                                long timestamp,
                                @Nonnull CheckpointStreamFactory streamFactory,
                                @Nonnull CheckpointOptions checkpointOptions) {

                            return new FutureTask<>(
                                    () -> {
                                        throw new Exception("Expected async snapshot exception.");
                                    });
                        }
                    };
                }
            }.build();
        }

        @Override
        public AsyncFailureStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
            return this;
        }
    }
}
