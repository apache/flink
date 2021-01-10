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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.state.AbstractSnapshotStrategy;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.tasks.ExceptionallyDoneFuture;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Integrated tests to verify the logic to notify checkpoint aborted via RPC message. */
@RunWith(Parameterized.class)
public class NotifyCheckpointAbortedITCase extends TestLogger {

    private static final long DECLINE_CHECKPOINT_ID = 2L;
    private static final long TEST_TIMEOUT = 100000;
    private static final String DECLINE_SINK_NAME = "DeclineSink";
    private static MiniClusterWithClientResource cluster;

    private static Path checkpointPath;

    @Parameterized.Parameter public boolean unalignedCheckpointEnabled;

    @Parameterized.Parameters(name = "unalignedCheckpointEnabled ={0}")
    public static Collection<Boolean> parameter() {
        return Arrays.asList(true, false);
    }

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);
        configuration.setString(HighAvailabilityOptions.HA_MODE, TestingHAFactory.class.getName());

        checkpointPath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        cluster.before();

        NormalMap.reset();
        DeclineSink.reset();
        TestingCompletedCheckpointStore.reset();
    }

    @After
    public void shutdown() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    /**
     * Verify operators would be notified as checkpoint aborted.
     *
     * <p>The job would run with at least two checkpoints. The 1st checkpoint would fail due to add
     * checkpoint to store, and the 2nd checkpoint would decline by async checkpoint phase of
     * 'DeclineSink'.
     *
     * <p>The job graph looks like: NormalSource --> keyBy --> NormalMap --> DeclineSink
     */
    @Test(timeout = TEST_TIMEOUT)
    public void testNotifyCheckpointAborted() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableUnalignedCheckpoints(unalignedCheckpointEnabled);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        env.disableOperatorChaining();
        env.setParallelism(1);

        final StateBackend failingStateBackend = new DeclineSinkFailingStateBackend(checkpointPath);
        env.setStateBackend(failingStateBackend);

        env.addSource(new NormalSource())
                .name("NormalSource")
                .keyBy((KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0)
                .transform("NormalMap", TypeInformation.of(Integer.class), new NormalMap())
                .transform(DECLINE_SINK_NAME, TypeInformation.of(Object.class), new DeclineSink());

        final ClusterClient<?> clusterClient = cluster.getClusterClient();
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        JobID jobID = jobGraph.getJobID();

        clusterClient.submitJob(jobGraph).get();

        TestingCompletedCheckpointStore.addCheckpointLatch.await();
        log.info("The checkpoint to abort is ready to add to checkpoint store.");
        TestingCompletedCheckpointStore.abortCheckpointLatch.trigger();

        log.info("Verifying whether all operators have been notified of checkpoint-1 aborted.");
        verifyAllOperatorsNotifyAborted();
        log.info("Verified that all operators have been notified of checkpoint-1 aborted.");
        resetAllOperatorsNotifyAbortedLatches();
        verifyAllOperatorsNotifyAbortedTimes(1);

        DeclineSink.waitLatch.trigger();
        log.info("Verifying whether all operators have been notified of checkpoint-2 aborted.");
        verifyAllOperatorsNotifyAborted();
        log.info("Verified that all operators have been notified of checkpoint-2 aborted.");
        verifyAllOperatorsNotifyAbortedTimes(2);

        clusterClient.cancel(jobID).get();
        log.info("Test is verified successfully as expected.");
    }

    private void verifyAllOperatorsNotifyAborted() throws InterruptedException {
        NormalMap.notifiedAbortedLatch.await();
        DeclineSink.notifiedAbortedLatch.await();
    }

    private void resetAllOperatorsNotifyAbortedLatches() {
        NormalMap.notifiedAbortedLatch.reset();
        DeclineSink.notifiedAbortedLatch.reset();
    }

    private void verifyAllOperatorsNotifyAbortedTimes(int expectedTimes) {
        assertEquals(expectedTimes, NormalMap.notifiedAbortedTimes.get());
        assertEquals(expectedTimes, DeclineSink.notifiedAbortedTimes.get());
    }

    /** Normal source function. */
    private static class NormalSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;
        protected volatile boolean running;

        NormalSource() {
            this.running = true;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(
                            Tuple2.of(
                                    ThreadLocalRandom.current().nextInt(),
                                    ThreadLocalRandom.current().nextInt()));
                }
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    private static class NormalMap extends StreamMap<Tuple2<Integer, Integer>, Integer> {
        private static final long serialVersionUID = 1L;
        private static final OneShotLatch notifiedAbortedLatch = new OneShotLatch();
        private static final AtomicInteger notifiedAbortedTimes = new AtomicInteger(0);

        public NormalMap() {
            super(new NormalMapFunction());
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            notifiedAbortedTimes.incrementAndGet();
            notifiedAbortedLatch.trigger();
        }

        static void reset() {
            notifiedAbortedLatch.reset();
            notifiedAbortedTimes.set(0);
        }
    }

    /** Normal map function. */
    private static class NormalMapFunction
            implements MapFunction<Tuple2<Integer, Integer>, Integer>, CheckpointedFunction {
        private static final long serialVersionUID = 1L;
        private ValueState<Integer> valueState;

        @Override
        public Integer map(Tuple2<Integer, Integer> value) throws Exception {
            valueState.update(value.f1);
            return value.f1;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {}

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            valueState =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("value", Integer.class));
        }
    }

    /** A decline sink. */
    private static class DeclineSink extends StreamSink<Integer> {
        private static final long serialVersionUID = 1L;
        private static final OneShotLatch notifiedAbortedLatch = new OneShotLatch();
        private static final OneShotLatch waitLatch = new OneShotLatch();
        private static final AtomicInteger notifiedAbortedTimes = new AtomicInteger(0);

        public DeclineSink() {
            super(
                    new SinkFunction<Integer>() {
                        private static final long serialVersionUID = 1L;
                    });
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            if (context.getCheckpointId() == DECLINE_CHECKPOINT_ID) {
                DeclineSink.waitLatch.await();
            }
            super.snapshotState(context);
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            notifiedAbortedTimes.incrementAndGet();
            notifiedAbortedLatch.trigger();
        }

        static void reset() {
            notifiedAbortedLatch.reset();
            waitLatch.reset();
            notifiedAbortedTimes.set(0);
        }
    }

    /** The snapshot strategy to create failing runnable future at the checkpoint to decline. */
    private static class DeclineSinkFailingSnapshotStrategy
            extends AbstractSnapshotStrategy<OperatorStateHandle> {

        protected DeclineSinkFailingSnapshotStrategy() {
            super("StuckAsyncSnapshotStrategy");
        }

        @Override
        public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
                long checkpointId,
                long timestamp,
                @Nonnull CheckpointStreamFactory streamFactory,
                @Nonnull CheckpointOptions checkpointOptions) {
            if (checkpointId == DECLINE_CHECKPOINT_ID) {
                return ExceptionallyDoneFuture.of(new ExpectedTestException());
            } else {
                return DoneFuture.of(SnapshotResult.empty());
            }
        }
    }

    /**
     * The operator statebackend to create {@link DeclineSinkFailingSnapshotStrategy} at {@link
     * DeclineSink}.
     */
    private static class DeclineSinkFailingOperatorStateBackend
            extends DefaultOperatorStateBackend {

        public DeclineSinkFailingOperatorStateBackend(
                ExecutionConfig executionConfig,
                CloseableRegistry closeStreamOnCancelRegistry,
                AbstractSnapshotStrategy<OperatorStateHandle> snapshotStrategy) {
            super(
                    executionConfig,
                    closeStreamOnCancelRegistry,
                    new HashMap<>(),
                    new HashMap<>(),
                    new HashMap<>(),
                    new HashMap<>(),
                    snapshotStrategy);
        }
    }

    /**
     * The state backend to create {@link DeclineSinkFailingOperatorStateBackend} at {@link
     * DeclineSink}.
     */
    private static class DeclineSinkFailingStateBackend extends FsStateBackend {
        private static final long serialVersionUID = 1L;

        public DeclineSinkFailingStateBackend(Path checkpointDataUri) {
            super(checkpointDataUri);
        }

        @Override
        public DeclineSinkFailingStateBackend configure(
                ReadableConfig config, ClassLoader classLoader) {
            return new DeclineSinkFailingStateBackend(checkpointPath);
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws BackendBuildingException {
            if (operatorIdentifier.contains(DECLINE_SINK_NAME)) {
                return new DeclineSinkFailingOperatorStateBackend(
                        env.getExecutionConfig(),
                        cancelStreamRegistry,
                        new DeclineSinkFailingSnapshotStrategy());
            } else {
                return new DefaultOperatorStateBackendBuilder(
                                env.getUserCodeClassLoader().asClassLoader(),
                                env.getExecutionConfig(),
                                false,
                                stateHandles,
                                cancelStreamRegistry)
                        .build();
            }
        }
    }

    private static class TestingHaServices extends EmbeddedHaServices {
        private final CheckpointRecoveryFactory checkpointRecoveryFactory;

        TestingHaServices(CheckpointRecoveryFactory checkpointRecoveryFactory, Executor executor) {
            super(executor);
            this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        }

        @Override
        public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
            return checkpointRecoveryFactory;
        }
    }

    /** An extension of {@link StandaloneCompletedCheckpointStore}. */
    private static class TestingCompletedCheckpointStore
            extends StandaloneCompletedCheckpointStore {
        private static final OneShotLatch addCheckpointLatch = new OneShotLatch();
        private static final OneShotLatch abortCheckpointLatch = new OneShotLatch();

        TestingCompletedCheckpointStore() {
            super(1);
        }

        @Override
        public void addCheckpoint(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup)
                throws Exception {
            if (abortCheckpointLatch.isTriggered()) {
                super.addCheckpoint(checkpoint, checkpointsCleaner, postCleanup);
            } else {
                // tell main thread that all checkpoints on task side have been finished.
                addCheckpointLatch.trigger();
                // wait for the main thread to throw exception so that the checkpoint would be
                // notified as aborted.
                abortCheckpointLatch.await();
                throw new ExpectedTestException();
            }
        }

        static void reset() {
            addCheckpointLatch.reset();
            abortCheckpointLatch.reset();
        }
    }

    /** Testing HA factory which needs to be public in order to be instantiatable. */
    public static class TestingHAFactory implements HighAvailabilityServicesFactory {

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            return new TestingHaServices(
                    PerJobCheckpointRecoveryFactory.useSameServicesForAllJobs(
                            new TestingCompletedCheckpointStore(),
                            new StandaloneCheckpointIDCounter()),
                    executor);
        }
    }
}
