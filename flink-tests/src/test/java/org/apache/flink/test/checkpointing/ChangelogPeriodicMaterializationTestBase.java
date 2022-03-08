/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterables.get;
import static org.apache.flink.test.util.TestUtils.getMostRecentCompletedCheckpointMaybe;
import static org.apache.flink.test.util.TestUtils.loadCheckpointMetadata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Base class for tests related to period materialization of ChangelogStateBackend. */
@RunWith(Parameterized.class)
public abstract class ChangelogPeriodicMaterializationTestBase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int NUM_TASK_SLOTS = 4;
    protected static final int NUM_SLOTS = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;
    protected static final int TOTAL_ELEMENTS = 10_000;

    protected final AbstractStateBackend delegatedStateBackend;

    protected MiniClusterWithClientResource cluster;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameterized.Parameters(name = "delegated state backend type ={0}")
    public static Collection<AbstractStateBackend> parameter() {
        return Arrays.asList(
                new HashMapStateBackend(),
                new EmbeddedRocksDBStateBackend(true),
                new EmbeddedRocksDBStateBackend(false));
    }

    public ChangelogPeriodicMaterializationTestBase(AbstractStateBackend delegatedStateBackend) {
        this.delegatedStateBackend = delegatedStateBackend;
    }

    @Before
    public void setup() throws Exception {
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configure())
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                                .build());
        cluster.before();
        cluster.getMiniCluster().overrideRestoreModeForChangelogStateBackend();
    }

    @After
    public void tearDown() throws IOException {
        cluster.after();
        // clear result in sink
        CollectionSink.clearExpectedResult();
    }

    protected StreamExecutionEnvironment getEnv(
            StateBackend stateBackend,
            File checkpointFile,
            long checkpointInterval,
            int restartAttempts,
            long materializationInterval,
            int materializationMaxFailure) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval)
                .enableChangelogStateBackend(true)
                .getCheckpointConfig()
                .setCheckpointStorage(checkpointFile.toURI());
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);
        env.setStateBackend(stateBackend)
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, 0));
        env.configure(
                new Configuration()
                        .set(
                                StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL,
                                Duration.ofMillis(materializationInterval))
                        .set(
                                StateChangelogOptions.MATERIALIZATION_MAX_FAILURES_ALLOWED,
                                materializationMaxFailure));
        return env;
    }

    protected JobGraph buildJobGraph(
            StreamExecutionEnvironment env, ControlledSource controlledSource, JobID jobId) {
        env.addSource(controlledSource)
                .keyBy(element -> element)
                .map(new CountMapper())
                .addSink(new CollectionSink())
                .setParallelism(1);
        return env.getStreamGraph().getJobGraph(jobId);
    }

    protected void waitAndAssert(JobGraph jobGraph) throws Exception {
        waitUntilJobFinished(jobGraph);
        assertEquals(CollectionSink.getActualResult(), ControlledSource.getExpectedResult());
    }

    protected JobID generateJobID() {
        byte[] randomBytes = new byte[AbstractID.SIZE];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        return JobID.fromByteArray(randomBytes);
    }

    protected static Set<StateHandleID> getAllStateHandleId(File checkpointFile)
            throws IOException {
        Optional<File> mostRecentCompletedCheckpoint =
                getMostRecentCompletedCheckpointMaybe(checkpointFile);
        if (!mostRecentCompletedCheckpoint.isPresent()) {
            return Collections.emptySet();
        }
        CheckpointMetadata checkpointMetadata =
                loadCheckpointMetadata(mostRecentCompletedCheckpoint.get().toString());
        Set<StateHandleID> materializationIds =
                checkpointMetadata.getOperatorStates().stream()
                        .flatMap(operatorState -> operatorState.getStates().stream())
                        .flatMap(
                                operatorSubtaskState ->
                                        operatorSubtaskState.getManagedKeyedState().stream())
                        .flatMap(
                                keyedStateHandle ->
                                        ((ChangelogStateBackendHandle) keyedStateHandle)
                                                .getMaterializedStateHandles().stream())
                        .map(KeyedStateHandle::getStateHandleId)
                        .collect(Collectors.toSet());
        if (!materializationIds.isEmpty()) {
            return materializationIds;
        }
        return Collections.emptySet();
    }

    private void waitUntilJobFinished(JobGraph jobGraph) throws Exception {
        JobSubmissionResult jobSubmissionResult =
                cluster.getMiniCluster().submitJob(jobGraph).get();
        JobResult jobResult =
                cluster.getMiniCluster().requestJobResult(jobSubmissionResult.getJobID()).get();
        if (jobResult.getSerializedThrowable().isPresent()) {
            throw jobResult.getSerializedThrowable().get();
        }
        assertSame(ApplicationStatus.SUCCEEDED, jobResult.getApplicationStatus());
    }

    private Configuration configure() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 200);
        FsStateChangelogStorageFactory.configure(configuration, TEMPORARY_FOLDER.newFolder());
        return configuration;
    }

    /** A source consumes data which is generated randomly and supports pre-handling record. */
    protected static class ControlledSource extends RichSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {

        private static final long serialVersionUID = 1L;

        protected volatile int currentIndex;

        protected final AtomicInteger completedCheckpointNum;

        protected volatile boolean isCanceled;

        private static final List<Integer> sourceList =
                Collections.unmodifiableList(initSourceData(TOTAL_ELEMENTS));

        private transient ListState<Integer> currentIndexState;

        private transient ListState<Integer> completedCheckpointNumState;

        public ControlledSource() {
            this.completedCheckpointNum = new AtomicInteger();
        }

        public static List<Integer> initSourceData(int totalNum) {
            List<Integer> sourceList = new ArrayList<>(totalNum);
            for (int i = 0; i < totalNum; i++) {
                sourceList.add(ThreadLocalRandom.current().nextInt(totalNum));
            }
            return sourceList;
        }

        public static Map<Integer, Integer> getExpectedResult() {
            return sourceList.stream()
                    .collect(
                            Collectors.toConcurrentMap(
                                    element -> element % 100, element -> 1, Integer::sum));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            currentIndexState.update(Collections.singletonList(currentIndex));
            completedCheckpointNumState.update(
                    Collections.singletonList(completedCheckpointNum.get()));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            currentIndexState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("currentIndexState", Integer.class));
            completedCheckpointNumState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "completedCheckpointNumState", Integer.class));
            if (context.isRestored()) {
                currentIndex = get(currentIndexState.get(), 0);
                completedCheckpointNum.compareAndSet(0, get(completedCheckpointNumState.get(), 0));
            }
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (!isCanceled && currentIndex < sourceList.size()) {
                beforeElement(ctx);
                synchronized (ctx.getCheckpointLock()) {
                    if (!isCanceled && currentIndex < sourceList.size()) {
                        int currentElement = sourceList.get(currentIndex++);
                        ctx.collect(currentElement % 100);
                    }
                }
            }
        }

        // it runs without holding the checkpoint lock
        protected void beforeElement(SourceContext<Integer> ctx) throws Exception {
            // do nothing by default
        }

        protected void waitWhile(SerializableBooleanSupplierWithException supplier)
                throws Exception {
            while (supplier.getAsBoolean()) {
                Thread.sleep(10);
            }
        }

        protected void throwArtificialFailure() throws Exception {
            throw new ArtificialFailure();
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            completedCheckpointNum.getAndIncrement();
        }
    }

    /** A sink puts the result of WordCount into a Map using for validating. */
    protected static class CollectionSink implements SinkFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private static final Map<Integer, Integer> expectedResult = new HashMap<>();

        @Override
        public void invoke(Tuple2<Integer, Integer> value, Context context) throws Exception {
            expectedResult.merge(value.f0, value.f1, Math::max);
        }

        public static void clearExpectedResult() {
            expectedResult.clear();
        }

        public static Map<Integer, Integer> getActualResult() {
            return expectedResult;
        }
    }

    /** A Map function with value state mocks word count. */
    protected static class CountMapper extends RichMapFunction<Integer, Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.countState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("countState", Integer.class));
        }

        @Override
        public Tuple2<Integer, Integer> map(Integer value) throws Exception {
            Integer count = countState.value();
            Integer currentCount = count == null ? 1 : count + 1;
            countState.update(currentCount);
            return Tuple2.of(value, currentCount);
        }
    }

    /** Wrapper of delegated state backend which supports apply the snapshot result. */
    protected static class DelegatedStateBackendWrapper extends AbstractStateBackend {

        private static final long serialVersionUID = 1L;

        private final AbstractStateBackend delegatedStataBackend;

        private final SerializableFunctionWithException<
                        RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                snapshotResultFunction;

        public DelegatedStateBackendWrapper(
                AbstractStateBackend delegatedStataBackend,
                SerializableFunctionWithException<RunnableFuture<SnapshotResult<KeyedStateHandle>>>
                        snapshotResultFunction) {
            this.delegatedStataBackend = delegatedStataBackend;
            this.snapshotResultFunction = snapshotResultFunction;
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws IOException {
            AbstractKeyedStateBackend<K> delegatedKeyedStateBackend =
                    delegatedStataBackend.createKeyedStateBackend(
                            env,
                            jobID,
                            operatorIdentifier,
                            keySerializer,
                            numberOfKeyGroups,
                            keyGroupRange,
                            kvStateRegistry,
                            ttlTimeProvider,
                            metricGroup,
                            stateHandles,
                            cancelStreamRegistry);
            return new AbstractKeyedStateBackend<K>(
                    kvStateRegistry,
                    keySerializer,
                    env.getUserCodeClassLoader().asClassLoader(),
                    env.getExecutionConfig(),
                    ttlTimeProvider,
                    delegatedKeyedStateBackend.getLatencyTrackingStateConfig(),
                    cancelStreamRegistry,
                    delegatedKeyedStateBackend.getKeyContext()) {
                @Override
                public void setCurrentKey(K newKey) {
                    delegatedKeyedStateBackend.setCurrentKey(newKey);
                }

                @Override
                public void notifyCheckpointComplete(long checkpointId) throws Exception {
                    delegatedKeyedStateBackend.notifyCheckpointComplete(checkpointId);
                }

                @Nonnull
                @Override
                public SavepointResources<K> savepoint() throws Exception {
                    return delegatedKeyedStateBackend.savepoint();
                }

                @Override
                public int numKeyValueStateEntries() {
                    return delegatedKeyedStateBackend.numKeyValueStateEntries();
                }

                @Override
                public <N> Stream<K> getKeys(String state, N namespace) {
                    return delegatedKeyedStateBackend.getKeys(state, namespace);
                }

                @Override
                public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
                    return delegatedKeyedStateBackend.getKeysAndNamespaces(state);
                }

                @Nonnull
                @Override
                public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
                        @Nonnull TypeSerializer<N> namespaceSerializer,
                        @Nonnull StateDescriptor<S, SV> stateDesc,
                        @Nonnull
                                StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                                        snapshotTransformFactory)
                        throws Exception {
                    return delegatedKeyedStateBackend.createInternalState(
                            namespaceSerializer, stateDesc, snapshotTransformFactory);
                }

                @Nonnull
                @Override
                public <
                                T extends
                                        HeapPriorityQueueElement & PriorityComparable<? super T>
                                                & Keyed<?>>
                        KeyGroupedInternalPriorityQueue<T> create(
                                @Nonnull String stateName,
                                @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
                    return delegatedKeyedStateBackend.create(
                            stateName, byteOrderedElementSerializer);
                }

                @Nonnull
                @Override
                public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
                        long checkpointId,
                        long timestamp,
                        @Nonnull CheckpointStreamFactory streamFactory,
                        @Nonnull CheckpointOptions checkpointOptions)
                        throws Exception {
                    RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotResultRunnableFuture =
                            delegatedKeyedStateBackend.snapshot(
                                    checkpointId, timestamp, streamFactory, checkpointOptions);
                    return snapshotResultFunction.apply(snapshotResultRunnableFuture);
                }
            };
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            return delegatedStataBackend.createOperatorStateBackend(
                    env, operatorIdentifier, stateHandles, cancelStreamRegistry);
        }
    }

    /** A BooleanSupplier supports to serialize and throw exceptions. */
    @FunctionalInterface
    protected interface SerializableBooleanSupplierWithException extends Serializable {

        /**
         * Gets a result.
         *
         * @return a result.
         * @throws Exception This supplier may throw an exception.
         */
        boolean getAsBoolean() throws Exception;
    }

    /** A FunctionWithException supports serialization. */
    @FunctionalInterface
    protected interface SerializableFunctionWithException<T>
            extends FunctionWithException<T, T, Exception>, Serializable {}

    /** An exception marks the failure is thrown artificially. */
    protected static class ArtificialFailure extends Exception {}
}
