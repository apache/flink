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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterables.get;
import static org.apache.flink.test.util.TestUtils.loadCheckpointMetadata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/** Base class for tests related to period materialization of ChangelogStateBackend. */
@RunWith(Parameterized.class)
public abstract class ChangelogRecoveryITCaseBase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int NUM_TASK_SLOTS = 4;
    protected static final int NUM_SLOTS = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;
    protected static final int TOTAL_ELEMENTS = 10_000;

    protected final AbstractStateBackend delegatedStateBackend;

    protected MiniClusterWithClientResource cluster;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Parameterized.Parameters(name = "delegated state backend type = {0}")
    public static Collection<AbstractStateBackend> parameter() {
        return Arrays.asList(
                new HashMapStateBackend(),
                new EmbeddedRocksDBStateBackend(true),
                new EmbeddedRocksDBStateBackend(false));
    }

    public ChangelogRecoveryITCaseBase(AbstractStateBackend delegatedStateBackend) {
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
            long checkpointInterval,
            int restartAttempts,
            long materializationInterval,
            int materializationMaxFailure) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpointInterval).enableChangelogStateBackend(true);
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

    protected StreamExecutionEnvironment getEnv(
            StateBackend stateBackend,
            File checkpointFile,
            long checkpointInterval,
            int restartAttempts,
            long materializationInterval,
            int materializationMaxFailure) {
        StreamExecutionEnvironment env =
                getEnv(
                        stateBackend,
                        checkpointInterval,
                        restartAttempts,
                        materializationInterval,
                        materializationMaxFailure);
        env.getCheckpointConfig().setCheckpointStorage(checkpointFile.toURI());
        return env;
    }

    protected JobGraph buildJobGraph(
            StreamExecutionEnvironment env, ControlledSource controlledSource, JobID jobId) {
        KeyedStream<Integer, Integer> keyedStream =
                env.addSource(controlledSource)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
                        .keyBy(element -> element);
        keyedStream.process(new CountFunction()).addSink(new CollectionSink()).setParallelism(1);
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
                .process(
                        new ProcessWindowFunction<Integer, Integer, Integer, TimeWindow>() {
                            @Override
                            public void process(
                                    Integer integer,
                                    Context context,
                                    Iterable<Integer> elements,
                                    Collector<Integer> out) {}
                        })
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph(env.getClass().getClassLoader(), jobId);
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

    public static Set<StateHandleID> getAllStateHandleId(JobID jobID, MiniCluster miniCluster)
            throws IOException, FlinkJobNotFoundException, ExecutionException,
                    InterruptedException {
        Optional<String> mostRecentCompletedCheckpointPath =
                getLatestCompletedCheckpointPath(jobID, miniCluster);
        if (!mostRecentCompletedCheckpointPath.isPresent()) {
            return Collections.emptySet();
        }

        CheckpointMetadata checkpointMetadata;
        try {
            checkpointMetadata = loadCheckpointMetadata(mostRecentCompletedCheckpointPath.get());
        } catch (IOException ioException) {
            if (ExceptionUtils.findThrowable(ioException, FileNotFoundException.class)
                    .isPresent()) {
                // return empty result when the metadata file do not exist due to subsumed
                // checkpoint.
                return Collections.emptySet();
            }
            throw ioException;
        }

        Set<StateHandleID> materializationIds =
                checkpointMetadata.getOperatorStates().stream()
                        .flatMap(operatorState -> operatorState.getStates().stream())
                        .flatMap(
                                operatorSubtaskState ->
                                        operatorSubtaskState.getManagedKeyedState().stream())
                        .flatMap(
                                keyedStateHandle ->
                                        keyedStateHandle instanceof ChangelogStateBackendHandle
                                                ? ((ChangelogStateBackendHandle) keyedStateHandle)
                                                        .getMaterializedStateHandles().stream()
                                                : Stream.of(keyedStateHandle))
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
        FsStateChangelogStorageFactory.configure(
                configuration, TEMPORARY_FOLDER.newFolder(), Duration.ofMinutes(1), 10);
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

    /** A Process function with value state mocks word count. */
    protected static class CountFunction
            extends KeyedProcessFunction<Integer, Integer, Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private ValueState<Integer> countState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            this.countState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("countState", Integer.class));
        }

        @Override
        public void processElement(
                Integer value, Context ctx, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {
            Integer count = countState.value();
            Integer currentCount = count == null ? 1 : count + 1;
            countState.update(currentCount);
            out.collect(Tuple2.of(value, currentCount));
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

    /** An exception marks the failure is thrown artificially. */
    protected static class ArtificialFailure extends Exception {}
}
