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

package org.apache.flink.queryablestate.itcases;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import com.esotericsoftware.kryo.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Base class for queryable state integration tests with a configurable state backend. */
public abstract class AbstractQueryableStateTestBase {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(200L);
    private static final long RETRY_TIMEOUT = 50L;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(() -> Executors.newScheduledThreadPool(4));

    private final ScheduledExecutor executor =
            new ScheduledExecutorServiceAdapter(EXECUTOR_EXTENSION.getExecutor());

    /** State backend to use. */
    private StateBackend stateBackend;

    /** Client shared between all the test. */
    protected static QueryableStateClient client;

    protected static ClusterClient<?> clusterClient;

    protected static int maxParallelism;

    @TempDir static File classloaderFolder;

    @BeforeEach
    void setUp() throws Exception {
        // NOTE: do not use a shared instance for all tests as the tests may break
        this.stateBackend = createStateBackend();

        assertThat(clusterClient).isNotNull();

        maxParallelism = 4;
    }

    /**
     * Creates a state backend instance which is used in the {@link #setUp()} method before each
     * test case.
     *
     * @return a state backend instance for each unit test
     */
    protected abstract StateBackend createStateBackend() throws Exception;

    /**
     * Runs a simple topology producing random (key, 1) pairs at the sources (where number of keys
     * is in fixed in range 0...numKeys). The records are keyed and a reducing queryable state
     * instance is created, which sums up the records.
     *
     * <p>After submitting the job in detached mode, the QueryableStateCLient is used to query the
     * counts of each key in rounds until all keys have non-zero counts.
     */
    @Test
    void testQueryableState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final int numKeys = 256;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source = env.addSource(new TestKeyRangeSource(numKeys));

        ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState =
                new ReducingStateDescriptor<>("any-name", new SumReduce(), source.getType());

        final String queryName = "hakuna-matata";

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 7143749578983540352L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .asQueryableState(queryName, reducingState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            final AtomicLongArray counts = new AtomicLongArray(numKeys);

            final List<CompletableFuture<ReducingState<Tuple2<Integer, Long>>>> futures =
                    new ArrayList<>(numKeys);

            boolean allNonZero = false;
            while (!allNonZero && deadline.hasTimeLeft()) {
                allNonZero = true;
                futures.clear();

                for (int i = 0; i < numKeys; i++) {
                    final int key = i;

                    if (counts.get(key) > 0L) {
                        // Skip this one
                        continue;
                    } else {
                        allNonZero = false;
                    }

                    CompletableFuture<ReducingState<Tuple2<Integer, Long>>> result =
                            getKvState(
                                    deadline,
                                    client,
                                    jobId,
                                    queryName,
                                    key,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    reducingState,
                                    false,
                                    executor);

                    result.thenAccept(
                            response -> {
                                assertThatCode(
                                                () -> {
                                                    Tuple2<Integer, Long> res = response.get();
                                                    counts.set(key, res.f1);
                                                    assertThat(key)
                                                            .isEqualTo(res.f0.intValue())
                                                            .withFailMessage("Key mismatch");
                                                })
                                        .doesNotThrowAnyException();
                            });

                    futures.add(result);
                }

                // wait for all the futures to complete
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
                        .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
            }

            assertThat(allNonZero).isTrue().withFailMessage("Not all keys are non-zero");

            // All should be non-zero
            for (int i = 0; i < numKeys; i++) {
                long count = counts.get(i);
                assertThat(count)
                        .isGreaterThan(0)
                        .withFailMessage("Count at position " + i + " is " + count);
            }
        }
    }

    /** Tests that duplicate query registrations fail the job at the JobManager. */
    @Test
    void testDuplicateRegistrationFailsJob() throws Exception {
        final int numKeys = 256;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source = env.addSource(new TestKeyRangeSource(numKeys));

        // Reducing state
        ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState =
                new ReducingStateDescriptor<>("any-name", new SumReduce(), source.getType());

        final String queryName = "duplicate-me";

        final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
                source.keyBy(
                                new KeySelector<Tuple2<Integer, Long>, Integer>() {
                                    private static final long serialVersionUID =
                                            -4126824763829132959L;

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Long> value) {
                                        return value.f0;
                                    }
                                })
                        .asQueryableState(queryName, reducingState);

        final QueryableStateStream<Integer, Tuple2<Integer, Long>> duplicate =
                source.keyBy(
                                new KeySelector<Tuple2<Integer, Long>, Integer>() {
                                    private static final long serialVersionUID =
                                            -6265024000462809436L;

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Long> value) {
                                        return value.f0;
                                    }
                                })
                        .asQueryableState(queryName);

        // Submit the job graph
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        clusterClient
                .submitJob(jobGraph)
                .thenCompose(clusterClient::requestJobResult)
                .thenApply(JobResult::getSerializedThrowable)
                .thenAccept(
                        serializedThrowable -> {
                            assertThat(serializedThrowable).isPresent();
                            final Throwable t =
                                    serializedThrowable
                                            .get()
                                            .deserializeError(getClass().getClassLoader());
                            final String failureCause = ExceptionUtils.stringifyException(t);
                            assertThat(failureCause)
                                    .contains(
                                            "KvState with name '"
                                                    + queryName
                                                    + "' has already been registered by another operator");
                        })
                .get();
    }

    /**
     * Tests simple value state queryable state instance. Each source emits (subtaskIndex,
     * 0)..(subtaskIndex, numElements) tuples, which are then queried. The tests succeeds after each
     * subtask index is queried with value numElements (the latest element updated the state).
     */
    @Test
    void testValueState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        // Value state
        ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
                new ValueStateDescriptor<>("any", source.getType());

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 7662520075515707428L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .asQueryableState("hakuna", valueState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();
            executeValueQuery(deadline, client, jobId, "hakuna", valueState, numElements);
        }
    }

    /** This test checks if custom Kryo serializers are loaded with correct classloader. */
    @Test
    void testCustomKryoSerializerHandling() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1;
        final String stateName = "teriberka";

        final String customSerializerClassName = "CustomKryo";
        final URLClassLoader userClassLoader =
                createLoaderWithCustomKryoSerializer(customSerializerClassName);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        // Custom serializer is not needed, it's used just to check if serialization works.
        env.getConfig()
                .addDefaultKryoSerializer(
                        Byte.class,
                        (Serializer<?> & Serializable) createSerializer(userClassLoader));

        // Here we *force* using Kryo, to check if custom serializers are handled correctly WRT
        // classloading
        @SuppressWarnings({"rawtypes", "unchecked"})
        ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
                new ValueStateDescriptor<>("any", new GenericTypeInfo(Tuple2.class));

        env.addSource(new TestAscendingValueSource(numElements))
                .keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 7662520075515707428L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .asQueryableState(stateName, valueState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();
            jobGraph.setClasspaths(Arrays.asList(userClassLoader.getURLs()));

            clusterClient.submitJob(jobGraph).get();

            try {
                client.setUserClassLoader(userClassLoader);
                executeValueQuery(deadline, client, jobId, stateName, valueState, numElements);
            } finally {
                client.setUserClassLoader(null);
            }
        }
    }

    @SuppressWarnings("unchecked")
    <T extends Serializer<Byte> & Serializable> T createSerializer(ClassLoader userClassLoader)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<Serializer<?>> customSerializerClass =
                (Class<Serializer<?>>) userClassLoader.loadClass("CustomKryo");

        return (T) customSerializerClass.newInstance();
    }

    /**
     * Tests that the correct exception is thrown if the query contains a wrong jobId or wrong
     * queryable state name.
     */
    @Test
    @Disabled
    void testWrongJobIdAndWrongQueryableStateName() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));
        ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
                new ValueStateDescriptor<>("any", source.getType());

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 7662520075515707428L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .asQueryableState("hakuna", valueState);

        try (AutoCancellableJob closableJobGraph =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            clusterClient.submitJob(closableJobGraph.getJobGraph()).get();

            CompletableFuture<JobStatus> jobStatusFuture =
                    clusterClient.getJobStatus(closableJobGraph.getJobId());

            while (deadline.hasTimeLeft()
                    && !jobStatusFuture
                            .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS)
                            .equals(JobStatus.RUNNING)) {
                Thread.sleep(50);
                jobStatusFuture = clusterClient.getJobStatus(closableJobGraph.getJobId());
            }

            assertThat(jobStatusFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS))
                    .isEqualTo(JobStatus.RUNNING);

            final JobID wrongJobId = new JobID();

            CompletableFuture<ValueState<Tuple2<Integer, Long>>> unknownJobFuture =
                    client.getKvState(
                            wrongJobId, // this is the wrong job id
                            "hakuna",
                            0,
                            BasicTypeInfo.INT_TYPE_INFO,
                            valueState);

            assertThatThrownBy(
                            () ->
                                    unknownJobFuture.get(
                                            deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage(
                            "FlinkJobNotFoundException: Could not find Flink job ("
                                    + wrongJobId
                                    + ")");

            CompletableFuture<ValueState<Tuple2<Integer, Long>>> unknownQSName =
                    client.getKvState(
                            closableJobGraph.getJobId(),
                            "wrong-hakuna", // this is the wrong name.
                            0,
                            BasicTypeInfo.INT_TYPE_INFO,
                            valueState);

            assertThatThrownBy(
                            () ->
                                    unknownQSName.get(
                                            deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage(
                            "UnknownKvStateLocation: No KvStateLocation found for KvState instance with name 'wrong-hakuna'.");
        }
    }

    /**
     * Similar tests as {@link #testValueState()} but before submitting the job, we already issue
     * one request which fails.
     */
    @Test
    void testQueryNonStartedJobState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because clusterClient is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
                new ValueStateDescriptor<>("any", source.getType(), null);

        QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
                source.keyBy(
                                new KeySelector<Tuple2<Integer, Long>, Integer>() {

                                    private static final long serialVersionUID =
                                            7480503339992214681L;

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Long> value) {
                                        return value.f0;
                                    }
                                })
                        .asQueryableState("hakuna", valueState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            long expected = numElements;

            // query once
            client.getKvState(
                    autoCancellableJob.getJobId(),
                    queryableState.getQueryableStateName(),
                    0,
                    BasicTypeInfo.INT_TYPE_INFO,
                    valueState);

            clusterClient.submitJob(jobGraph).get();
            executeValueQuery(deadline, client, jobId, "hakuna", valueState, expected);
        }
    }

    /**
     * Tests simple value state queryable state instance with a default value set. Each source emits
     * (subtaskIndex, 0)..(subtaskIndex, numElements) tuples, the key is mapped to 1 but key 0 is
     * queried which should throw a {@link UnknownKeyOrNamespaceException} exception.
     *
     * @throws UnknownKeyOrNamespaceException thrown due querying a non-existent key
     */
    @Test
    void testValueStateDefault() throws Throwable {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        ValueStateDescriptor<Tuple2<Integer, Long>> valueState =
                new ValueStateDescriptor<>("any", source.getType(), Tuple2.of(0, 1337L));

        // only expose key "1"
        QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
                source.keyBy(
                                new KeySelector<Tuple2<Integer, Long>, Integer>() {
                                    private static final long serialVersionUID =
                                            4509274556892655887L;

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Long> value) {
                                        return 1;
                                    }
                                })
                        .asQueryableState("hakuna", valueState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            // Now query
            int key = 0;
            CompletableFuture<ValueState<Tuple2<Integer, Long>>> future =
                    getKvState(
                            deadline,
                            client,
                            jobId,
                            queryableState.getQueryableStateName(),
                            key,
                            BasicTypeInfo.INT_TYPE_INFO,
                            valueState,
                            true,
                            executor);

            assertThatThrownBy(
                            () -> future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS))
                    .cause()
                    .isInstanceOf(UnknownKeyOrNamespaceException.class);
        }
    }

    /**
     * Tests simple value state queryable state instance. Each source emits (subtaskIndex,
     * 0)..(subtaskIndex, numElements) tuples, which are then queried. The tests succeeds after each
     * subtask index is queried with value numElements (the latest element updated the state).
     *
     * <p>This is the same as the simple value state test, but uses the API shortcut.
     */
    @Test
    void testValueStateShortcut() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        // Value state shortcut
        final QueryableStateStream<Integer, Tuple2<Integer, Long>> queryableState =
                source.keyBy(
                                new KeySelector<Tuple2<Integer, Long>, Integer>() {
                                    private static final long serialVersionUID =
                                            9168901838808830068L;

                                    @Override
                                    public Integer getKey(Tuple2<Integer, Long> value) {
                                        return value.f0;
                                    }
                                })
                        .asQueryableState("matata");

        @SuppressWarnings("unchecked")
        final ValueStateDescriptor<Tuple2<Integer, Long>> stateDesc =
                (ValueStateDescriptor<Tuple2<Integer, Long>>) queryableState.getStateDescriptor();

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();
            executeValueQuery(deadline, client, jobId, "matata", stateDesc, numElements);
        }
    }

    /**
     * Tests simple reducing state queryable state instance. Each source emits (subtaskIndex,
     * 0)..(subtaskIndex, numElements) tuples, which are then queried. The reducing state instance
     * sums these up. The test succeeds after each subtask index is queried with result n*(n+1)/2.
     */
    @Test
    void testReducingState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        ReducingStateDescriptor<Tuple2<Integer, Long>> reducingState =
                new ReducingStateDescriptor<>("any", new SumReduce(), source.getType());

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 8470749712274833552L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .asQueryableState("jungle", reducingState);

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            final long expected = numElements * (numElements + 1L) / 2L;

            for (int key = 0; key < maxParallelism; key++) {
                boolean success = false;
                while (deadline.hasTimeLeft() && !success) {
                    CompletableFuture<ReducingState<Tuple2<Integer, Long>>> future =
                            getKvState(
                                    deadline,
                                    client,
                                    jobId,
                                    "jungle",
                                    key,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    reducingState,
                                    false,
                                    executor);

                    Tuple2<Integer, Long> value =
                            future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS).get();

                    assertThat(key).isEqualTo(value.f0.intValue()).withFailMessage("Key mismatch");
                    if (expected == value.f1) {
                        success = true;
                    } else {
                        // Retry
                        Thread.sleep(RETRY_TIMEOUT);
                    }
                }

                assertThat(success).isTrue().withFailMessage("Did not succeed query");
            }
        }
    }

    /**
     * Tests simple map state queryable state instance. Each source emits (subtaskIndex,
     * 0)..(subtaskIndex, numElements) tuples, which are then queried. The map state instance sums
     * the values up. The test succeeds after each subtask index is queried with result n*(n+1)/2.
     */
    @Test
    void testMapState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        final MapStateDescriptor<Integer, Tuple2<Integer, Long>> mapStateDescriptor =
                new MapStateDescriptor<>("timon", BasicTypeInfo.INT_TYPE_INFO, source.getType());
        mapStateDescriptor.setQueryable("timon-queryable");

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 8470749712274833552L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .process(
                        new ProcessFunction<Tuple2<Integer, Long>, Object>() {
                            private static final long serialVersionUID = -805125545438296619L;

                            private transient MapState<Integer, Tuple2<Integer, Long>> mapState;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                super.open(openContext);
                                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                            }

                            @Override
                            public void processElement(
                                    Tuple2<Integer, Long> value, Context ctx, Collector<Object> out)
                                    throws Exception {
                                Tuple2<Integer, Long> v = mapState.get(value.f0);
                                if (v == null) {
                                    v = new Tuple2<>(value.f0, 0L);
                                }
                                mapState.put(value.f0, new Tuple2<>(v.f0, v.f1 + value.f1));
                            }
                        });

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            final long expected = numElements * (numElements + 1L) / 2L;

            for (int key = 0; key < maxParallelism; key++) {
                boolean success = false;
                while (deadline.hasTimeLeft() && !success) {
                    CompletableFuture<MapState<Integer, Tuple2<Integer, Long>>> future =
                            getKvState(
                                    deadline,
                                    client,
                                    jobId,
                                    "timon-queryable",
                                    key,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    mapStateDescriptor,
                                    false,
                                    executor);

                    Tuple2<Integer, Long> value =
                            future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS)
                                    .get(key);

                    if (value != null && value.f0 != null && expected == value.f1) {
                        assertThat(key)
                                .isEqualTo(value.f0.intValue())
                                .withFailMessage("Key mismatch");
                        success = true;
                    } else {
                        // Retry
                        Thread.sleep(RETRY_TIMEOUT);
                    }
                }
                assertThat(success).isTrue().withFailMessage("Did not succeed query");
            }
        }
    }

    /**
     * Tests simple list state queryable state instance. Each source emits (subtaskIndex,
     * 0)..(subtaskIndex, numElements) tuples, which are then queried. The list state instance add
     * the values to the list. The test succeeds after each subtask index is queried and the list
     * contains the correct number of distinct elements.
     */
    @Test
    void testListState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        final ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<Long>("list", BasicTypeInfo.LONG_TYPE_INFO);
        listStateDescriptor.setQueryable("list-queryable");

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 8470749712274833552L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .process(
                        new ProcessFunction<Tuple2<Integer, Long>, Object>() {
                            private static final long serialVersionUID = -805125545438296619L;

                            private transient ListState<Long> listState;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                super.open(openContext);
                                listState = getRuntimeContext().getListState(listStateDescriptor);
                            }

                            @Override
                            public void processElement(
                                    Tuple2<Integer, Long> value, Context ctx, Collector<Object> out)
                                    throws Exception {
                                listState.add(value.f1);
                            }
                        });

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            final Map<Integer, Set<Long>> results = new HashMap<>();

            for (int key = 0; key < maxParallelism; key++) {
                boolean success = false;
                while (deadline.hasTimeLeft() && !success) {
                    final CompletableFuture<ListState<Long>> future =
                            getKvState(
                                    deadline,
                                    client,
                                    jobId,
                                    "list-queryable",
                                    key,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    listStateDescriptor,
                                    false,
                                    executor);

                    Iterable<Long> value =
                            future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS).get();

                    Set<Long> res = new HashSet<>();
                    for (Long v : value) {
                        res.add(v);
                    }

                    // the source starts at 0, so +1
                    if (res.size() == numElements + 1L) {
                        success = true;
                        results.put(key, res);
                    } else {
                        // Retry
                        Thread.sleep(RETRY_TIMEOUT);
                    }
                }

                assertThat(success).isTrue().withFailMessage("Did not succeed query");
            }

            for (int key = 0; key < maxParallelism; key++) {
                Set<Long> values = results.get(key);
                for (long i = 0L; i <= numElements; i++) {
                    assertThat(values).contains(i);
                }
            }
        }
    }

    @Test
    void testAggregatingState() throws Exception {
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);
        final long numElements = 1024L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setParallelism(maxParallelism);
        // Very important, because cluster is shared between tests and we
        // don't explicitly check that all slots are available before
        // submitting.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000L));

        DataStream<Tuple2<Integer, Long>> source =
                env.addSource(new TestAscendingValueSource(numElements));

        final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String>
                aggrStateDescriptor =
                        new AggregatingStateDescriptor<>("aggregates", new SumAggr(), String.class);
        aggrStateDescriptor.setQueryable("aggr-queryable");

        source.keyBy(
                        new KeySelector<Tuple2<Integer, Long>, Integer>() {
                            private static final long serialVersionUID = 8470749712274833552L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Long> value) {
                                return value.f0;
                            }
                        })
                .transform(
                        "TestAggregatingOperator",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new AggregatingTestOperator(aggrStateDescriptor));

        try (AutoCancellableJob autoCancellableJob =
                new AutoCancellableJob(deadline, clusterClient, env)) {

            final JobID jobId = autoCancellableJob.getJobId();
            final JobGraph jobGraph = autoCancellableJob.getJobGraph();

            clusterClient.submitJob(jobGraph).get();

            for (int key = 0; key < maxParallelism; key++) {
                boolean success = false;
                while (deadline.hasTimeLeft() && !success) {
                    CompletableFuture<AggregatingState<Tuple2<Integer, Long>, String>> future =
                            getKvState(
                                    deadline,
                                    client,
                                    jobId,
                                    "aggr-queryable",
                                    key,
                                    BasicTypeInfo.INT_TYPE_INFO,
                                    aggrStateDescriptor,
                                    false,
                                    executor);

                    String value =
                            future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS).get();

                    if (Long.parseLong(value) == numElements * (numElements + 1L) / 2L) {
                        success = true;
                    } else {
                        // Retry
                        Thread.sleep(RETRY_TIMEOUT);
                    }
                }

                assertThat(success).isTrue().withFailMessage("Did not succeed query");
            }
        }
    }

    /////				Sources/UDFs Used in the Tests			//////

    /**
     * Test source producing (key, 0)..(key, maxValue) with key being the sub task index.
     *
     * <p>After all tuples have been emitted, the source waits to be cancelled and does not
     * immediately finish.
     */
    private static class TestAscendingValueSource
            extends RichParallelSourceFunction<Tuple2<Integer, Long>> {

        private static final long serialVersionUID = 1459935229498173245L;

        private final long maxValue;
        private volatile boolean isRunning = true;

        TestAscendingValueSource(long maxValue) {
            Preconditions.checkArgument(maxValue >= 0);
            this.maxValue = maxValue;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
            // f0 => key
            int key = getRuntimeContext().getIndexOfThisSubtask();
            Tuple2<Integer, Long> record = new Tuple2<>(key, 0L);

            long currentValue = 0;
            while (isRunning && currentValue <= maxValue) {
                synchronized (ctx.getCheckpointLock()) {
                    record.f1 = currentValue;
                    ctx.collect(record);
                }

                currentValue++;
            }

            while (isRunning) {
                synchronized (this) {
                    wait();
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

            synchronized (this) {
                notifyAll();
            }
        }
    }

    /** Test source producing (key, 1) tuples with random key in key range (numKeys). */
    private static class TestKeyRangeSource
            extends RichParallelSourceFunction<Tuple2<Integer, Long>>
            implements CheckpointListener {

        private static final long serialVersionUID = -5744725196953582710L;

        private static final AtomicLong LATEST_CHECKPOINT_ID = new AtomicLong();
        private final int numKeys;
        private final ThreadLocalRandom random = ThreadLocalRandom.current();
        private volatile boolean isRunning = true;
        private int counter = 0;

        TestKeyRangeSource(int numKeys) {
            this.numKeys = numKeys;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                LATEST_CHECKPOINT_ID.set(0L);
            }
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
            // f0 => key
            Tuple2<Integer, Long> record = new Tuple2<>(0, 1L);

            while (isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    record.f0 = random.nextInt(numKeys);
                    ctx.collect(record);
                    counter++;
                }

                if (counter % 50 == 0) {
                    // mild slow down
                    Thread.sleep(1L);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
                LATEST_CHECKPOINT_ID.set(checkpointId);
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }

    /**
     * An operator that uses {@link AggregatingState}.
     *
     * <p>The operator exists for lack of possibility to get an {@link AggregatingState} from the
     * {@link org.apache.flink.api.common.functions.RuntimeContext}. If this were not the case, we
     * could have a {@link ProcessFunction}.
     */
    private static class AggregatingTestOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<Tuple2<Integer, Long>, String> {

        private static final long serialVersionUID = 1L;

        private final AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String>
                stateDescriptor;
        private transient AggregatingState<Tuple2<Integer, Long>, String> state;

        AggregatingTestOperator(
                AggregatingStateDescriptor<Tuple2<Integer, Long>, String, String> stateDesc) {
            this.stateDescriptor = stateDesc;
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.state =
                    getKeyedStateBackend()
                            .getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    stateDescriptor);
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, Long>> element) throws Exception {
            state.add(element.getValue());
        }
    }

    /**
     * Test {@link AggregateFunction} concatenating the already stored string with the long passed
     * as argument.
     */
    private static class SumAggr
            implements AggregateFunction<Tuple2<Integer, Long>, String, String> {

        private static final long serialVersionUID = -6249227626701264599L;

        @Override
        public String createAccumulator() {
            return "0";
        }

        @Override
        public String add(Tuple2<Integer, Long> value, String accumulator) {
            long acc = Long.valueOf(accumulator);
            acc += value.f1;
            return Long.toString(acc);
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            return Long.toString(Long.valueOf(a) + Long.valueOf(b));
        }
    }

    /** Test {@link ReduceFunction} summing up its two arguments. */
    protected static class SumReduce implements ReduceFunction<Tuple2<Integer, Long>> {
        private static final long serialVersionUID = -8651235077342052336L;

        @Override
        public Tuple2<Integer, Long> reduce(
                Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
            value1.f1 += value2.f1;
            return value1;
        }
    }

    /////				General Utility Methods				//////

    /**
     * A wrapper of the job graph that makes sure to cancel the job and wait for termination after
     * the execution of every test.
     */
    private static class AutoCancellableJob implements AutoCloseable {

        private final ClusterClient<?> clusterClient;
        private final JobGraph jobGraph;

        private final JobID jobId;

        private final Deadline deadline;

        AutoCancellableJob(
                Deadline deadline,
                final ClusterClient<?> clusterClient,
                final StreamExecutionEnvironment env) {
            Preconditions.checkNotNull(env);

            this.clusterClient = Preconditions.checkNotNull(clusterClient);
            this.jobGraph = env.getStreamGraph().getJobGraph();

            this.jobId = Preconditions.checkNotNull(jobGraph.getJobID());

            this.deadline = deadline;
        }

        JobGraph getJobGraph() {
            return jobGraph;
        }

        JobID getJobId() {
            return jobId;
        }

        @Override
        public void close() throws Exception {
            // Free cluster resources
            clusterClient.cancel(jobId).get();
            // cancel() is non-blocking so do this to make sure the job finished
            CompletableFuture<JobStatus> jobStatusFuture =
                    FutureUtils.retrySuccessfulWithDelay(
                            () -> clusterClient.getJobStatus(jobId),
                            Duration.ofMillis(50),
                            deadline,
                            (jobStatus) -> jobStatus.equals(JobStatus.CANCELED),
                            new ScheduledExecutorServiceAdapter(EXECUTOR_EXTENSION.getExecutor()));
            assertThat(jobStatusFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS))
                    .isEqualTo(JobStatus.CANCELED);
        }
    }

    private static <K, S extends State, V> CompletableFuture<S> getKvState(
            final Deadline deadline,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryName,
            final K key,
            final TypeInformation<K> keyTypeInfo,
            final StateDescriptor<S, V> stateDescriptor,
            final boolean failForUnknownKeyOrNamespace,
            final ScheduledExecutor executor) {

        final CompletableFuture<S> resultFuture = new CompletableFuture<>();
        getKvStateIgnoringCertainExceptions(
                deadline,
                resultFuture,
                client,
                jobId,
                queryName,
                key,
                keyTypeInfo,
                stateDescriptor,
                failForUnknownKeyOrNamespace,
                executor);
        return resultFuture;
    }

    private static <K, S extends State, V> void getKvStateIgnoringCertainExceptions(
            final Deadline deadline,
            final CompletableFuture<S> resultFuture,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryName,
            final K key,
            final TypeInformation<K> keyTypeInfo,
            final StateDescriptor<S, V> stateDescriptor,
            final boolean failForUnknownKeyOrNamespace,
            final ScheduledExecutor executor) {

        if (!resultFuture.isDone()) {
            CompletableFuture<S> expected =
                    client.getKvState(jobId, queryName, key, keyTypeInfo, stateDescriptor);
            expected.whenCompleteAsync(
                    (result, throwable) -> {
                        if (throwable != null) {
                            if (throwable.getCause() instanceof CancellationException
                                    || throwable.getCause() instanceof AssertionError
                                    || (failForUnknownKeyOrNamespace
                                            && throwable.getCause()
                                                    instanceof UnknownKeyOrNamespaceException)) {
                                resultFuture.completeExceptionally(throwable.getCause());
                            } else if (deadline.hasTimeLeft()) {
                                getKvStateIgnoringCertainExceptions(
                                        deadline,
                                        resultFuture,
                                        client,
                                        jobId,
                                        queryName,
                                        key,
                                        keyTypeInfo,
                                        stateDescriptor,
                                        failForUnknownKeyOrNamespace,
                                        executor);
                            }
                        } else {
                            resultFuture.complete(result);
                        }
                    },
                    executor);

            resultFuture.whenComplete((result, throwable) -> expected.cancel(false));
        }
    }

    /**
     * Retry a query for state for keys between 0 and {@link #maxParallelism} until
     * <tt>expected</tt> equals the value of the result tuple's second field.
     */
    private void executeValueQuery(
            final Deadline deadline,
            final QueryableStateClient client,
            final JobID jobId,
            final String queryableStateName,
            final ValueStateDescriptor<Tuple2<Integer, Long>> stateDescriptor,
            final long expected)
            throws Exception {

        for (int key = 0; key < maxParallelism; key++) {
            boolean success = false;
            while (deadline.hasTimeLeft() && !success) {
                CompletableFuture<ValueState<Tuple2<Integer, Long>>> future =
                        getKvState(
                                deadline,
                                client,
                                jobId,
                                queryableStateName,
                                key,
                                BasicTypeInfo.INT_TYPE_INFO,
                                stateDescriptor,
                                false,
                                executor);

                Tuple2<Integer, Long> value =
                        future.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS).value();

                assertThat(key).isEqualTo(value.f0.intValue()).withFailMessage("Key mismatch");
                if (expected == value.f1) {
                    success = true;
                } else {
                    // Retry
                    Thread.sleep(RETRY_TIMEOUT);
                }
            }

            assertThat(success).isTrue().withFailMessage("Did not succeed query");
        }
    }

    private static URLClassLoader createLoaderWithCustomKryoSerializer(String className)
            throws IOException {
        return ClassLoaderUtils.compileAndLoadJava(
                classloaderFolder,
                className + ".java",
                "import com.esotericsoftware.kryo.Kryo;\n"
                        + "import com.esotericsoftware.kryo.Serializer;\n"
                        + "import com.esotericsoftware.kryo.io.Input;\n"
                        + "import com.esotericsoftware.kryo.io.Output;\n"
                        + "import java.io.Serializable;\n"
                        + "public class "
                        + className
                        + " extends Serializer<Byte> implements Serializable {\n"
                        + "    @Override\n"
                        + "    public void write(Kryo kryo, Output output, Byte testJob) {}\n"
                        + "    @Override\n"
                        + "    public Byte read(Kryo kryo, Input input, Class<Byte> aClass) {\n"
                        + "        return null;\n"
                        + "    }\n"
                        + "}\n");
    }
}
