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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PerJobCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesFactory;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.assertEquals;

/** Tests for region failover with multi regions. */
public class RegionFailoverITCase extends TestLogger {

    private static final int FAIL_BASE = 1000;
    private static final int NUM_OF_REGIONS = 3;
    private static final int MAX_PARALLELISM = 2 * NUM_OF_REGIONS;
    private static final Set<Integer> EXPECTED_INDICES_MULTI_REGION =
            IntStream.range(0, NUM_OF_REGIONS).boxed().collect(Collectors.toSet());
    private static final Set<Integer> EXPECTED_INDICES_SINGLE_REGION = Collections.singleton(0);
    private static final int NUM_OF_RESTARTS = 3;
    private static final int NUM_ELEMENTS = FAIL_BASE * 10;

    private static final String SINGLE_REGION_SOURCE_NAME = "single-source";
    private static final String MULTI_REGION_SOURCE_NAME = "multi-source";

    private static AtomicLong lastCompletedCheckpointId = new AtomicLong(0);
    private static AtomicInteger numCompletedCheckpoints = new AtomicInteger(0);
    private static AtomicInteger jobFailedCnt = new AtomicInteger(0);

    private static Map<Long, Integer> snapshotIndicesOfSubTask = new HashMap<>();

    private static MiniClusterWithClientResource cluster;

    private static boolean restoredState = false;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
        configuration.setString(HighAvailabilityOptions.HA_MODE, TestingHAFactory.class.getName());

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(2)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();
        jobFailedCnt.set(0);
        numCompletedCheckpoints.set(0);
    }

    @AfterClass
    public static void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    /**
     * Tests that a simple job (Source -> Map) with multi regions could restore with operator state.
     *
     * <p>The last subtask of Map function in the 1st stream graph would fail {@code
     * NUM_OF_RESTARTS} times, and it will verify whether the restored state is identical to last
     * completed checkpoint's.
     */
    @Test(timeout = 60000)
    public void testMultiRegionFailover() {
        try {
            JobGraph jobGraph = createJobGraph();
            ClusterClient<?> client = cluster.getClusterClient();
            submitJobAndWaitForResult(client, jobGraph, getClass().getClassLoader());
            verifyAfterJobExecuted();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void verifyAfterJobExecuted() {
        Assert.assertTrue(
                "The test multi-region job has never ever restored state.", restoredState);

        int keyCount = 0;
        for (Map<Integer, Integer> map : ValidatingSink.maps) {
            for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                assertEquals(4 * entry.getKey() + 1, (int) entry.getValue());
                keyCount += 1;
            }
        }
        assertEquals(NUM_ELEMENTS / 2, keyCount);
    }

    private JobGraph createJobGraph() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(NUM_OF_REGIONS);
        env.setMaxParallelism(MAX_PARALLELISM);
        env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.disableOperatorChaining();

        // Use DataStreamUtils#reinterpretAsKeyed to avoid merge regions and this stream graph would
        // exist num of 'NUM_OF_REGIONS' individual regions.
        DataStreamUtils.reinterpretAsKeyedStream(
                        env.addSource(
                                        new StringGeneratingSourceFunction(
                                                NUM_ELEMENTS, NUM_ELEMENTS / NUM_OF_RESTARTS))
                                .name(MULTI_REGION_SOURCE_NAME)
                                .setParallelism(NUM_OF_REGIONS),
                        (KeySelector<Tuple2<Integer, Integer>, Integer>) value -> value.f0,
                        TypeInformation.of(Integer.class))
                .map(new FailingMapperFunction(NUM_OF_RESTARTS))
                .setParallelism(NUM_OF_REGIONS)
                .addSink(new ValidatingSink())
                .setParallelism(NUM_OF_REGIONS);

        // another stream graph totally disconnected with the above one.
        env.addSource(
                        new StringGeneratingSourceFunction(
                                NUM_ELEMENTS, NUM_ELEMENTS / NUM_OF_RESTARTS))
                .name(SINGLE_REGION_SOURCE_NAME)
                .setParallelism(1)
                .map((MapFunction<Tuple2<Integer, Integer>, Object>) value -> value)
                .setParallelism(1);

        return env.getStreamGraph().getJobGraph();
    }

    private static class StringGeneratingSourceFunction
            extends RichParallelSourceFunction<Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private final long numElements;
        private final long checkpointLatestAt;

        private int index = -1;

        private int lastRegionIndex = -1;

        private volatile boolean isRunning = true;

        private ListState<Integer> listState;

        private static final ListStateDescriptor<Integer> stateDescriptor =
                new ListStateDescriptor<>("list-1", Integer.class);

        private ListState<Integer> unionListState;

        private static final ListStateDescriptor<Integer> unionStateDescriptor =
                new ListStateDescriptor<>("list-2", Integer.class);

        StringGeneratingSourceFunction(long numElements, long checkpointLatestAt) {
            this.numElements = numElements;
            this.checkpointLatestAt = checkpointLatestAt;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            if (index < 0) {
                // not been restored, so initialize
                index = 0;
            }

            int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            while (isRunning && index < numElements) {

                synchronized (ctx.getCheckpointLock()) {
                    int key = index / 2;
                    int forwardTaskIndex =
                            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                    key, MAX_PARALLELISM, NUM_OF_REGIONS);
                    // pre-partition output keys
                    if (forwardTaskIndex == subTaskIndex) {
                        // we would send data with the same key twice.
                        ctx.collect(Tuple2.of(key, index));
                    }
                    index += 1;
                }

                if (numCompletedCheckpoints.get() < 3) {
                    // not yet completed enough checkpoints, so slow down
                    if (index < checkpointLatestAt) {
                        // mild slow down
                        Thread.sleep(1);
                    } else {
                        // wait until the checkpoints are completed
                        while (isRunning && numCompletedCheckpoints.get() < 3) {
                            Thread.sleep(300);
                        }
                    }
                }
                if (jobFailedCnt.get() < NUM_OF_RESTARTS) {
                    // slow down if job has not failed for 'NUM_OF_RESTARTS' times.
                    Thread.sleep(1);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            if (indexOfThisSubtask != 0) {
                listState.clear();
                listState.add(index);
                if (indexOfThisSubtask == NUM_OF_REGIONS - 1) {
                    lastRegionIndex = index;
                    snapshotIndicesOfSubTask.put(context.getCheckpointId(), lastRegionIndex);
                }
            }
            unionListState.clear();
            unionListState.add(indexOfThisSubtask);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            if (context.isRestored()) {
                restoredState = true;

                unionListState =
                        context.getOperatorStateStore().getUnionListState(unionStateDescriptor);
                Set<Integer> actualIndices =
                        StreamSupport.stream(unionListState.get().spliterator(), false)
                                .collect(Collectors.toSet());
                if (getRuntimeContext().getTaskName().contains(SINGLE_REGION_SOURCE_NAME)) {
                    Assert.assertTrue(
                            CollectionUtils.isEqualCollection(
                                    EXPECTED_INDICES_SINGLE_REGION, actualIndices));
                } else {
                    Assert.assertTrue(
                            CollectionUtils.isEqualCollection(
                                    EXPECTED_INDICES_MULTI_REGION, actualIndices));
                }

                if (indexOfThisSubtask == 0) {
                    listState = context.getOperatorStateStore().getListState(stateDescriptor);
                    Assert.assertTrue(
                            "list state should be empty for subtask-0",
                            ((List<Integer>) listState.get()).isEmpty());
                } else {
                    listState = context.getOperatorStateStore().getListState(stateDescriptor);
                    Assert.assertTrue(
                            "list state should not be empty for subtask-" + indexOfThisSubtask,
                            ((List<Integer>) listState.get()).size() > 0);

                    if (indexOfThisSubtask == NUM_OF_REGIONS - 1) {
                        index = listState.get().iterator().next();
                        if (index
                                != snapshotIndicesOfSubTask.get(lastCompletedCheckpointId.get())) {
                            throw new RuntimeException(
                                    "Test failed due to unexpected recovered index: "
                                            + index
                                            + ", while last completed checkpoint record index: "
                                            + snapshotIndicesOfSubTask.get(
                                                    lastCompletedCheckpointId.get()));
                        }
                    }
                }
            } else {
                unionListState =
                        context.getOperatorStateStore().getUnionListState(unionStateDescriptor);

                if (indexOfThisSubtask != 0) {
                    listState = context.getOperatorStateStore().getListState(stateDescriptor);
                }
            }
        }
    }

    private static class FailingMapperFunction
            extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        private final int restartTimes;
        private ValueState<Integer> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("value", Integer.class));
        }

        FailingMapperFunction(int restartTimes) {
            this.restartTimes = restartTimes;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

            if (input.f1 > FAIL_BASE * (jobFailedCnt.get() + 1)) {

                // we would let region-0 to failover first
                if (jobFailedCnt.get() < 1 && indexOfThisSubtask == 0) {
                    jobFailedCnt.incrementAndGet();
                    throw new TestException();
                }

                // then let last region to failover
                if (jobFailedCnt.get() < restartTimes && indexOfThisSubtask == NUM_OF_REGIONS - 1) {
                    jobFailedCnt.incrementAndGet();
                    throw new TestException();
                }
            }

            // take input (1, 2) and (1, 3) for example, we would finally emit (1, 5) out with the
            // usage of keyed state.
            Integer value = valueState.value();
            if (value == null) {
                valueState.update(input.f1);
                return input;
            } else {
                return Tuple2.of(input.f0, value + input.f1);
            }
        }
    }

    private static class ValidatingSink extends RichSinkFunction<Tuple2<Integer, Integer>>
            implements ListCheckpointed<HashMap<Integer, Integer>> {

        @SuppressWarnings("unchecked")
        private static Map<Integer, Integer>[] maps =
                (Map<Integer, Integer>[]) new Map<?, ?>[NUM_OF_REGIONS];

        private HashMap<Integer, Integer> counts = new HashMap<>();

        @Override
        public void invoke(Tuple2<Integer, Integer> input) {
            counts.merge(input.f0, input.f1, Math::max);
        }

        @Override
        public void close() throws Exception {
            maps[getRuntimeContext().getIndexOfThisSubtask()] = counts;
        }

        @Override
        public List<HashMap<Integer, Integer>> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this.counts);
        }

        @Override
        public void restoreState(List<HashMap<Integer, Integer>> state) throws Exception {
            if (state.size() != 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.counts.putAll(state.get(0));
        }
    }

    private static class TestException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * An extension of {@link StandaloneCompletedCheckpointStore} which would record information of
     * last completed checkpoint id and the number of completed checkpoints.
     */
    private static class TestingCompletedCheckpointStore
            extends StandaloneCompletedCheckpointStore {

        TestingCompletedCheckpointStore() {
            super(1);
        }

        @Override
        public void addCheckpoint(
                CompletedCheckpoint checkpoint,
                CheckpointsCleaner checkpointsCleaner,
                Runnable postCleanup)
                throws Exception {
            super.addCheckpoint(checkpoint, checkpointsCleaner, postCleanup);
            // we record the information when adding completed checkpoint instead of
            // 'notifyCheckpointComplete' invoked
            // on task side to avoid race condition. See FLINK-13601.
            lastCompletedCheckpointId.set(checkpoint.getCheckpointID());
            numCompletedCheckpoints.incrementAndGet();
        }
    }

    /** Testing HA factory which needs to be public in order to be instantiatable. */
    public static class TestingHAFactory implements HighAvailabilityServicesFactory {

        @Override
        public HighAvailabilityServices createHAServices(
                Configuration configuration, Executor executor) {
            final CheckpointRecoveryFactory checkpointRecoveryFactory =
                    PerJobCheckpointRecoveryFactory.withoutCheckpointStoreRecovery(
                            maxCheckpoints -> new TestingCompletedCheckpointStore());
            return new EmbeddedHaServicesWithLeadershipControl(executor, checkpointRecoveryFactory);
        }
    }
}
