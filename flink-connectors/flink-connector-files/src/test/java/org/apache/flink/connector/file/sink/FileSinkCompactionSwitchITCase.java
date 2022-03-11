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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.connector.file.sink.FileSink.DefaultRowFormatBuilder;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.IntDecoder;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.IntEncoder;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.ModuloBucketAssigner;
import org.apache.flink.connector.file.sink.utils.PartSizeAndCheckpointRollingPolicy;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests of switching on or off compaction for the {@link FileSink}. */
public class FileSinkCompactionSwitchITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    protected static final int NUM_SOURCES = 4;

    protected static final int NUM_SINKS = 3;

    protected static final int NUM_RECORDS = 10000;

    protected static final int NUM_BUCKETS = 4;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();

    private String latchId;

    @Before
    public void setup() {
        this.latchId = UUID.randomUUID().toString();
        // Wait for 3 checkpoints to ensure that the coordinator and all compactors have state
        LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 3));
    }

    @After
    public void teardown() {
        LATCH_MAP.remove(latchId);
    }

    @Test
    public void testSwitchNeverEnabledToEnabled() throws Exception {
        String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        FileSink<Integer> originFileSink = createFileSink(path, null, false);
        FileSink<Integer> restoredFileSink =
                createFileSink(path, createFileCompactStrategy(), false);
        testSwitchingCompaction(path, originFileSink, restoredFileSink);
    }

    @Test
    public void testSwitchDisabledToEnabled() throws Exception {
        String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        FileSink<Integer> originFileSink = createFileSink(path, null, true);
        FileSink<Integer> restoredFileSink =
                createFileSink(path, createFileCompactStrategy(), false);
        testSwitchingCompaction(path, originFileSink, restoredFileSink);
    }

    @Test
    public void testSwitchEnabledToDisabled() throws Exception {
        String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        FileSink<Integer> originFileSink = createFileSink(path, createFileCompactStrategy(), false);
        FileSink<Integer> restoredFileSink = createFileSink(path, null, true);
        testSwitchingCompaction(path, originFileSink, restoredFileSink);
    }

    @Test
    public void testSwitchEnabledToDisabledImproperly() throws Exception {
        String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        FileSink<Integer> originFileSink = createFileSink(path, createFileCompactStrategy(), false);
        FileSink<Integer> restoredFileSink = createFileSink(path, null, false);
        try {
            testSwitchingCompaction(path, originFileSink, restoredFileSink);
        } catch (JobExecutionException expected) {
            return;
        }
        fail("Job is not failing when compaction is disabled improperly");
    }

    private void testSwitchingCompaction(
            String path, FileSink<Integer> originFileSink, FileSink<Integer> restoredFileSink)
            throws Exception {
        String cpPath = "file://" + TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        SharedReference<ConcurrentHashMap<Integer, Integer>> sendCountMap =
                sharedObjects.add(new ConcurrentHashMap<>());
        JobGraph jobGraph = createJobGraph(cpPath, originFileSink, false, sendCountMap);
        JobGraph restoringJobGraph = createJobGraph(cpPath, restoredFileSink, true, sendCountMap);

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.submitJob(jobGraph);

            LATCH_MAP.get(latchId).await();

            String savepointPath =
                    miniCluster
                            .triggerSavepoint(
                                    jobGraph.getJobID(),
                                    TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                                    true,
                                    SavepointFormatType.CANONICAL)
                            .get();

            // We wait for two successful checkpoints in sources before shutting down. This ensures
            // that the sink can commit its data.
            LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 2));

            restoringJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, false));
            miniCluster.executeJobBlocking(restoringJobGraph);
        }

        checkIntegerSequenceSinkOutput(path, sendCountMap.get(), NUM_BUCKETS, NUM_SOURCES);
    }

    private JobGraph createJobGraph(
            String cpPath,
            FileSink<Integer> fileSink,
            boolean isFinite,
            SharedReference<ConcurrentHashMap<Integer, Integer>> sendCountMap) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        // disable changelog state in case it's randomly enabled, since it will fail the savepoint
        config.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
        env.configure(config, getClass().getClassLoader());

        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(cpPath));
        env.setStateBackend(new HashMapStateBackend());

        env.addSource(new CountingTestSource(latchId, NUM_RECORDS, isFinite, sendCountMap))
                .setParallelism(NUM_SOURCES)
                .sinkTo(fileSink)
                .uid("sink")
                .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private FileSink<Integer> createFileSink(
            String path, FileCompactStrategy compactStrategy, boolean disableCompact) {
        DefaultRowFormatBuilder<Integer> sinkBuilder =
                FileSink.forRowFormat(new Path(path), new IntEncoder())
                        .withBucketAssigner(new ModuloBucketAssigner(NUM_BUCKETS))
                        .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy<>(1024, false));

        if (compactStrategy != null) {
            sinkBuilder = sinkBuilder.enableCompact(compactStrategy, createFileCompactor());
        } else if (disableCompact) {
            sinkBuilder = sinkBuilder.disableCompact();
        }

        return sinkBuilder.build();
    }

    private static FileCompactor createFileCompactor() {
        return new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(IntDecoder::new));
    }

    private static FileCompactStrategy createFileCompactStrategy() {
        return FileCompactStrategy.Builder.newBuilder().enableCompactionOnCheckpoint(2).build();
    }

    private static void checkIntegerSequenceSinkOutput(
            String path, Map<Integer, Integer> countMap, int numBuckets, int numSources)
            throws Exception {
        assertEquals(numSources, countMap.size());

        File dir = new File(path);
        String[] subDirNames = dir.list();
        assertNotNull(subDirNames);

        Arrays.sort(subDirNames, Comparator.comparingInt(Integer::parseInt));
        assertEquals(numBuckets, subDirNames.length);
        for (int i = 0; i < numBuckets; ++i) {
            assertEquals(Integer.toString(i), subDirNames[i]);

            // now check its content
            File bucketDir = new File(path, subDirNames[i]);
            assertTrue(
                    bucketDir.getAbsolutePath() + " Should be a existing directory",
                    bucketDir.isDirectory());

            Map<Integer, Integer> counts = new HashMap<>();
            File[] files = bucketDir.listFiles(f -> !f.getName().startsWith("."));
            assertNotNull(files);

            for (File file : files) {
                assertTrue(file.isFile());

                try (DataInputStream dataInputStream =
                        new DataInputStream(new FileInputStream(file))) {
                    while (true) {
                        int value = dataInputStream.readInt();
                        counts.compute(value, (k, v) -> v == null ? 1 : v + 1);
                    }
                } catch (EOFException e) {
                    // End the reading
                }
            }

            int bucketId = i;
            int expectedCount =
                    countMap.values().stream()
                            .map(
                                    numRecords ->
                                            numRecords / numBuckets
                                                    + (bucketId < numRecords % numBuckets ? 1 : 0))
                            .mapToInt(num -> num)
                            .max()
                            .getAsInt();
            assertEquals(expectedCount, counts.size());

            List<Integer> countList = new ArrayList<>(countMap.values());
            Collections.sort(countList);
            for (int j = 0; j < countList.size(); j++) {
                int rangeFrom = j == 0 ? 0 : countList.get(j - 1);
                rangeFrom =
                        bucketId
                                + (rangeFrom % numBuckets == 0
                                        ? rangeFrom
                                        : (rangeFrom + numBuckets - rangeFrom % numBuckets));
                int rangeTo = countList.get(j);
                for (int k = rangeFrom; k < rangeTo; k += numBuckets) {
                    assertEquals(
                            "The record "
                                    + k
                                    + " should occur "
                                    + (numBuckets - j)
                                    + " times, "
                                    + " but only occurs "
                                    + counts.getOrDefault(k, 0)
                                    + "time",
                            numBuckets - j,
                            counts.getOrDefault(k, 0).intValue());
                }
            }
        }
    }

    private static class CountingTestSource extends RichParallelSourceFunction<Integer>
            implements CheckpointListener, CheckpointedFunction {

        private final String latchId;

        private final int numberOfRecords;

        private final boolean isFinite;

        private final SharedReference<ConcurrentHashMap<Integer, Integer>> sendCountMap;

        private ListState<Integer> nextValueState;

        private int nextValue;

        private volatile boolean isCanceled;

        private volatile boolean snapshottedAfterAllRecordsOutput;

        private volatile boolean isWaitingCheckpointComplete;

        public CountingTestSource(
                String latchId,
                int numberOfRecords,
                boolean isFinite,
                SharedReference<ConcurrentHashMap<Integer, Integer>> sendCountMap) {
            this.latchId = latchId;
            this.numberOfRecords = numberOfRecords;
            this.isFinite = isFinite;
            this.sendCountMap = sendCountMap;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            nextValueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

            if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
                nextValue = nextValueState.get().iterator().next();
            }
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            // If we are not going to trigger failover or we have already triggered failover,
            // run until finished.
            sendRecordsUntil(isFinite ? (nextValue + numberOfRecords) : Integer.MAX_VALUE, ctx);

            // Wait the last checkpoint to commit all the pending records.
            isWaitingCheckpointComplete = true;
            CountDownLatch latch = LATCH_MAP.get(latchId);
            latch.await();
        }

        private void sendRecordsUntil(int targetNumber, SourceContext<Integer> ctx)
                throws InterruptedException {
            while (!isCanceled && nextValue < targetNumber) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextValue++);
                    if (!isFinite && nextValue % 100 == 0) {
                        // slow down the source in case too many records are sent
                        Thread.sleep(1);
                    }
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            nextValueState.update(Collections.singletonList(nextValue));
            sendCountMap.consumeSync(
                    m -> m.put(getRuntimeContext().getIndexOfThisSubtask(), nextValue));

            if (isWaitingCheckpointComplete) {
                snapshottedAfterAllRecordsOutput = true;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (!isFinite || (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput)) {
                CountDownLatch latch = LATCH_MAP.get(latchId);
                latch.countDown();
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
