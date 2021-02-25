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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.checkpointing.utils.AccumulatingIntegerSink;
import org.apache.flink.test.checkpointing.utils.CancellingIntegerSource;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Tests recovery from a snapshot created in different UC mode (i.e. unaligned checkpoints
 * enabled/disabled).
 */
@RunWith(Parameterized.class)
@Category(FailsWithAdaptiveScheduler.class) // FLINK-21333
public class UnalignedCheckpointCompatibilityITCase extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int TOTAL_ELEMENTS = 20;
    private static final int FIRST_RUN_EL_COUNT = TOTAL_ELEMENTS / 2;
    private static final int FIRST_RUN_BACKPRESSURE_MS = 100; // per element
    private static final int PARALLELISM = 1;

    private final boolean startAligned;
    private final CheckpointType type;

    private static MiniClusterWithClientResource miniCluster;

    @Parameterized.Parameters(name = "type: {0}, startAligned: {1}")
    public static Object[][] parameters() {
        return new Object[][] {
            {CHECKPOINT, true},
            {CHECKPOINT, false},
            {SAVEPOINT, true},
            {SAVEPOINT, false},
        };
    }

    public UnalignedCheckpointCompatibilityITCase(CheckpointType type, boolean startAligned) {
        this.startAligned = startAligned;
        this.type = type;
    }

    @BeforeClass
    public static void setupMiniCluster() throws Exception {
        File folder = temporaryFolder.getRoot();
        final Configuration conf = new Configuration();
        conf.set(CHECKPOINTS_DIRECTORY, folder.toURI().toString());
        // prevent deletion of checkpoint files while it's being checked and used
        conf.set(MAX_RETAINED_CHECKPOINTS, Integer.MAX_VALUE);

        miniCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(conf)
                                .build());
        miniCluster.before();
    }

    @AfterClass
    public static void teardownMiniCluster() {
        miniCluster.after();
    }

    @Before
    public void cleanDirectory() throws IOException {
        FileUtils.cleanDirectory(temporaryFolder.getRoot());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        Tuple2<String, Map<String, Object>> pathAndAccumulators =
                type.isSavepoint() ? runAndTakeSavepoint() : runAndTakeExternalCheckpoint();
        String savepointPath = pathAndAccumulators.f0;
        Map<String, Object> accumulatorsBeforeBarrier = pathAndAccumulators.f1;
        Map<String, Object> accumulatorsAfterBarrier =
                runFromSavepoint(savepointPath, !startAligned, TOTAL_ELEMENTS);
        if (type.isSavepoint()) { // consistency can only be checked for savepoints because output
            // is lost for ext. checkpoints
            assertEquals(
                    intRange(0, TOTAL_ELEMENTS),
                    extractAndConcat(accumulatorsBeforeBarrier, accumulatorsAfterBarrier));
        }
    }

    private Tuple2<String, Map<String, Object>> runAndTakeSavepoint() throws Exception {
        JobClient jobClient = submitJobInitially(env(startAligned, 0));
        waitForAllTaskRunning(
                () -> miniCluster.getMiniCluster().getExecutionGraph(jobClient.getJobID()).get());
        Thread.sleep(FIRST_RUN_BACKPRESSURE_MS); // wait for some backpressure from sink

        Future<Map<String, Object>> accFuture =
                jobClient
                        .getJobExecutionResult()
                        .thenApply(JobExecutionResult::getAllAccumulatorResults);
        Future<String> savepointFuture =
                jobClient.stopWithSavepoint(false, tempFolder().toURI().toString());
        return new Tuple2<>(savepointFuture.get(), accFuture.get());
    }

    private Tuple2<String, Map<String, Object>> runAndTakeExternalCheckpoint() throws Exception {
        JobClient jobClient = submitJobInitially(env(startAligned, 100));
        // structure: root/attempt/checkpoint/_metadata
        File attemptDir = waitForChild(temporaryFolder.getRoot(), (dir, name) -> true);
        File checkpointDir = waitForChild(attemptDir, (dir, name) -> name.startsWith("chk-"));
        File metadata = waitForChild(checkpointDir, (dir, name) -> name.equals("_metadata"));
        cancelJob(jobClient);
        return new Tuple2<>(metadata.getParentFile().toString(), emptyMap());
    }

    private static JobClient submitJobInitially(StreamExecutionEnvironment env) throws Exception {
        return env.executeAsync(dag(FIRST_RUN_EL_COUNT, true, FIRST_RUN_BACKPRESSURE_MS, env));
    }

    private Map<String, Object> runFromSavepoint(String path, boolean isAligned, int totalCount)
            throws Exception {
        StreamExecutionEnvironment env = env(isAligned, 50);
        final StreamGraph streamGraph = dag(totalCount, false, 0, env);
        streamGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(path));
        return env.execute(streamGraph).getJobExecutionResult().getAllAccumulatorResults();
    }

    @SuppressWarnings({"ConstantConditions"})
    private static File waitForChild(File dir, FilenameFilter filenameFilter)
            throws InterruptedException {
        File[] files = dir.listFiles(filenameFilter);
        while (files.length == 0) {
            Thread.sleep(50);
            files = dir.listFiles(filenameFilter);
        }
        return Arrays.stream(files).max(Comparator.naturalOrder()).get();
    }

    private void cancelJob(JobClient jobClient)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        jobClient.cancel().get();
        try {
            jobClient.getJobExecutionResult(); // wait for cancellation
        } catch (Exception e) {
            // ignore cancellation exception
        }
    }

    @SuppressWarnings("unchecked")
    private StreamExecutionEnvironment env(boolean isAligned, int checkpointingInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env.getCheckpointConfig().enableUnalignedCheckpoints(!isAligned);
        env.getCheckpointConfig().setAlignmentTimeout(0);
        env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        if (checkpointingInterval > 0) {
            env.enableCheckpointing(checkpointingInterval);
        }
        return env;
    }

    private static StreamGraph dag(
            int numElements,
            boolean continueAfterNumElementsReached,
            int sinkDelayMillis,
            StreamExecutionEnvironment env) {
        env.addSource(CancellingIntegerSource.upTo(numElements, continueAfterNumElementsReached))
                .addSink(new AccumulatingIntegerSink(sinkDelayMillis));
        return env.getStreamGraph();
    }

    private static List<Integer> intRange(int from, int to) {
        return IntStream.range(from, to).boxed().collect(Collectors.toList());
    }

    private static List<Integer> extractAndConcat(Map<String, Object>... accumulators) {
        return Stream.of(accumulators)
                .map(AccumulatingIntegerSink::getOutput)
                .peek(l -> checkState(!l.isEmpty()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private File tempFolder() throws IOException {
        return temporaryFolder.newFolder();
    }
}
