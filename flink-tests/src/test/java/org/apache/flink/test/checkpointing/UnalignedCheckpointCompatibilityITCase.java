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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.checkpointing.utils.AccumulatingIntegerSink;
import org.apache.flink.test.checkpointing.utils.CancellingIntegerSource;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests recovery from a snapshot created in different UC mode (i.e. unaligned checkpoints
 * enabled/disabled).
 */
@ExtendWith({TestLoggerExtension.class, ParameterizedTestExtension.class})
class UnalignedCheckpointCompatibilityITCase {

    private static final int TOTAL_ELEMENTS = 20;
    private static final int FIRST_RUN_EL_COUNT = TOTAL_ELEMENTS / 2;
    private static final int FIRST_RUN_BACKPRESSURE_MS = 100; // per element
    private static final int PARALLELISM = 1;

    @Parameter private SnapshotType type;

    @Parameter(1)
    private boolean startAligned;

    @TempDir private static File tempDir;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .build());

    @Parameters
    private static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {CHECKPOINT, true},
                    {CHECKPOINT, false},
                    {SavepointType.savepoint(SavepointFormatType.CANONICAL), true},
                    {SavepointType.savepoint(SavepointFormatType.CANONICAL), false},
                });
    }

    private static Configuration getConfiguration() {
        final Configuration conf = new Configuration();
        // prevent deletion of checkpoint files while it's being checked and used
        conf.set(MAX_RETAINED_CHECKPOINTS, Integer.MAX_VALUE);
        return conf;
    }

    @TestTemplate
    @SuppressWarnings("unchecked")
    void test(@InjectMiniCluster MiniCluster miniCluster) throws Exception {
        Tuple2<String, Map<String, Object>> pathAndAccumulators =
                type.isSavepoint()
                        ? runAndTakeSavepoint(miniCluster)
                        : runAndTakeExternalCheckpoint(miniCluster);
        String savepointPath = pathAndAccumulators.f0;
        Map<String, Object> accumulatorsBeforeBarrier = pathAndAccumulators.f1;
        Map<String, Object> accumulatorsAfterBarrier =
                runFromSavepoint(savepointPath, !startAligned, TOTAL_ELEMENTS);
        if (type.isSavepoint()) { // consistency can only be checked for savepoints because output
            // is lost for ext. checkpoints
            assertThat(extractAndConcat(accumulatorsBeforeBarrier, accumulatorsAfterBarrier))
                    .isEqualTo(intRange(0, TOTAL_ELEMENTS));
        }
    }

    private Tuple2<String, Map<String, Object>> runAndTakeSavepoint(MiniCluster miniCluster)
            throws Exception {
        JobClient jobClient = submitJobInitially(env(startAligned, 0));
        waitForAllTaskRunning(
                () -> miniCluster.getExecutionGraph(jobClient.getJobID()).get(), false);
        Thread.sleep(FIRST_RUN_BACKPRESSURE_MS); // wait for some backpressure from sink

        Future<Map<String, Object>> accFuture =
                jobClient
                        .getJobExecutionResult()
                        .thenApply(JobExecutionResult::getAllAccumulatorResults);
        Future<String> savepointFuture =
                jobClient.stopWithSavepoint(
                        false,
                        TempDirUtils.newFolder(tempDir.toPath()).toURI().toString(),
                        SavepointFormatType.CANONICAL);
        return new Tuple2<>(savepointFuture.get(), accFuture.get());
    }

    private Tuple2<String, Map<String, Object>> runAndTakeExternalCheckpoint(
            MiniCluster miniCluster) throws Exception {
        JobClient jobClient = submitJobInitially(env(startAligned, Integer.MAX_VALUE));
        waitForAllTaskRunning(
                () -> miniCluster.getExecutionGraph(jobClient.getJobID()).get(), false);
        Thread.sleep(FIRST_RUN_BACKPRESSURE_MS); // wait for some backpressure from sink

        String checkpointPath = miniCluster.triggerCheckpoint(jobClient.getJobID()).get();
        cancelJob(jobClient);
        return new Tuple2<>(checkpointPath, emptyMap());
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

    private void cancelJob(JobClient jobClient)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        jobClient.cancel().get();
        try {
            jobClient.getJobExecutionResult(); // wait for cancellation
        } catch (Exception e) {
            // ignore cancellation exception
        }
    }

    private StreamExecutionEnvironment env(boolean isAligned, int checkpointingInterval) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.getCheckpointConfig().enableUnalignedCheckpoints(!isAligned);
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ZERO);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION);
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
}
