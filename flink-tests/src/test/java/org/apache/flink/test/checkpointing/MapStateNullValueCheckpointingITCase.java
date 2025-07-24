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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Either;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for checkpointing and restoring a job with a {@link MapState} that contains null
 * user values.
 */
@RunWith(Parameterized.class)
public class MapStateNullValueCheckpointingITCase extends TestLogger {

    @Parameterized.Parameters(name = "stateBackend : {0}, snapshotType : {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"rocksdb", Either.Left(CheckpointType.FULL)},
                    {"rocksdb", Either.Left(CheckpointType.INCREMENTAL)},
                    {"rocksdb", Either.Right(SavepointFormatType.CANONICAL)},
                    {"rocksdb", Either.Right(SavepointFormatType.NATIVE)},
                    {"hashmap", Either.Left(CheckpointType.FULL)},
                    {"hashmap", Either.Left(CheckpointType.INCREMENTAL)},
                    {"hashmap", Either.Right(SavepointFormatType.CANONICAL)},
                    {"hashmap", Either.Right(SavepointFormatType.NATIVE)},
                    {"forst", Either.Left(CheckpointType.FULL)},
                    {"forst", Either.Left(CheckpointType.INCREMENTAL)},
                    {"forst", Either.Right(SavepointFormatType.NATIVE)}
                });
    }

    @Parameterized.Parameter public String stateBackend;

    @Parameterized.Parameter(1)
    public Either<CheckpointType, SavepointFormatType> snapshotType;

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    private static MiniClusterWithClientResource cluster;

    @Before
    public void before() throws Exception {
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(new Configuration())
                                .setNumberTaskManagers(10)
                                .setNumberSlotsPerTaskManager(1)
                                .build());
        cluster.before();

        StatefulMapper.firstRunFuture = new CompletableFuture<>();
        StatefulMapper.secondRunFuture = new CompletableFuture<>();
    }

    @After
    public void after() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test
    public void testMapStateWithNullValueCheckpointingAndRestore() throws Exception {
        final String savepointPath = runJobAndTakeSnapshot();
        assertThat(savepointPath).isNotEmpty();
        restoreAndVerify(savepointPath);
    }

    private String runJobAndTakeSnapshot() throws Exception {
        Configuration conf = new Configuration();
        conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tmpFolder.newFolder().toURI().toString());
        conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, RETAIN_ON_CANCELLATION);
        conf.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, tmpFolder.newFolder().toURI().toString());
        conf.set(StateBackendOptions.STATE_BACKEND, stateBackend);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Data Generator Source")
                .keyBy(v -> 0)
                .map(new StatefulMapper(true))
                .sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        MiniCluster miniCluster = cluster.getMiniCluster();
        miniCluster.submitJob(jobGraph).get();

        JobID jobID = jobGraph.getJobID();

        // Wait for the job to be running and the state to be populated.
        StatefulMapper.firstRunFuture.get(2, TimeUnit.MINUTES);

        if (snapshotType.isLeft()) {
            // Trigger a checkpoint
            cluster.getClusterClient()
                    .triggerCheckpoint(jobID, snapshotType.left())
                    .get(2, TimeUnit.MINUTES);
            String checkpointPath =
                    CommonTestUtils.getLatestCompletedCheckpointPath(jobID, miniCluster)
                            .<NoSuchElementException>orElseThrow(
                                    () -> {
                                        throw new NoSuchElementException(
                                                "No checkpoint was created yet");
                                    });
            cluster.getClusterClient().cancel(jobID);
            return checkpointPath;
        } else {
            // Trigger a savepoint
            return cluster.getClusterClient()
                    .stopWithSavepoint(jobID, false, null, snapshotType.right())
                    .get(2, TimeUnit.MINUTES);
        }
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(
                (GeneratorFunction<Long, Long>) value -> value,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(5),
                BasicTypeInfo.LONG_TYPE_INFO);
    }

    private void restoreAndVerify(String savepointPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tmpFolder.newFolder().toURI().toString());
        conf.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY, tmpFolder.newFolder().toURI().toString());
        conf.set(StateBackendOptions.STATE_BACKEND, stateBackend);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Data Generator Source")
                .keyBy(v -> 0)
                .map(new StatefulMapper(false))
                .sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

        MiniCluster miniCluster = cluster.getMiniCluster();
        miniCluster.submitJob(jobGraph).get();

        Map<String, String> restoredState = StatefulMapper.secondRunFuture.get(2, TimeUnit.MINUTES);

        assertThat(restoredState.get("key")).isEqualTo("value");
        assertThat(restoredState.get("null-key")).isNull();
        assertThat(restoredState.containsKey("null-key")).isTrue();
    }

    private static class StatefulMapper extends RichMapFunction<Long, Long> {

        static CompletableFuture<Void> firstRunFuture;
        static CompletableFuture<Map<String, String>> secondRunFuture;

        private final boolean isFirstRun;
        private boolean hasPopulated;
        private transient MapState<String, String> mapState;

        StatefulMapper(boolean isFirstRun) {
            this.isFirstRun = isFirstRun;
        }

        @Override
        public void open(OpenContext context) {
            MapStateDescriptor<String, String> mapStateDescriptor =
                    new MapStateDescriptor<>(
                            "map-state",
                            BasicTypeInfo.STRING_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);

            ValueStateDescriptor<Boolean> hasPopulatedStateDescriptor =
                    new ValueStateDescriptor<>("has-populated", BasicTypeInfo.BOOLEAN_TYPE_INFO);
            hasPopulated = false;
        }

        @Override
        public Long map(Long value) throws Exception {
            if (hasPopulated) {
                return value;
            }
            if (isFirstRun) {
                mapState.put("key", "value");
                mapState.put("null-key", null);
                firstRunFuture.complete(null);
            } else {
                // This is the first record for this key after restore.
                // Verify that the state is correctly restored.
                Map<String, String> restoredState = new HashMap<>();
                restoredState.put("key", mapState.get("key"));
                restoredState.put("null-key", mapState.get("null-key"));

                secondRunFuture.complete(restoredState);
            }
            hasPopulated = true;
            return value;
        }
    }
}
