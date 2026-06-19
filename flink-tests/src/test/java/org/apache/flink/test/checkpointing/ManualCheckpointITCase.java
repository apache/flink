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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.CheckpointStorageUtils;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/** Tests for manually triggering checkpoints. */
@ExtendWith(ParameterizedTestExtension.class)
class ManualCheckpointITCase extends AbstractTestBase {

    @Parameter private StorageConfigurer storageConfigurer;

    @TempDir private File tempDir;

    private interface StorageConfigurer extends BiConsumer<String, StreamExecutionEnvironment> {}

    @Parameters
    private static Collection<StorageConfigurer> parameters() {
        return Arrays.asList(
                (s, env) -> CheckpointStorageUtils.configureJobManagerCheckpointStorage(env),
                (s, env) -> CheckpointStorageUtils.configureFileSystemCheckpointStorage(env, s));
    }

    @TestTemplate
    void testTriggeringWhenPeriodicDisabled(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        int parallelism = MINI_CLUSTER_EXTENSION.getNumberSlots();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        storageConfigurer.accept(tempDir.toURI().toString(), env);

        env.fromSource(
                        MockSource.continuous(parallelism).build(),
                        WatermarkStrategy.noWatermarks(),
                        "generator")
                .keyBy(key -> key % parallelism)
                .flatMap(new StatefulMapper())
                .sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();
        final JobID jobID = jobClient.getJobID();

        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);

        // wait for the checkpoint to be taken
        miniCluster.triggerCheckpoint(jobID).get();
        miniCluster.cancelJob(jobID).get();
        queryCompletedCheckpointsUntil(miniCluster, jobID, count -> count == 1);
    }

    @TestTemplate
    void testTriggeringWhenPeriodicEnabled(@InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        int parallelism = MINI_CLUSTER_EXTENSION.getNumberSlots();
        final int checkpointingInterval = 500;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointingInterval);
        storageConfigurer.accept(tempDir.toURI().toString(), env);

        env.fromSource(
                        MockSource.continuous(parallelism).build(),
                        WatermarkStrategy.noWatermarks(),
                        "generator")
                .keyBy(key -> key % parallelism)
                .flatMap(new StatefulMapper())
                .sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();
        final JobID jobID = jobClient.getJobID();

        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);
        CommonTestUtils.waitUntilCondition(
                () -> queryCompletedCheckpoints(miniCluster, jobID) > 0L,
                checkpointingInterval / 2);

        final long numberOfPeriodicCheckpoints = queryCompletedCheckpoints(miniCluster, jobID);
        // wait for the checkpoint to be taken
        miniCluster.triggerCheckpoint(jobID).get();
        miniCluster.cancelJob(jobID).get();
        queryCompletedCheckpointsUntil(
                miniCluster, jobID, count -> count >= numberOfPeriodicCheckpoints + 1);
    }

    private void queryCompletedCheckpointsUntil(
            MiniCluster miniCluster, JobID jobID, Predicate<Long> predicate) throws Exception {

        long counts;
        do {
            counts = queryCompletedCheckpoints(miniCluster, jobID);
        } while (!predicate.test(counts));
    }

    private long queryCompletedCheckpoints(MiniCluster miniCluster, JobID jobID)
            throws InterruptedException, ExecutionException {
        return miniCluster
                .getArchivedExecutionGraph(jobID)
                .get()
                .getCheckpointStatsSnapshot()
                .getCounts()
                .getNumberOfCompletedCheckpoints();
    }

    private static final class StatefulMapper extends RichFlatMapFunction<Integer, Long> {
        private ValueState<Long> count;

        @Override
        public void open(OpenContext openContext) throws Exception {
            count =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "count", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public void flatMap(Integer value, Collector<Long> out) throws Exception {
            final long sum = Optional.ofNullable(count.value()).orElse(0L) + value;
            count.update(sum);
            out.collect(sum);
        }
    }
}
