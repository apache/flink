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
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

/** Tests for manually triggering checkpoints. */
@RunWith(Parameterized.class)
public class ManualCheckpointITCase extends AbstractTestBase {

    @Parameterized.Parameter public StorageSupplier storageSupplier;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private interface StorageSupplier extends Function<String, CheckpointStorage> {}

    @Parameterized.Parameters
    public static StorageSupplier[] parameters() throws IOException {
        return new StorageSupplier[] {
            path -> new JobManagerCheckpointStorage(), FileSystemCheckpointStorage::new
        };
    }

    @Test
    public void testTriggeringWhenPeriodicDisabled() throws Exception {
        int parallelism = MINI_CLUSTER_RESOURCE.getNumberSlots();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        storageSupplier.apply(temporaryFolder.newFolder().toURI().toString()));

        env.fromSource(
                        MockSource.continuous(parallelism).build(),
                        WatermarkStrategy.noWatermarks(),
                        "generator")
                .keyBy(key -> key % parallelism)
                .flatMap(new StatefulMapper())
                .sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();
        final JobID jobID = jobClient.getJobID();
        final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();

        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.RUNNING));
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);

        // wait for the checkpoint to be taken
        miniCluster.triggerCheckpoint(jobID).get();
        miniCluster.cancelJob(jobID).get();
        queryCompletedCheckpointsUntil(miniCluster, jobID, count -> count == 1);
    }

    @Test
    public void testTriggeringWhenPeriodicEnabled() throws Exception {
        int parallelism = MINI_CLUSTER_RESOURCE.getNumberSlots();
        final int checkpointingInterval = 500;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointingInterval);
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        storageSupplier.apply(temporaryFolder.newFolder().toURI().toString()));

        env.fromSource(
                        MockSource.continuous(parallelism).build(),
                        WatermarkStrategy.noWatermarks(),
                        "generator")
                .keyBy(key -> key % parallelism)
                .flatMap(new StatefulMapper())
                .sinkTo(new DiscardingSink<>());

        final JobClient jobClient = env.executeAsync();
        final JobID jobID = jobClient.getJobID();
        final MiniCluster miniCluster = MINI_CLUSTER_RESOURCE.getMiniCluster();

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
