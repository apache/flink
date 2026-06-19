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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.finishJobVertex;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createSchedulerAndDeploy;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that blocking result partitions are properly released. */
class BlockingResultPartitionReleaseTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ScheduledExecutorService scheduledExecutorService;
    private ComponentMainThreadExecutor mainThreadExecutor;
    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    @BeforeEach
    void setup() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);
        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
    }

    @AfterEach
    void teardown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    void testMultipleConsumersForAdaptiveBatchScheduler() throws Exception {
        testResultPartitionConsumedByMultiConsumers(true);
    }

    @Test
    void testMultipleConsumersForDefaultScheduler() throws Exception {
        testResultPartitionConsumedByMultiConsumers(false);
    }

    private void testResultPartitionConsumedByMultiConsumers(boolean isAdaptive) throws Exception {
        int parallelism = 2;
        JobID jobId = new JobID();
        JobVertex producer = ExecutionGraphTestUtils.createNoOpVertex("producer", parallelism);
        JobVertex consumer1 = ExecutionGraphTestUtils.createNoOpVertex("consumer1", parallelism);
        JobVertex consumer2 = ExecutionGraphTestUtils.createNoOpVertex("consumer2", parallelism);

        TestingPartitionTracker partitionTracker = new TestingPartitionTracker();
        SchedulerBase scheduler =
                createSchedulerAndDeploy(
                        isAdaptive,
                        jobId,
                        producer,
                        new JobVertex[] {consumer1, consumer2},
                        DistributionPattern.ALL_TO_ALL,
                        new TestingBlobWriter(Integer.MAX_VALUE),
                        mainThreadExecutor,
                        ioExecutor,
                        partitionTracker,
                        EXECUTOR_RESOURCE.getExecutor(),
                        new Configuration());
        ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        assertThat(partitionTracker.releasedPartitions).isEmpty();

        CompletableFuture.runAsync(
                        () -> finishJobVertex(executionGraph, consumer1.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        assertThat(partitionTracker.releasedPartitions).isEmpty();

        CompletableFuture.runAsync(
                        () -> finishJobVertex(executionGraph, consumer2.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        assertThat(partitionTracker.releasedPartitions).hasSize((parallelism));
        for (int i = 0; i < parallelism; ++i) {
            ExecutionJobVertex ejv = checkNotNull(executionGraph.getJobVertex(producer.getID()));
            assertThat(
                            partitionTracker.releasedPartitions.stream()
                                    .map(ResultPartitionID::getPartitionId))
                    .containsExactlyInAnyOrder(
                            Arrays.stream(ejv.getProducedDataSets()[0].getPartitions())
                                    .map(IntermediateResultPartition::getPartitionId)
                                    .toArray(IntermediateResultPartitionID[]::new));
        }
    }

    private static class TestingPartitionTracker extends NoOpJobMasterPartitionTracker {

        private final List<ResultPartitionID> releasedPartitions = new ArrayList<>();

        @Override
        public void stopTrackingAndReleasePartitions(
                Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {
            releasedPartitions.addAll(checkNotNull(resultPartitionIds));
        }
    }
}
