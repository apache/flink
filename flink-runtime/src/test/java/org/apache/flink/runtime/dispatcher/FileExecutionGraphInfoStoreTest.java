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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.areExecutionGraphInfoMathPartially;
import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.createDefaultExecutionGraphInfoStore;
import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.generateJobDetails;
import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.generateTerminalExecutionGraphInfos;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FileExecutionGraphInfoStore}. */
class FileExecutionGraphInfoStoreTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @TempDir private Path tempDir;

    /**
     * Tests that we can put {@link ExecutionGraphInfo} into the {@link FileExecutionGraphInfoStore}
     * and that the graph is persisted.
     */
    @Test
    void testPut() throws IOException {
        assertPutJobGraphWithStatus(JobStatus.FINISHED);
    }

    /** Tests that a SUSPENDED job can be persisted. */
    @Test
    void testPutSuspendedJob() throws IOException {
        assertPutJobGraphWithStatus(JobStatus.SUSPENDED);
    }

    /** Tests that null is returned if we request an unknown JobID. */
    @Test
    void testUnknownGet() throws IOException {
        final File rootDir = TempDirUtils.newFolder(tempDir);

        try (final FileExecutionGraphInfoStore executionGraphStore =
                createDefaultExecutionGraphInfoStore(
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            assertThat(executionGraphStore.get(new JobID())).isNull();
        }
    }

    /** Tests that we obtain the correct jobs overview. */
    @Test
    void testStoredJobsOverview() throws IOException {
        final int numberExecutionGraphs = 10;
        final Collection<ExecutionGraphInfo> executionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final List<JobStatus> jobStatuses =
                executionGraphInfos.stream()
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        final JobsOverview expectedJobsOverview = JobsOverview.create(jobStatuses);

        final File rootDir = TempDirUtils.newFolder(tempDir);

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
            }

            assertThat(executionGraphInfoStore.getStoredJobsOverview())
                    .isEqualTo(expectedJobsOverview);
        }
    }

    /** Tests that we obtain the correct collection of available job details. */
    @Test
    void testAvailableJobDetails() throws IOException {
        final int numberExecutionGraphs = 10;
        final Collection<ExecutionGraphInfo> executionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final Collection<JobDetails> jobDetails = generateJobDetails(executionGraphInfos);

        final File rootDir = TempDirUtils.newFolder(tempDir);

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
            }

            assertThat(executionGraphInfoStore.getAvailableJobDetails())
                    .containsExactlyInAnyOrderElementsOf(jobDetails);
        }
    }

    /** Tests that an expired execution graph is removed from the execution graph store. */
    @Test
    void testExecutionGraphExpiration() throws Exception {
        final File rootDir = TempDirUtils.newFolder(tempDir);

        final Time expirationTime = Time.milliseconds(1L);

        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        final ManualTicker manualTicker = new ManualTicker();

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                new FileExecutionGraphInfoStore(
                        rootDir,
                        expirationTime,
                        Integer.MAX_VALUE,
                        10000L,
                        scheduledExecutor,
                        manualTicker)) {

            final ExecutionGraphInfo executionGraphInfo =
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setState(JobStatus.FINISHED)
                                    .build());

            executionGraphInfoStore.put(executionGraphInfo);

            // there should one execution graph
            assertThat(executionGraphInfoStore.size()).isOne();

            manualTicker.advanceTime(expirationTime.toMilliseconds(), TimeUnit.MILLISECONDS);

            // this should trigger the cleanup after expiration
            scheduledExecutor.triggerScheduledTasks();

            assertThat(executionGraphInfoStore.size()).isZero();

            assertThat(executionGraphInfoStore.get(executionGraphInfo.getJobId())).isNull();

            final File storageDirectory = executionGraphInfoStore.getStorageDir();

            // check that the persisted file has been deleted
            assertThat(storageDirectory.listFiles()).isEmpty();
        }
    }

    /** Tests that all persisted files are cleaned up after closing the store. */
    @Test
    void testCloseCleansUp() throws IOException {
        final File rootDir = TempDirUtils.newFolder(tempDir);

        assertThat(rootDir.listFiles()).isEmpty();

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {

            assertThat(rootDir.listFiles()).hasSize(1);

            final File storageDirectory = executionGraphInfoStore.getStorageDir();

            assertThat(storageDirectory.listFiles()).isEmpty();

            executionGraphInfoStore.put(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setState(JobStatus.FINISHED)
                                    .build()));

            assertThat(storageDirectory.listFiles()).hasSize(1);
        }

        assertThat(rootDir.listFiles()).isEmpty();
    }

    /** Tests that evicted {@link ExecutionGraphInfo} are loaded from disk again. */
    @Test
    void testCacheLoading() throws IOException {
        final File rootDir = TempDirUtils.newFolder(tempDir);

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                new FileExecutionGraphInfoStore(
                        rootDir,
                        Time.hours(1L),
                        Integer.MAX_VALUE,
                        100L << 10,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                        Ticker.systemTicker())) {

            final LoadingCache<JobID, ExecutionGraphInfo> executionGraphInfoCache =
                    executionGraphInfoStore.getExecutionGraphInfoCache();

            Collection<ExecutionGraphInfo> executionGraphInfos = new ArrayList<>(64);

            boolean continueInserting = true;

            // insert execution graphs until the first one got evicted
            while (continueInserting) {
                // has roughly a size of 1.4 KB
                final ExecutionGraphInfo executionGraphInfo =
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder()
                                        .setState(JobStatus.FINISHED)
                                        .build());

                executionGraphInfoStore.put(executionGraphInfo);

                executionGraphInfos.add(executionGraphInfo);

                continueInserting = executionGraphInfoCache.size() == executionGraphInfos.size();
            }

            final File storageDirectory = executionGraphInfoStore.getStorageDir();

            assertThat(storageDirectory.listFiles()).hasSameSizeAs(executionGraphInfos);

            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                assertThat(
                                areExecutionGraphInfoMathPartially(
                                        executionGraphInfoStore.get(executionGraphInfo.getJobId()),
                                        executionGraphInfo))
                        .isTrue();
            }
        }
    }

    /**
     * Tests that the size of {@link FileExecutionGraphInfoStore} is no more than the configured max
     * capacity and the old execution graphs will be purged if the total added number exceeds the
     * max capacity.
     */
    @Test
    void testMaximumCapacity() throws IOException {
        final File rootDir = TempDirUtils.newFolder(tempDir);

        final int maxCapacity = 10;
        final int numberExecutionGraphs = 10;

        final Collection<ExecutionGraphInfo> oldExecutionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);
        final Collection<ExecutionGraphInfo> newExecutionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final Collection<JobDetails> jobDetails = generateJobDetails(newExecutionGraphInfos);

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                new FileExecutionGraphInfoStore(
                        rootDir,
                        Time.hours(1L),
                        maxCapacity,
                        10000L,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                        Ticker.systemTicker())) {

            for (ExecutionGraphInfo executionGraphInfo : oldExecutionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
                // no more than the configured maximum capacity
                assertThat(executionGraphInfoStore.size()).isLessThanOrEqualTo(maxCapacity);
            }

            for (ExecutionGraphInfo executionGraphInfo : newExecutionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
                // equals to the configured maximum capacity
                assertThat(executionGraphInfoStore.size()).isEqualTo(maxCapacity);
            }

            // the older execution graphs are purged
            assertThat(executionGraphInfoStore.getAvailableJobDetails())
                    .containsExactlyInAnyOrderElementsOf(jobDetails);
        }
    }

    /** Tests that a session cluster can terminate gracefully when jobs are still running. */
    @Test
    void testPutSuspendedJobOnClusterShutdown() throws Exception {
        File rootDir = TempDirUtils.newFolder(tempDir);
        try (final MiniCluster miniCluster =
                new ExecutionGraphInfoStoreTestUtils.PersistingMiniCluster(
                        new MiniClusterConfiguration.Builder().withRandomPorts().build(),
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {
            miniCluster.start();
            final JobVertex vertex = new JobVertex("blockingVertex");
            // The adaptive scheduler expects that every vertex has a configured parallelism
            vertex.setParallelism(1);
            vertex.setInvokableClass(
                    ExecutionGraphInfoStoreTestUtils.SignallingBlockingNoOpInvokable.class);
            final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);
            miniCluster.submitJob(jobGraph);
            ExecutionGraphInfoStoreTestUtils.SignallingBlockingNoOpInvokable.LATCH.await();
        }
    }

    private void assertPutJobGraphWithStatus(JobStatus jobStatus) throws IOException {
        final ExecutionGraphInfo dummyExecutionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().setState(jobStatus).build());
        final File rootDir = TempDirUtils.newFolder(tempDir);

        try (final FileExecutionGraphInfoStore executionGraphStore =
                createDefaultExecutionGraphInfoStore(
                        rootDir,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {

            final File storageDirectory = executionGraphStore.getStorageDir();

            // check that the storage directory is empty
            assertThat(storageDirectory.listFiles()).isEmpty();

            executionGraphStore.put(dummyExecutionGraphInfo);

            // check that we have persisted the given execution graph
            assertThat(storageDirectory.listFiles()).hasSize(1);

            assertThat(
                            ExecutionGraphInfoStoreTestUtils.areExecutionGraphInfoMathPartially(
                                    executionGraphStore.get(dummyExecutionGraphInfo.getJobId()),
                                    dummyExecutionGraphInfo))
                    .isTrue();
        }
    }
}
