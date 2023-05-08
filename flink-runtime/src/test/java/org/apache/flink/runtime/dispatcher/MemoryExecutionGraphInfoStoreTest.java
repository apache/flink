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
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.generateJobDetails;
import static org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStoreTestUtils.generateTerminalExecutionGraphInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link MemoryExecutionGraphInfoStore}. */
public class MemoryExecutionGraphInfoStoreTest extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    /**
     * Tests that we can put {@link ExecutionGraphInfo} into the {@link
     * MemoryExecutionGraphInfoStore} and that the graph is persisted.
     */
    @Test
    public void testPut() throws IOException {
        assertPutJobGraphWithStatus(JobStatus.FINISHED);
    }

    /** Tests that a SUSPENDED job can be persisted. */
    @Test
    public void testPutSuspendedJob() throws IOException {
        assertPutJobGraphWithStatus(JobStatus.SUSPENDED);
    }

    /** Tests that null is returned if we request an unknown JobID. */
    @Test
    public void testUnknownGet() throws IOException {

        try (final MemoryExecutionGraphInfoStore executionGraphStore =
                createMemoryExecutionGraphInfoStore()) {
            assertThat(executionGraphStore.get(new JobID()), Matchers.nullValue());
        }
    }

    /** Tests that we obtain the correct jobs overview. */
    @Test
    public void testStoredJobsOverview() throws IOException {
        final int numberExecutionGraphs = 10;
        final Collection<ExecutionGraphInfo> executionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final List<JobStatus> jobStatuses =
                executionGraphInfos.stream()
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        final JobsOverview expectedJobsOverview = JobsOverview.create(jobStatuses);

        try (final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                createMemoryExecutionGraphInfoStore()) {
            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
            }

            assertThat(
                    executionGraphInfoStore.getStoredJobsOverview(),
                    Matchers.equalTo(expectedJobsOverview));
        }
    }

    /** Tests that we obtain the correct collection of available job details. */
    @Test
    public void testAvailableJobDetails() throws IOException {
        final int numberExecutionGraphs = 10;
        final Collection<ExecutionGraphInfo> executionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final Collection<JobDetails> jobDetails = generateJobDetails(executionGraphInfos);

        try (final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                createMemoryExecutionGraphInfoStore()) {
            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
            }

            assertThat(
                    executionGraphInfoStore.getAvailableJobDetails(),
                    Matchers.containsInAnyOrder(jobDetails.toArray()));
        }
    }

    /** Tests that an expired execution graph is removed from the execution graph store. */
    @Test
    public void testExecutionGraphExpiration() throws Exception {
        final Time expirationTime = Time.milliseconds(1L);

        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        final ManualTicker manualTicker = new ManualTicker();

        try (final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                new MemoryExecutionGraphInfoStore(
                        expirationTime, Integer.MAX_VALUE, scheduledExecutor, manualTicker)) {

            final ExecutionGraphInfo executionGraphInfo =
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setState(JobStatus.FINISHED)
                                    .build());

            executionGraphInfoStore.put(executionGraphInfo);

            // there should one execution graph
            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(1));

            manualTicker.advanceTime(expirationTime.toMilliseconds(), TimeUnit.MILLISECONDS);

            // this should trigger the cleanup after expiration
            scheduledExecutor.triggerScheduledTasks();

            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(0));

            assertThat(
                    executionGraphInfoStore.get(executionGraphInfo.getJobId()),
                    Matchers.nullValue());

            // check that the store is empty
            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(0));
        }
    }

    /** Tests that all job graphs are cleaned up after closing the store. */
    @Test
    public void testCloseCleansUp() throws IOException {
        try (final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                createMemoryExecutionGraphInfoStore()) {

            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(0));

            executionGraphInfoStore.put(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setState(JobStatus.FINISHED)
                                    .build()));

            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(1));

            executionGraphInfoStore.close();
            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(0));
        }
    }

    /**
     * Tests that the size of {@link MemoryExecutionGraphInfoStore} is no more than the configured
     * max capacity and the old execution graphs will be purged if the total added number exceeds
     * the max capacity.
     */
    @Test
    public void testMaximumCapacity() throws IOException {
        final int maxCapacity = 10;
        final int numberExecutionGraphs = 10;

        final Collection<ExecutionGraphInfo> oldExecutionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);
        final Collection<ExecutionGraphInfo> newExecutionGraphInfos =
                generateTerminalExecutionGraphInfos(numberExecutionGraphs);

        final Collection<JobDetails> jobDetails = generateJobDetails(newExecutionGraphInfos);

        try (final MemoryExecutionGraphInfoStore executionGraphInfoStore =
                createMemoryExecutionGraphInfoStore(Time.hours(1L), maxCapacity)) {

            for (ExecutionGraphInfo executionGraphInfo : oldExecutionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
                // no more than the configured maximum capacity
                assertTrue(executionGraphInfoStore.size() <= maxCapacity);
            }

            for (ExecutionGraphInfo executionGraphInfo : newExecutionGraphInfos) {
                executionGraphInfoStore.put(executionGraphInfo);
                // equals to the configured maximum capacity
                assertEquals(maxCapacity, executionGraphInfoStore.size());
            }

            // the older execution graphs are purged
            assertThat(
                    executionGraphInfoStore.getAvailableJobDetails(),
                    Matchers.containsInAnyOrder(jobDetails.toArray()));
        }
    }

    private void assertPutJobGraphWithStatus(JobStatus jobStatus) throws IOException {
        final ExecutionGraphInfo dummyExecutionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().setState(jobStatus).build());
        try (final MemoryExecutionGraphInfoStore executionGraphStore =
                createMemoryExecutionGraphInfoStore()) {

            // check that the graph store is empty
            assertThat(executionGraphStore.size(), Matchers.equalTo(0));

            executionGraphStore.put(dummyExecutionGraphInfo);

            // check that we have persisted the given execution graph
            assertThat(executionGraphStore.size(), Matchers.equalTo(1));

            assertThat(
                    executionGraphStore.get(dummyExecutionGraphInfo.getJobId()),
                    new ExecutionGraphInfoStoreTestUtils.PartialExecutionGraphInfoMatcher(
                            dummyExecutionGraphInfo));
        }
    }

    private MemoryExecutionGraphInfoStore createMemoryExecutionGraphInfoStore() {
        return new MemoryExecutionGraphInfoStore();
    }

    private MemoryExecutionGraphInfoStore createMemoryExecutionGraphInfoStore(
            Time expirationTime, int maximumCapacity) {
        return new MemoryExecutionGraphInfoStore(
                expirationTime,
                maximumCapacity,
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                Ticker.systemTicker());
    }
}
