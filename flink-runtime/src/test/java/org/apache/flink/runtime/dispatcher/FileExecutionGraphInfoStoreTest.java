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
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link FileExecutionGraphInfoStore}. */
public class FileExecutionGraphInfoStoreTest extends TestLogger {

    private static final List<JobStatus> GLOBALLY_TERMINAL_JOB_STATUS =
            Arrays.stream(JobStatus.values())
                    .filter(JobStatus::isGloballyTerminalState)
                    .collect(Collectors.toList());

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * Tests that we can put {@link ExecutionGraphInfo} into the {@link FileExecutionGraphInfoStore}
     * and that the graph is persisted.
     */
    @Test
    public void testPut() throws IOException {
        final ExecutionGraphInfo dummyExecutionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).build());
        final File rootDir = temporaryFolder.newFolder();

        try (final FileExecutionGraphInfoStore executionGraphStore =
                createDefaultExecutionGraphInfoStore(rootDir)) {

            final File storageDirectory = executionGraphStore.getStorageDir();

            // check that the storage directory is empty
            assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));

            executionGraphStore.put(dummyExecutionGraphInfo);

            // check that we have persisted the given execution graph
            assertThat(storageDirectory.listFiles().length, Matchers.equalTo(1));

            assertThat(
                    executionGraphStore.get(dummyExecutionGraphInfo.getJobId()),
                    new PartialExecutionGraphInfoMatcher(dummyExecutionGraphInfo));
        }
    }

    /** Tests that null is returned if we request an unknown JobID. */
    @Test
    public void testUnknownGet() throws IOException {
        final File rootDir = temporaryFolder.newFolder();

        try (final FileExecutionGraphInfoStore executionGraphStore =
                createDefaultExecutionGraphInfoStore(rootDir)) {
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

        final File rootDir = temporaryFolder.newFolder();

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(rootDir)) {
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

        final File rootDir = temporaryFolder.newFolder();

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(rootDir)) {
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
        final File rootDir = temporaryFolder.newFolder();

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
            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(1));

            manualTicker.advanceTime(expirationTime.toMilliseconds(), TimeUnit.MILLISECONDS);

            // this should trigger the cleanup after expiration
            scheduledExecutor.triggerScheduledTasks();

            assertThat(executionGraphInfoStore.size(), Matchers.equalTo(0));

            assertThat(
                    executionGraphInfoStore.get(executionGraphInfo.getJobId()),
                    Matchers.nullValue());

            final File storageDirectory = executionGraphInfoStore.getStorageDir();

            // check that the persisted file has been deleted
            assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));
        }
    }

    /** Tests that all persisted files are cleaned up after closing the store. */
    @Test
    public void testCloseCleansUp() throws IOException {
        final File rootDir = temporaryFolder.newFolder();

        assertThat(rootDir.listFiles().length, Matchers.equalTo(0));

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                createDefaultExecutionGraphInfoStore(rootDir)) {

            assertThat(rootDir.listFiles().length, Matchers.equalTo(1));

            final File storageDirectory = executionGraphInfoStore.getStorageDir();

            assertThat(storageDirectory.listFiles().length, Matchers.equalTo(0));

            executionGraphInfoStore.put(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder()
                                    .setState(JobStatus.FINISHED)
                                    .build()));

            assertThat(storageDirectory.listFiles().length, Matchers.equalTo(1));
        }

        assertThat(rootDir.listFiles().length, Matchers.equalTo(0));
    }

    /** Tests that evicted {@link ExecutionGraphInfo} are loaded from disk again. */
    @Test
    public void testCacheLoading() throws IOException {
        final File rootDir = temporaryFolder.newFolder();

        try (final FileExecutionGraphInfoStore executionGraphInfoStore =
                new FileExecutionGraphInfoStore(
                        rootDir,
                        Time.hours(1L),
                        Integer.MAX_VALUE,
                        100L << 10,
                        TestingUtils.defaultScheduledExecutor(),
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

            assertThat(
                    storageDirectory.listFiles().length,
                    Matchers.equalTo(executionGraphInfos.size()));

            for (ExecutionGraphInfo executionGraphInfo : executionGraphInfos) {
                assertThat(
                        executionGraphInfoStore.get(executionGraphInfo.getJobId()),
                        matchesPartiallyWith(executionGraphInfo));
            }
        }
    }

    /**
     * Tests that the size of {@link FileExecutionGraphInfoStore} is no more than the configured max
     * capacity and the old execution graphs will be purged if the total added number exceeds the
     * max capacity.
     */
    @Test
    public void testMaximumCapacity() throws IOException {
        final File rootDir = temporaryFolder.newFolder();

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
                        TestingUtils.defaultScheduledExecutor(),
                        Ticker.systemTicker())) {

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

    private Collection<ExecutionGraphInfo> generateTerminalExecutionGraphInfos(int number) {
        final Collection<ExecutionGraphInfo> executionGraphInfos = new ArrayList<>(number);

        for (int i = 0; i < number; i++) {
            final JobStatus state =
                    GLOBALLY_TERMINAL_JOB_STATUS.get(
                            ThreadLocalRandom.current()
                                    .nextInt(GLOBALLY_TERMINAL_JOB_STATUS.size()));
            executionGraphInfos.add(
                    new ExecutionGraphInfo(
                            new ArchivedExecutionGraphBuilder().setState(state).build()));
        }

        return executionGraphInfos;
    }

    private FileExecutionGraphInfoStore createDefaultExecutionGraphInfoStore(File storageDirectory)
            throws IOException {
        return new FileExecutionGraphInfoStore(
                storageDirectory,
                Time.hours(1L),
                Integer.MAX_VALUE,
                10000L,
                TestingUtils.defaultScheduledExecutor(),
                Ticker.systemTicker());
    }

    private static final class PartialExecutionGraphInfoMatcher
            extends BaseMatcher<ExecutionGraphInfo> {

        private final ExecutionGraphInfo expectedExecutionGraphInfo;

        private PartialExecutionGraphInfoMatcher(ExecutionGraphInfo expectedExecutionGraphInfo) {
            this.expectedExecutionGraphInfo =
                    Preconditions.checkNotNull(expectedExecutionGraphInfo);
        }

        @Override
        public boolean matches(Object o) {
            if (expectedExecutionGraphInfo == o) {
                return true;
            }
            if (o == null || expectedExecutionGraphInfo.getClass() != o.getClass()) {
                return false;
            }
            ExecutionGraphInfo that = (ExecutionGraphInfo) o;

            ArchivedExecutionGraph thisExecutionGraph =
                    expectedExecutionGraphInfo.getArchivedExecutionGraph();
            ArchivedExecutionGraph thatExecutionGraph = that.getArchivedExecutionGraph();
            return thisExecutionGraph.isStoppable() == thatExecutionGraph.isStoppable()
                    && Objects.equals(thisExecutionGraph.getJobID(), thatExecutionGraph.getJobID())
                    && Objects.equals(
                            thisExecutionGraph.getJobName(), thatExecutionGraph.getJobName())
                    && thisExecutionGraph.getState() == thatExecutionGraph.getState()
                    && Objects.equals(
                            thisExecutionGraph.getJsonPlan(), thatExecutionGraph.getJsonPlan())
                    && Objects.equals(
                            thisExecutionGraph.getAccumulatorsSerialized(),
                            thatExecutionGraph.getAccumulatorsSerialized())
                    && Objects.equals(
                            thisExecutionGraph.getCheckpointCoordinatorConfiguration(),
                            thatExecutionGraph.getCheckpointCoordinatorConfiguration())
                    && thisExecutionGraph.getAllVertices().size()
                            == thatExecutionGraph.getAllVertices().size()
                    && Objects.equals(
                            expectedExecutionGraphInfo.getExceptionHistory(),
                            that.getExceptionHistory());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(
                    "Matches against " + ExecutionGraphInfo.class.getSimpleName() + '.');
        }
    }

    private static Matcher<ExecutionGraphInfo> matchesPartiallyWith(
            ExecutionGraphInfo executionGraphInfo) {
        return new PartialExecutionGraphInfoMatcher(executionGraphInfo);
    }

    private static Collection<JobDetails> generateJobDetails(
            Collection<ExecutionGraphInfo> executionGraphInfos) {
        return executionGraphInfos.stream()
                .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                .map(JobDetails::createDetailsForJob)
                .collect(Collectors.toList());
    }
}
