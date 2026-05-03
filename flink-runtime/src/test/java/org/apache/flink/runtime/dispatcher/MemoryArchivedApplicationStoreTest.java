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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateApplicationDetails;
import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateJobDetails;
import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateTerminalArchivedApplications;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link MemoryArchivedApplicationStore}. */
class MemoryArchivedApplicationStoreTest extends TestLogger {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    /**
     * Tests that we can put {@link ArchivedApplication} into the {@link ArchivedApplicationStore}
     * and that the application is persisted.
     */
    @Test
    void testPut() throws IOException {
        final ArchivedApplication expectedArchivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();
        final ExecutionGraphInfo expectedExecutionGraphInfo =
                expectedArchivedApplication.getJobs().values().iterator().next();
        final ApplicationID applicationId = expectedArchivedApplication.getApplicationId();
        final JobID jobId = expectedExecutionGraphInfo.getJobId();

        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {

            // check that the store is empty
            assertEquals(0, archivedApplicationStore.size());

            archivedApplicationStore.put(expectedArchivedApplication);

            // check that we have persisted the given application
            assertEquals(1, archivedApplicationStore.size());

            assertThat(
                    archivedApplicationStore.get(applicationId).orElse(null),
                    new ArchivedApplicationStoreTestUtils.PartialArchivedApplicationMatcher(
                            expectedArchivedApplication));

            assertThat(
                    archivedApplicationStore.getExecutionGraphInfo(jobId).orElse(null),
                    new ArchivedApplicationStoreTestUtils.PartialExecutionGraphInfoMatcher(
                            expectedExecutionGraphInfo));
        }
    }

    /** Tests that empty is returned if we request an unknown ApplicationID. */
    @Test
    void testUnknownGet() throws IOException {
        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {
            assertNull(archivedApplicationStore.get(new ApplicationID()).orElse(null));
        }
    }

    /** Tests that we obtain the correct jobs overview. */
    @Test
    void testJobsOverview() throws IOException {
        final int numberArchivedApplications = 10;
        final Collection<ArchivedApplication> archivedApplications =
                generateTerminalArchivedApplications(numberArchivedApplications);

        final List<JobStatus> jobStatuses =
                archivedApplications.stream()
                        .flatMap(
                                archivedApplication ->
                                        archivedApplication.getJobs().values().stream())
                        .map(ExecutionGraphInfo::getArchivedExecutionGraph)
                        .map(ArchivedExecutionGraph::getState)
                        .collect(Collectors.toList());

        final JobsOverview expectedJobsOverview = JobsOverview.create(jobStatuses);

        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {
            for (ArchivedApplication executionGraphInfo : archivedApplications) {
                archivedApplicationStore.put(executionGraphInfo);
            }

            assertEquals(expectedJobsOverview, archivedApplicationStore.getJobsOverview());
        }
    }

    /** Tests that we obtain the correct collection of available jobs. */
    @Test
    void testGetJobDetails() throws IOException {
        final int numberArchivedApplications = 10;
        final Collection<ArchivedApplication> archivedApplications =
                generateTerminalArchivedApplications(numberArchivedApplications);

        final Collection<JobDetails> jobDetails = generateJobDetails(archivedApplications);

        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {
            for (ArchivedApplication archivedApplication : archivedApplications) {
                archivedApplicationStore.put(archivedApplication);
            }

            assertThat(
                    archivedApplicationStore.getJobDetails(),
                    Matchers.containsInAnyOrder(jobDetails.toArray()));
        }
    }

    /** Tests that we obtain the correct collection of available application details. */
    @Test
    void testGetApplicationDetails() throws IOException {
        final int numberArchivedApplications = 10;
        final Collection<ArchivedApplication> archivedApplications =
                generateTerminalArchivedApplications(numberArchivedApplications);

        final Collection<ApplicationDetails> applicationDetails =
                generateApplicationDetails(archivedApplications);

        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {
            for (ArchivedApplication archivedApplication : archivedApplications) {
                archivedApplicationStore.put(archivedApplication);
            }

            assertThat(
                    archivedApplicationStore.getApplicationDetails(),
                    Matchers.containsInAnyOrder(applicationDetails.toArray()));
        }
    }

    /** Tests that an expired application is removed from the store. */
    @Test
    void testArchivedApplicationExpiration() throws Exception {
        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();
        final ExecutionGraphInfo executionGraphInfo =
                archivedApplication.getJobs().values().iterator().next();
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        final JobID jobId = executionGraphInfo.getJobId();

        final Duration expirationTime = Duration.ofMillis(1L);

        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();

        final ManualTicker manualTicker = new ManualTicker();

        try (final MemoryArchivedApplicationStore archivedApplicationStore =
                new MemoryArchivedApplicationStore(
                        expirationTime, Integer.MAX_VALUE, scheduledExecutor, manualTicker)) {

            archivedApplicationStore.put(archivedApplication);

            // there should one application
            assertEquals(1, archivedApplicationStore.size());

            manualTicker.advanceTime(expirationTime.toMillis(), TimeUnit.MILLISECONDS);

            // this should trigger the cleanup after expiration
            scheduledExecutor.triggerScheduledTasks();

            // check that the store is empty
            assertEquals(0, archivedApplicationStore.size());

            assertNull(archivedApplicationStore.get(applicationId).orElse(null));

            assertNull(archivedApplicationStore.getExecutionGraphInfo(jobId).orElse(null));
        }
    }

    /** Tests that all applications are cleaned up after closing the store. */
    @Test
    void testCloseCleansUp() throws IOException {
        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();

        try (final ArchivedApplicationStore archivedApplicationStore =
                createDefaultArchivedApplicationStore()) {

            assertEquals(0, archivedApplicationStore.size());

            archivedApplicationStore.put(archivedApplication);

            assertEquals(1, archivedApplicationStore.size());

            archivedApplicationStore.close();
            assertEquals(0, archivedApplicationStore.size());
        }
    }

    /**
     * Tests that the size of {@link ArchivedApplicationStore} is no more than the configured max
     * capacity and the old applications will be purged if the total added number exceeds the max
     * capacity.
     */
    @Test
    void testMaximumCapacity() throws IOException {
        final int maxCapacity = 10;
        final int numberArchivedApplications = 10;

        final Collection<ArchivedApplication> oldArchivedApplications =
                generateTerminalArchivedApplications(numberArchivedApplications);
        final Collection<ArchivedApplication> newArchivedApplications =
                generateTerminalArchivedApplications(numberArchivedApplications);

        final Collection<ApplicationDetails> applicationDetails =
                generateApplicationDetails(newArchivedApplications);

        try (final ArchivedApplicationStore archivedApplicationStore =
                createArchivedApplicationStore(maxCapacity)) {

            for (ArchivedApplication archivedApplication : oldArchivedApplications) {
                archivedApplicationStore.put(archivedApplication);
                // no more than the configured maximum capacity
                assertTrue(archivedApplicationStore.size() <= maxCapacity);
            }

            for (ArchivedApplication archivedApplication : newArchivedApplications) {
                archivedApplicationStore.put(archivedApplication);
                // equals to the configured maximum capacity
                assertEquals(maxCapacity, archivedApplicationStore.size());
            }

            // the older applications are purged
            assertThat(
                    archivedApplicationStore.getApplicationDetails(),
                    Matchers.containsInAnyOrder(applicationDetails.toArray()));
        }
    }

    ArchivedApplicationStore createDefaultArchivedApplicationStore() throws IOException {
        return new MemoryArchivedApplicationStore();
    }

    ArchivedApplicationStore createArchivedApplicationStore(int maximumCapacity)
            throws IOException {
        return new MemoryArchivedApplicationStore(
                Duration.ofHours(1),
                maximumCapacity,
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                Ticker.systemTicker());
    }
}
