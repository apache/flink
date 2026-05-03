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
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.util.ManualTicker;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava33.com.google.common.cache.LoadingCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.createFileArchivedApplicationStore;
import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateTerminalArchivedApplications;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for the {@link FileArchivedApplicationStore}. */
class FileArchivedApplicationStoreTest extends MemoryArchivedApplicationStoreTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @TempDir File temporaryFolder;

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

        try (final FileArchivedApplicationStore archivedApplicationStore =
                new FileArchivedApplicationStore(
                        temporaryFolder,
                        expirationTime,
                        Integer.MAX_VALUE,
                        10000L,
                        scheduledExecutor,
                        manualTicker)) {

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

            final File storageDirectory = archivedApplicationStore.getStorageDir();

            // check that the persisted file has been deleted
            assertEquals(0, storageDirectory.listFiles().length);
        }
    }

    /** Tests that all applications are cleaned up after closing the store. */
    @Test
    void testCloseCleansUp() throws IOException {
        assertEquals(0, temporaryFolder.listFiles().length);

        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();

        try (final FileArchivedApplicationStore archivedApplicationStore =
                createFileArchivedApplicationStore(
                        temporaryFolder,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()))) {

            assertEquals(1, temporaryFolder.listFiles().length);

            final File storageDirectory = archivedApplicationStore.getStorageDir();

            assertEquals(0, storageDirectory.listFiles().length);

            archivedApplicationStore.put(archivedApplication);

            assertEquals(1, storageDirectory.listFiles().length);

            archivedApplicationStore.close();

            assertEquals(0, temporaryFolder.listFiles().length);
        }
    }

    /** Tests that evicted {@link ArchivedApplication} are loaded from disk again. */
    @Test
    void testCacheLoading() throws IOException {
        try (final FileArchivedApplicationStore archivedApplicationStore =
                new FileArchivedApplicationStore(
                        temporaryFolder,
                        Duration.ofHours(1L),
                        Integer.MAX_VALUE,
                        100L << 10,
                        new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                        Ticker.systemTicker())) {

            final LoadingCache<ApplicationID, ArchivedApplication> archivedApplicationCache =
                    archivedApplicationStore.getArchivedApplicationCache();

            Collection<ArchivedApplication> archivedApplications = new ArrayList<>(64);

            boolean continueInserting = true;

            // insert applications until the first one got evicted
            while (continueInserting) {
                // has roughly a size of 3.1 KB
                final ArchivedApplication archivedApplication =
                        generateTerminalArchivedApplications(1).iterator().next();

                archivedApplicationStore.put(archivedApplication);

                archivedApplications.add(archivedApplication);

                continueInserting = archivedApplicationCache.size() == archivedApplications.size();
            }

            final File storageDirectory = archivedApplicationStore.getStorageDir();

            assertEquals(archivedApplications.size(), storageDirectory.listFiles().length);

            for (ArchivedApplication archivedApplication : archivedApplications) {
                final ExecutionGraphInfo executionGraphInfo =
                        archivedApplication.getJobs().values().iterator().next();
                final ApplicationID applicationId = archivedApplication.getApplicationId();
                final JobID jobId = executionGraphInfo.getJobId();

                assertThat(
                        archivedApplicationStore.get(applicationId).orElse(null),
                        new ArchivedApplicationStoreTestUtils.PartialArchivedApplicationMatcher(
                                archivedApplication));

                assertThat(
                        archivedApplicationStore.getExecutionGraphInfo(jobId).orElse(null),
                        new ArchivedApplicationStoreTestUtils.PartialExecutionGraphInfoMatcher(
                                executionGraphInfo));
            }
        }
    }

    @Override
    ArchivedApplicationStore createDefaultArchivedApplicationStore() throws IOException {
        return createFileArchivedApplicationStore(
                temporaryFolder,
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));
    }

    @Override
    ArchivedApplicationStore createArchivedApplicationStore(int maximumCapacity)
            throws IOException {
        return new FileArchivedApplicationStore(
                temporaryFolder,
                Duration.ofHours(1L),
                maximumCapacity,
                10000L,
                new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()),
                Ticker.systemTicker());
    }
}
