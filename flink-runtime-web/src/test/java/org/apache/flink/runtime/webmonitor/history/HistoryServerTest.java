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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.history.FsJobArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveProcessor.ProcessEventType;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.HistoryServerOptions.HISTORY_SERVER_UNZIPPED_JOBS_MAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the HistoryServer. */
class HistoryServerTest {

    private static final JsonFactory JACKSON_FACTORY =
            new JsonFactory()
                    .enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
                    .disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);
    private static final ObjectMapper OBJECT_MAPPER =
            JacksonMapperFactory.createObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES);

    private MiniClusterWithClientResource cluster;
    private File jmDirectory;
    private File hsDirectory;
    private File archivedJobsDirectory;

    @BeforeEach
    void setUp(@TempDir File jmDirectory, @TempDir File hsDirectory) throws Exception {
        this.jmDirectory = jmDirectory;
        this.hsDirectory = hsDirectory;
        this.archivedJobsDirectory = new File(this.hsDirectory, "archivedJobs");

        Configuration clusterConfig = new Configuration();
        clusterConfig.setString(JobManagerOptions.ARCHIVE_DIR, jmDirectory.toURI().toString());

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfig)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(2)
                                .build());
        cluster.before();
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.after();
        }
    }

    @ParameterizedTest(name = "Flink version less than 1.4: {0}")
    @ValueSource(booleans = {true, false})
    void testHistoryServerIntegration(final boolean versionLessThan14) throws Exception {
        final int numJobs = 2;
        final int numLegacyJobs = 1;

        CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs + numLegacyJobs);

        Configuration historyServerConfig = createTestConfiguration(false);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            if (event.getType() == ProcessEventType.DOWNLOADED) {
                                numExpectedArchivedJobs.countDown();
                            }
                        });

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();

            assertThat(getJobsOverview(baseUrl).getJobs()).isEmpty();

            for (int x = 0; x < numJobs; x++) {
                runJob();
            }
            createLegacyArchive(jmDirectory.toPath(), versionLessThan14);
            waitForArchivesCreation(numJobs + numLegacyJobs);

            assertThat(numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir()).hasSize(numJobs + numLegacyJobs);

            // checks whether the dashboard configuration contains all expected fields
            getDashboardConfiguration(baseUrl);
        } finally {
            hs.stop();
        }
    }

    @ParameterizedTest(name = "Flink version less than 1.4: {0}")
    @ValueSource(booleans = {true, false})
    void testRemoveOldestModifiedArchivesBeyondHistorySizeLimit(final boolean versionLessThan14)
            throws Exception {
        final int numArchivesToKeepInHistory = 2;
        final int numArchivesBeforeHsStarted = 4;
        final int numArchivesAfterHsStarted = 2;
        final int numArchivesToRemoveUponHsStart =
                numArchivesBeforeHsStarted - numArchivesToKeepInHistory;
        final long oneMinuteSinceEpoch = 1000L * 60L;
        List<String> expectedJobIdsToKeep = new LinkedList<>();

        for (int j = 0; j < numArchivesBeforeHsStarted; j++) {
            String jobId =
                    createLegacyArchive(
                            jmDirectory.toPath(), j * oneMinuteSinceEpoch, versionLessThan14);
            if (j >= numArchivesToRemoveUponHsStart) {
                expectedJobIdsToKeep.add(jobId);
            }
        }

        CountDownLatch numArchivesJobsInitially = new CountDownLatch(numArchivesToKeepInHistory);
        CountDownLatch numArchivesDeletedInitially =
                new CountDownLatch(numArchivesToRemoveUponHsStart);
        CountDownLatch numArchivesCreatedTotal =
                new CountDownLatch(
                        numArchivesBeforeHsStarted
                                - numArchivesToRemoveUponHsStart
                                + numArchivesAfterHsStarted);
        CountDownLatch numArchivesDeletedTotal =
                new CountDownLatch(numArchivesToRemoveUponHsStart + numArchivesAfterHsStarted);

        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS.defaultValue());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS, numArchivesToKeepInHistory);
        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsInitially.countDown();
                                    numArchivesCreatedTotal.countDown();
                                    break;
                                case DELETED:
                                    numArchivesDeletedInitially.countDown();
                                    numArchivesDeletedTotal.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            assertThat(numArchivesJobsInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesDeletedInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir()).isEqualTo(new HashSet<>(expectedJobIdsToKeep));

            for (int j = numArchivesBeforeHsStarted;
                    j < numArchivesBeforeHsStarted + numArchivesAfterHsStarted;
                    j++) {
                expectedJobIdsToKeep.remove(0);
                expectedJobIdsToKeep.add(
                        createLegacyArchive(
                                jmDirectory.toPath(), j * oneMinuteSinceEpoch, versionLessThan14));
            }
            assertThat(numArchivesCreatedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesDeletedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir()).isEqualTo(new HashSet<>(expectedJobIdsToKeep));
        } finally {
            hs.stop();
        }
    }

    @ParameterizedTest(name = "Flink version less than 1.4: {0}")
    @ValueSource(booleans = {true, false})
    void testTriggerUnzipArchivedFile(final boolean versionLessThan14) throws Exception {
        final int numArchivesBeforeHsStarted = 0;
        final int numArchivesAfterHsStarted = 2;
        final int numTriggerUnzipJobNum = 1;
        final int numArchivesToRemove = 1;
        final long oneMinuteSinceEpoch = 1000L * 60L;

        for (int j = 0; j < numArchivesBeforeHsStarted; j++) {
            createLegacyArchive(jmDirectory.toPath(), j * oneMinuteSinceEpoch, versionLessThan14);
        }

        CountDownLatch numArchivesJobsInitially = new CountDownLatch(numArchivesBeforeHsStarted);
        CountDownLatch numArchivesJobsTotal =
                new CountDownLatch(numArchivesBeforeHsStarted + numArchivesAfterHsStarted);
        CountDownLatch numUnzippedJobsTotal = new CountDownLatch(numTriggerUnzipJobNum);
        CountDownLatch numArchivesCleanedTotal = new CountDownLatch(numArchivesToRemove);
        CountDownLatch numArchivesDeletedTotal = new CountDownLatch(numArchivesToRemove);

        Configuration historyServerConfig = createTestConfiguration(true);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsInitially.countDown();
                                    numArchivesJobsTotal.countDown();
                                    break;
                                case UNZIPPED:
                                    numUnzippedJobsTotal.countDown();
                                    break;
                                case CLEANED:
                                    numArchivesCleanedTotal.countDown();
                                    break;
                                case DELETED:
                                    numArchivesDeletedTotal.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            assertThat(numArchivesJobsInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numUnzippedJobsTotal.getCount()).isEqualTo(numTriggerUnzipJobNum);
            assertThat(getIdsFromArchivedDir()).hasSize(numArchivesBeforeHsStarted);

            for (int x = 0; x < numArchivesAfterHsStarted; x++) {
                runJob();
            }
            assertThat(numArchivesJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numUnzippedJobsTotal.getCount()).isEqualTo(numTriggerUnzipJobNum);
            assertThat(getIdsFromArchivedDir())
                    .hasSize(numArchivesBeforeHsStarted + numArchivesAfterHsStarted);

            String jobIdToView = getIdsFromArchivedDir().iterator().next();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            JobDetailsInfo jobDetailsViewed = getJobOverview(baseUrl, jobIdToView);
            assertThat(numUnzippedJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(jobDetailsViewed.getJobId().toString()).isEqualTo(jobIdToView);

            Files.deleteIfExists(jmDirectory.toPath().resolve(jobIdToView));
            assertThat(numArchivesDeletedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesCleanedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir())
                    .hasSize(
                            numArchivesBeforeHsStarted
                                    + numArchivesAfterHsStarted
                                    - numArchivesToRemove);
        } finally {
            hs.stop();
        }
    }

    @ParameterizedTest(name = "Flink version less than 1.4: {0}")
    @ValueSource(booleans = {true, false})
    void testCleanUnzipArchivedFile(final boolean versionLessThan14) throws Exception {
        final int numArchivesBeforeHsStarted = 0;
        final int numArchivesAfterHsStarted = 6;
        final int numTriggerUnzipJobNum = 5;
        final int numCached = 3;
        final int numArchivesToClean = numTriggerUnzipJobNum - numCached;
        final int numArchivesToRemove = 1;
        final long oneMinuteSinceEpoch = 1000L * 60L;

        for (int j = 0; j < numArchivesBeforeHsStarted; j++) {
            createLegacyArchive(jmDirectory.toPath(), j * oneMinuteSinceEpoch, versionLessThan14);
        }

        CountDownLatch numArchivesJobsInitially = new CountDownLatch(numArchivesBeforeHsStarted);
        CountDownLatch numArchivesJobsTotal =
                new CountDownLatch(numArchivesBeforeHsStarted + numArchivesAfterHsStarted);
        CountDownLatch numUnzippedJobsTotal = new CountDownLatch(numTriggerUnzipJobNum);
        CountDownLatch numArchivesCleanedTotal = new CountDownLatch(numArchivesToClean);
        CountDownLatch numArchivesDeletedTotal = new CountDownLatch(numArchivesToRemove);

        Configuration historyServerConfig = createTestConfiguration(true);
        historyServerConfig.set(HISTORY_SERVER_UNZIPPED_JOBS_MAX, numCached);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsInitially.countDown();
                                    numArchivesJobsTotal.countDown();
                                    break;
                                case UNZIPPED:
                                    numUnzippedJobsTotal.countDown();
                                    break;
                                case CLEANED:
                                    numArchivesCleanedTotal.countDown();
                                    break;
                                case DELETED:
                                    numArchivesDeletedTotal.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            assertThat(numArchivesJobsInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numUnzippedJobsTotal.getCount()).isEqualTo(numTriggerUnzipJobNum);
            assertThat(getIdsFromArchivedDir()).hasSize(numArchivesBeforeHsStarted);

            for (int x = 0; x < numArchivesAfterHsStarted; x++) {
                runJob();
            }
            assertThat(numArchivesJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numUnzippedJobsTotal.getCount()).isEqualTo(numTriggerUnzipJobNum);
            assertThat(getIdsFromArchivedDir())
                    .hasSize(numArchivesBeforeHsStarted + numArchivesAfterHsStarted);

            String jobIdToRemove = null;
            Set<String> jobSet = getIdsFromArchivedDir();
            int triggerNum = numTriggerUnzipJobNum;
            Iterator<String> jobIterator = jobSet.iterator();
            while (jobIterator.hasNext() && triggerNum > 0) {
                String jobIdToView = jobIterator.next();
                String baseUrl = "http://localhost:" + hs.getWebPort();
                getJobOverview(baseUrl, jobIdToView);
                triggerNum--;
                if (triggerNum == 0) {
                    jobIdToRemove = jobIdToView;
                }
            }
            assertThat(numUnzippedJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesCleanedTotal.await(10L, TimeUnit.SECONDS)).isTrue();

            if (jobIdToRemove != null) {
                Files.deleteIfExists(jmDirectory.toPath().resolve(jobIdToRemove));
            }
            assertThat(numArchivesDeletedTotal.await(10L, TimeUnit.SECONDS)).isTrue();

            assertThat(getIdsFromArchivedDir())
                    .hasSize(numArchivesAfterHsStarted - numArchivesToRemove);
        } finally {
            hs.stop();
        }
    }

    @Test
    void testCleanWhenHistoryServer() throws Exception {
        final int numArchivesBeforeHsStarted = 4;
        final int numTriggerUnzipJobNum = 2;
        final int numArchivesToRemove = 1;

        CountDownLatch numArchivesJobsReloaded = new CountDownLatch(numTriggerUnzipJobNum);
        CountDownLatch numArchivesJobsTotal = new CountDownLatch(numArchivesBeforeHsStarted);
        CountDownLatch numUnzippedJobsTotal = new CountDownLatch(numTriggerUnzipJobNum);

        Configuration historyServerConfig = createTestConfiguration(true);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsTotal.countDown();
                                    break;
                                case UNZIPPED:
                                    numUnzippedJobsTotal.countDown();
                                    break;
                                case RELOADED:
                                    numArchivesJobsReloaded.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            for (int x = 0; x < numArchivesBeforeHsStarted; x++) {
                runJob();
            }
            assertThat(numArchivesJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();

            Set<String> jobSet = getIdsFromArchivedDir();
            int triggerNum = numTriggerUnzipJobNum;
            Iterator<String> jobIterator = jobSet.iterator();
            while (jobIterator.hasNext() && triggerNum > 0) {
                String jobIdToView = jobIterator.next();
                String baseUrl = "http://localhost:" + hs.getWebPort();
                getJobOverview(baseUrl, jobIdToView);
                triggerNum--;
            }
            assertThat(numUnzippedJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir()).hasSize(numArchivesBeforeHsStarted);
        } finally {
            hs.stop();
        }

        assertThat(getFileNameInHSDir()).hasSize(3);
        assertThat(getFileNameInHSDir()).contains("overviews");
        assertThat(getFileNameInHSDir()).contains("jobs");
        assertThat(getFileNameInHSDir()).contains("archivedJobs");
    }

    @Test
    void testReloadUnzipArchivedFile() throws Exception {
        final int numArchivesBeforeHsStarted = 4;
        final int numTriggerUnzipJobNum = 2;
        final int numArchivesToRemove = 1;

        CountDownLatch numArchivesJobsReloaded = new CountDownLatch(numTriggerUnzipJobNum);
        CountDownLatch numArchivesJobsTotal = new CountDownLatch(numArchivesBeforeHsStarted);
        CountDownLatch numUnzippedJobsTotal = new CountDownLatch(numTriggerUnzipJobNum);

        Configuration historyServerConfig = createTestConfiguration(true);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsTotal.countDown();
                                    break;
                                case UNZIPPED:
                                    numUnzippedJobsTotal.countDown();
                                    break;
                                case RELOADED:
                                    numArchivesJobsReloaded.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            for (int x = 0; x < numArchivesBeforeHsStarted; x++) {
                runJob();
            }
            assertThat(numArchivesJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();

            Set<String> jobSet = getIdsFromArchivedDir();
            int triggerNum = numTriggerUnzipJobNum;
            Iterator<String> jobIterator = jobSet.iterator();
            while (jobIterator.hasNext() && triggerNum > 0) {
                String jobIdToView = jobIterator.next();
                String baseUrl = "http://localhost:" + hs.getWebPort();
                getJobOverview(baseUrl, jobIdToView);
                triggerNum--;
            }
            assertThat(numUnzippedJobsTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir()).hasSize(numArchivesBeforeHsStarted);
        } finally {
            hs.stop();
        }

        String jobToRemove = getIdsFromArchivedDir().iterator().next();
        Files.deleteIfExists(jmDirectory.toPath().resolve(jobToRemove));

        hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numArchivesJobsTotal.countDown();
                                    break;
                                case UNZIPPED:
                                    numUnzippedJobsTotal.countDown();
                                    break;
                                case RELOADED:
                                    numArchivesJobsReloaded.countDown();
                                    break;
                            }
                        });
        try {
            hs.start();
            assertThat(numArchivesJobsReloaded.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromArchivedDir())
                    .hasSize(numArchivesBeforeHsStarted - numArchivesToRemove);
        } finally {
            hs.stop();
        }
    }

    @Deprecated
    private Set<String> getIdsFromJobOverview(String baseUrl) throws Exception {
        return getJobsOverview(baseUrl).getJobs().stream()
                .map(JobDetails::getJobId)
                .map(JobID::toString)
                .collect(Collectors.toSet());
    }

    private Set<String> getIdsFromArchivedDir() {
        return Arrays.stream(this.archivedJobsDirectory.list()).collect(Collectors.toSet());
    }

    private Set<String> getFileNameInHSDir() {
        return Arrays.stream(this.hsDirectory.list()).collect(Collectors.toSet());
    }

    @Test
    void testFailIfHistorySizeLimitIsZero() throws Exception {
        assertThatThrownBy(() -> startHistoryServerWithSizeLimit(0))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testFailIfHistorySizeLimitIsLessThanMinusOne() throws Exception {
        assertThatThrownBy(() -> startHistoryServerWithSizeLimit(-2))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    private void startHistoryServerWithSizeLimit(int maxHistorySize)
            throws IOException, FlinkException, InterruptedException {
        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS.defaultValue());
        historyServerConfig.setInteger(
                HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS, maxHistorySize);
        new HistoryServer(historyServerConfig).start();
    }

    @Test
    void testCleanExpiredJob() throws Exception {
        runArchiveExpirationTest(true);
    }

    @Test
    void testRemainExpiredJob() throws Exception {
        runArchiveExpirationTest(false);
    }

    private void runArchiveExpirationTest(boolean cleanupExpiredJobs) throws Exception {
        int numExpiredJobs = cleanupExpiredJobs ? 1 : 0;
        int numJobs = 3;
        for (int x = 0; x < numJobs; x++) {
            runJob();
        }
        waitForArchivesCreation(numJobs);

        CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs);
        CountDownLatch numExpectedUnzipJobs = new CountDownLatch(numJobs);
        CountDownLatch firstArchiveExpiredLatch = new CountDownLatch(numExpiredJobs);
        CountDownLatch allArchivesExpiredLatch =
                new CountDownLatch(cleanupExpiredJobs ? numJobs : 0);
        CountDownLatch firstCleanLatch = new CountDownLatch(numExpiredJobs);
        CountDownLatch allCleanLatch = new CountDownLatch(cleanupExpiredJobs ? numJobs : 0);

        Configuration historyServerConfig = createTestConfiguration(cleanupExpiredJobs);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case DOWNLOADED:
                                    numExpectedArchivedJobs.countDown();
                                    break;
                                case UNZIPPED:
                                    numExpectedUnzipJobs.countDown();
                                    break;
                                case CLEANED:
                                    firstCleanLatch.countDown();
                                    allCleanLatch.countDown();
                                    break;
                                case DELETED:
                                    firstArchiveExpiredLatch.countDown();
                                    allArchivesExpiredLatch.countDown();
                                    break;
                            }
                        });

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS)).isTrue();

            Set<String> jobs = getIdsFromArchivedDir();
            assertThat(jobs).hasSize(numJobs);

            // trigger another fetch and delete one archive from jm
            // we fetch again to probabilistically cause a concurrent deletion
            hs.fetchArchives();
            assertThat(numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numExpectedUnzipJobs.getCount()).isEqualTo(numJobs);

            String jobIdToDelete = getIdsFromArchivedDir().iterator().next();
            Files.deleteIfExists(jmDirectory.toPath().resolve(jobIdToDelete));
            assertThat(firstArchiveExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(firstCleanLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numExpectedUnzipJobs.getCount()).isEqualTo(numJobs);

            jobs = getIdsFromArchivedDir();
            assertThat(jobs).hasSize(numJobs - numExpiredJobs);
            // delete remaining archives from jm and ensure files are cleaned up
            for (String remainingJobId : jobs) {
                Files.deleteIfExists(jmDirectory.toPath().resolve(remainingJobId));
            }
            assertThat(allArchivesExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(allCleanLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numExpectedUnzipJobs.getCount()).isEqualTo(numJobs);

            assertJobFilesCleanedUp(cleanupExpiredJobs);
        } finally {
            hs.stop();
        }
    }

    private void assertJobFilesCleanedUp(boolean jobFilesShouldBeDeleted) throws IOException {
        try (Stream<Path> paths = Files.walk(hsDirectory.toPath())) {
            final List<Path> jobFiles =
                    paths.filter(path -> !path.equals(hsDirectory.toPath()))
                            .map(path -> hsDirectory.toPath().relativize(path))
                            .filter(path -> !path.equals(Paths.get("config.json")))
                            .filter(path -> !path.equals(Paths.get("archivedJobs")))
                            .filter(path -> !path.equals(Paths.get("jobs")))
                            .filter(path -> !path.equals(Paths.get("jobs", "overview.json")))
                            .filter(path -> !path.equals(Paths.get("overviews")))
                            .collect(Collectors.toList());

            if (jobFilesShouldBeDeleted) {
                assertThat(jobFiles).isEmpty();
            } else {
                assertThat(jobFiles).isNotEmpty();
            }
        }
    }

    private void waitForArchivesCreation(int numJobs) throws InterruptedException {
        // the job is archived asynchronously after env.execute() returns
        File[] archives = jmDirectory.listFiles();
        while (archives == null || archives.length != numJobs) {
            Thread.sleep(50);
            archives = jmDirectory.listFiles();
        }
    }

    private Configuration createTestConfiguration(boolean cleanupExpiredJobs) {
        Configuration historyServerConfig = new Configuration();
        historyServerConfig.setString(
                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
        historyServerConfig.setString(
                HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());
        historyServerConfig.setLong(
                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL, 100L);

        historyServerConfig.setBoolean(
                HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS, cleanupExpiredJobs);

        historyServerConfig.setInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);
        return historyServerConfig;
    }

    private static DashboardConfiguration getDashboardConfiguration(String baseUrl)
            throws Exception {
        Tuple2<Integer, String> response =
                HttpUtils.getFromHTTP(
                        baseUrl
                                + DashboardConfigurationHeaders.INSTANCE
                                        .getTargetRestEndpointURL());
        return OBJECT_MAPPER.readValue(response.f1, DashboardConfiguration.class);
    }

    private static MultipleJobsDetails getJobsOverview(String baseUrl) throws Exception {
        Tuple2<Integer, String> response = HttpUtils.getFromHTTP(baseUrl + JobsOverviewHeaders.URL);
        return OBJECT_MAPPER.readValue(response.f1, MultipleJobsDetails.class);
    }

    private static JobDetailsInfo getJobOverview(String baseUrl, String jobID) throws Exception {
        String jobUrl = baseUrl + "/jobs/" + jobID;
        Tuple2<Integer, String> response = HttpUtils.getFromHTTP(jobUrl);
        return OBJECT_MAPPER.readValue(response.f1, JobDetailsInfo.class);
    }

    private static void runJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3).addSink(new DiscardingSink<>());

        env.execute();
    }

    private static String createLegacyArchive(
            Path directory, long fileModifiedDate, boolean versionLessThan14) throws IOException {
        String jobId = createLegacyArchive(directory, versionLessThan14);
        File jobArchive = directory.resolve(jobId).toFile();
        jobArchive.setLastModified(fileModifiedDate);
        return jobId;
    }

    private static String createLegacyArchive(Path directory, boolean versionLessThan14)
            throws IOException {
        JobID jobId = JobID.generate();

        StringWriter sw = new StringWriter();
        try (JsonGenerator gen = JACKSON_FACTORY.createGenerator(sw)) {
            try (JsonObject root = new JsonObject(gen)) {
                try (JsonArray finished = new JsonArray(gen, "finished")) {
                    try (JsonObject job = new JsonObject(gen)) {
                        gen.writeStringField("jid", jobId.toString());
                        gen.writeStringField("name", "testjob");
                        gen.writeStringField("state", JobStatus.FINISHED.name());

                        gen.writeNumberField("start-time", 0L);
                        gen.writeNumberField("end-time", 1L);
                        gen.writeNumberField("duration", 1L);
                        gen.writeNumberField("last-modification", 1L);

                        try (JsonObject tasks = new JsonObject(gen, "tasks")) {
                            gen.writeNumberField("total", 0);

                            if (versionLessThan14) {
                                gen.writeNumberField("pending", 0);
                            } else {
                                gen.writeNumberField("created", 0);
                                gen.writeNumberField("deploying", 0);
                                gen.writeNumberField("scheduled", 0);
                            }
                            gen.writeNumberField("running", 0);
                            gen.writeNumberField("finished", 0);
                            gen.writeNumberField("canceling", 0);
                            gen.writeNumberField("canceled", 0);
                            gen.writeNumberField("failed", 0);
                        }
                    }
                }
            }
        }
        String json = sw.toString();
        ArchivedJson archivedJson = new ArchivedJson("/joboverview", json);
        FsJobArchivist.archiveJob(
                new org.apache.flink.core.fs.Path(directory.toUri()),
                jobId,
                Collections.singleton(archivedJson));

        return jobId.toString();
    }

    private static final class JsonObject implements AutoCloseable {

        private final JsonGenerator gen;

        JsonObject(JsonGenerator gen) throws IOException {
            this.gen = gen;
            gen.writeStartObject();
        }

        private JsonObject(JsonGenerator gen, String name) throws IOException {
            this.gen = gen;
            gen.writeObjectFieldStart(name);
        }

        @Override
        public void close() throws IOException {
            gen.writeEndObject();
        }
    }

    private static final class JsonArray implements AutoCloseable {

        private final JsonGenerator gen;

        JsonArray(JsonGenerator gen, String name) throws IOException {
            this.gen = gen;
            gen.writeArrayFieldStart(name);
        }

        @Override
        public void close() throws IOException {
            gen.writeEndArray();
        }
    }
}
