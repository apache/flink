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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.history.ArchivePathUtils;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetailsInfo;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleApplicationsDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.ApplicationIDPathParameter;
import org.apache.flink.runtime.rest.messages.ApplicationsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.DashboardConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.rest.messages.application.ApplicationDetailsHeaders;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
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

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private Configuration clusterConfig;

    @BeforeEach
    void setUp(@TempDir File jmDirectory, @TempDir File hsDirectory) throws Exception {
        this.jmDirectory = jmDirectory;
        this.hsDirectory = hsDirectory;

        clusterConfig = new Configuration();
        clusterConfig.set(JobManagerOptions.ARCHIVE_DIR, jmDirectory.toURI().toString());

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
                            if (event.getType()
                                    == HistoryServerArchiveFetcher.ArchiveEventType.CREATED) {
                                numExpectedArchivedJobs.countDown();
                            }
                        },
                        (event) -> {
                            throw new RuntimeException("Should not call");
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
            assertThat(getJobsOverview(baseUrl).getJobs()).hasSize(numJobs + numLegacyJobs);

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
        List<JobID> expectedJobIdsToKeep = new LinkedList<>();

        for (int j = 0; j < numArchivesBeforeHsStarted; j++) {
            JobID jobId =
                    createLegacyArchive(
                            jmDirectory.toPath(), j * oneMinuteSinceEpoch, versionLessThan14);
            if (j >= numArchivesToRemoveUponHsStart) {
                expectedJobIdsToKeep.add(jobId);
            }
        }

        CountDownLatch numArchivesCreatedInitially = new CountDownLatch(numArchivesToKeepInHistory);
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
                                case CREATED:
                                    numArchivesCreatedInitially.countDown();
                                    numArchivesCreatedTotal.countDown();
                                    break;
                                case DELETED:
                                    numArchivesDeletedInitially.countDown();
                                    numArchivesDeletedTotal.countDown();
                                    break;
                            }
                        },
                        (event) -> {
                            throw new RuntimeException("Should not call");
                        });

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numArchivesCreatedInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesDeletedInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getIdsFromJobOverview(baseUrl))
                    .isEqualTo(new HashSet<>(expectedJobIdsToKeep));

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
            assertThat(getIdsFromJobOverview(baseUrl))
                    .isEqualTo(new HashSet<>(expectedJobIdsToKeep));
        } finally {
            hs.stop();
        }
    }

    private Set<JobID> getIdsFromJobOverview(String baseUrl) throws Exception {
        return getJobsOverview(baseUrl).getJobs().stream()
                .map(JobDetails::getJobId)
                .collect(Collectors.toSet());
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
        historyServerConfig.set(HistoryServerOptions.HISTORY_SERVER_RETAINED_JOBS, maxHistorySize);
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

    @Test
    void testClearWebDir() throws Exception {
        // Test the path configured by 'historyserver.web.tmpdir' is clean.
        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS.defaultValue());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.toURI().toString());
        HistoryServer hs = new HistoryServer(historyServerConfig);
        assertInitializedHistoryServerWebDir(hs.getWebDir());

        // Test the path configured by 'historyserver.web.tmpdir' is dirty.
        new File(hsDirectory.toURI() + "/dirtyEmptySubDir").mkdir();
        new File(hsDirectory.toURI() + "/dirtyEmptySubFile.json").createNewFile();
        new File(hsDirectory.toURI() + "/overviews/dirtyEmptySubDir").mkdir();
        new File(hsDirectory.toURI() + "/overviews/dirtyEmptySubFile.json").createNewFile();
        new File(hsDirectory.toURI() + "/jobs/dirtyEmptySubDir").mkdir();
        new File(hsDirectory.toURI() + "/jobs/dirtyEmptySubFile.json").createNewFile();
        new File(hsDirectory.toURI() + "/application-overviews/dirtyEmptySubDir").mkdir();
        new File(hsDirectory.toURI() + "/application-overviews/dirtyEmptySubFile.json")
                .createNewFile();
        new File(hsDirectory.toURI() + "/applications/dirtyEmptySubDir").mkdir();
        new File(hsDirectory.toURI() + "/applications/dirtyEmptySubFile.json").createNewFile();
        hs = new HistoryServer(historyServerConfig);
        assertInitializedHistoryServerWebDir(hs.getWebDir());
    }

    private void assertInitializedHistoryServerWebDir(File historyWebDir) {
        assertThat(historyWebDir.list())
                .containsExactlyInAnyOrder(
                        "overviews", "jobs", "application-overviews", "applications");
        assertThat(new File(historyWebDir, "overviews")).exists().isDirectory().isEmptyDirectory();
        assertThat(new File(historyWebDir, "jobs").list()).containsExactly("overview.json");
        assertThat(new File(historyWebDir, "application-overviews"))
                .exists()
                .isDirectory()
                .isEmptyDirectory();
        assertThat(new File(historyWebDir, "applications").list()).containsExactly("overview.json");
    }

    private void runArchiveExpirationTest(boolean cleanupExpiredJobs) throws Exception {
        int numExpiredJobs = cleanupExpiredJobs ? 1 : 0;
        int numJobs = 3;
        for (int x = 0; x < numJobs; x++) {
            runJob();
        }
        waitForArchivesCreation(numJobs);

        CountDownLatch numExpectedArchivedJobs = new CountDownLatch(numJobs);
        CountDownLatch firstArchiveExpiredLatch = new CountDownLatch(numExpiredJobs);
        CountDownLatch allArchivesExpiredLatch =
                new CountDownLatch(cleanupExpiredJobs ? numJobs : 0);

        Configuration historyServerConfig = createTestConfiguration(cleanupExpiredJobs);

        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            switch (event.getType()) {
                                case CREATED:
                                    numExpectedArchivedJobs.countDown();
                                    break;
                                case DELETED:
                                    firstArchiveExpiredLatch.countDown();
                                    allArchivesExpiredLatch.countDown();
                                    break;
                            }
                        },
                        (event) -> {
                            throw new RuntimeException("Should not call");
                        });

        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numExpectedArchivedJobs.await(10L, TimeUnit.SECONDS)).isTrue();

            Collection<JobDetails> jobs = getJobsOverview(baseUrl).getJobs();
            assertThat(jobs).hasSize(numJobs);

            String jobIdToDelete =
                    jobs.stream()
                            .findFirst()
                            .map(JobDetails::getJobId)
                            .map(JobID::toString)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Expected at least one existing job"));

            // trigger another fetch and delete one archive from jm
            // we fetch again to probabilistically cause a concurrent deletion
            hs.fetchArchives();
            Files.deleteIfExists(jmDirectory.toPath().resolve(jobIdToDelete));

            assertThat(firstArchiveExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();

            // check that archive is still/no longer present in hs
            Collection<JobDetails> jobsAfterDeletion = getJobsOverview(baseUrl).getJobs();
            assertThat(jobsAfterDeletion).hasSize(numJobs - numExpiredJobs);
            assertThat(
                            jobsAfterDeletion.stream()
                                    .map(JobDetails::getJobId)
                                    .map(JobID::toString)
                                    .filter(jobId -> jobId.equals(jobIdToDelete))
                                    .count())
                    .isEqualTo(1 - numExpiredJobs);

            // delete remaining archives from jm and ensure files are cleaned up
            List<String> remainingJobIds =
                    jobsAfterDeletion.stream()
                            .map(JobDetails::getJobId)
                            .map(JobID::toString)
                            .collect(Collectors.toList());

            for (String remainingJobId : remainingJobIds) {
                Files.deleteIfExists(jmDirectory.toPath().resolve(remainingJobId));
            }

            assertThat(allArchivesExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();

            assertFilesCleanedUp(cleanupExpiredJobs);
        } finally {
            hs.stop();
        }
    }

    private void assertFilesCleanedUp(boolean filesShouldBeDeleted) throws IOException {
        try (Stream<Path> paths = Files.walk(hsDirectory.toPath())) {
            final List<Path> applicationOrJobFiles =
                    paths.filter(path -> !path.equals(hsDirectory.toPath()))
                            .map(path -> hsDirectory.toPath().relativize(path))
                            .filter(path -> !path.equals(Paths.get("config.json")))
                            .filter(path -> !path.equals(Paths.get("jobs")))
                            .filter(path -> !path.equals(Paths.get("jobs", "overview.json")))
                            .filter(path -> !path.equals(Paths.get("overviews")))
                            .filter(path -> !path.equals(Paths.get("applications")))
                            .filter(
                                    path ->
                                            !path.equals(
                                                    Paths.get("applications", "overview.json")))
                            .filter(path -> !path.equals(Paths.get("application-overviews")))
                            .collect(Collectors.toList());

            if (filesShouldBeDeleted) {
                assertThat(applicationOrJobFiles).isEmpty();
            } else {
                assertThat(applicationOrJobFiles).isNotEmpty();
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
        return createTestConfiguration(
                cleanupExpiredJobs,
                HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_APPLICATIONS.defaultValue());
    }

    private Configuration createTestConfiguration(
            boolean cleanupExpiredJobs, boolean cleanupExpiredApplications) {
        Configuration historyServerConfig = new Configuration();
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS, jmDirectory.toURI().toString());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_WEB_DIR, hsDirectory.getAbsolutePath());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL,
                Duration.ofMillis(100L));

        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS, cleanupExpiredJobs);
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_APPLICATIONS,
                cleanupExpiredApplications);

        historyServerConfig.set(HistoryServerOptions.HISTORY_SERVER_WEB_PORT, 0);
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

    private static void runJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(1, 2, 3).sinkTo(new DiscardingSink<>());

        env.execute();
    }

    private static JobID createLegacyArchive(
            Path directory, long fileModifiedDate, boolean versionLessThan14) throws IOException {
        JobID jobId = createLegacyArchive(directory, versionLessThan14);
        File jobArchive = directory.resolve(jobId.toString()).toFile();
        jobArchive.setLastModified(fileModifiedDate);
        return jobId;
    }

    private static JobID createLegacyArchive(Path directory, boolean versionLessThan14)
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
        FsJsonArchivist.writeArchivedJsons(
                new org.apache.flink.core.fs.Path(
                        directory.toAbsolutePath().toString(), jobId.toString()),
                Collections.singleton(archivedJson));

        return jobId;
    }

    @Test
    void testApplicationAndJobArchives() throws Exception {
        int numApplications = 2;
        int numJobsPerApplication = 2;
        // jobs that are not part of an application
        int numJobsOutsideApplication = 1;

        Map<ApplicationID, Set<JobID>> expectedApplicationAndJobIds =
                new HashMap<>(numApplications);
        for (int i = 0; i < numApplications; i++) {
            ArchivedApplication archivedApplication = mockApplicationArchive(numJobsPerApplication);
            ApplicationID applicationId = archivedApplication.getApplicationId();
            List<JobID> jobIds =
                    archivedApplication.getJobs().values().stream()
                            .map(ExecutionGraphInfo::getJobId)
                            .collect(Collectors.toList());
            expectedApplicationAndJobIds.put(applicationId, new HashSet<>(jobIds));
        }
        Set<JobID> expectedJobIdsOutsideApplication = new HashSet<>(numJobsOutsideApplication);
        for (int i = 0; i < numJobsOutsideApplication; i++) {
            ExecutionGraphInfo executionGraphInfo = createExecutionGraphInfo();
            mockJobArchive(executionGraphInfo, null);
            expectedJobIdsOutsideApplication.add(executionGraphInfo.getJobId());
        }

        int numTotalJobs = numApplications * numJobsPerApplication + numJobsOutsideApplication;
        int numTotal = numApplications + numTotalJobs;
        CountDownLatch numExpectedArchives = new CountDownLatch(numTotal);
        Configuration historyServerConfig = createTestConfiguration(false);
        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            if (event.getType()
                                    == HistoryServerArchiveFetcher.ArchiveEventType.CREATED) {
                                numExpectedArchives.countDown();
                            }
                        },
                        (event) -> {
                            if (event.getType()
                                    == HistoryServerApplicationArchiveFetcher.ArchiveEventType
                                            .CREATED) {
                                numExpectedArchives.countDown();
                            }
                        });
        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numExpectedArchives.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getApplicationAndJobIdsFromApplicationOverview(baseUrl))
                    .isEqualTo(expectedApplicationAndJobIds);
            Set<JobID> expectedJobIds =
                    Stream.concat(
                                    expectedApplicationAndJobIds.values().stream()
                                            .flatMap(Set::stream),
                                    expectedJobIdsOutsideApplication.stream())
                            .collect(Collectors.toSet());
            assertThat(getIdsFromJobOverview(baseUrl)).isEqualTo(expectedJobIds);
            // checks whether the dashboard configuration contains all expected fields
            getDashboardConfiguration(baseUrl);
        } finally {
            hs.stop();
        }
    }

    @Test
    void testRemoveApplicationArchivesBeyondHistorySizeLimit() throws Exception {
        int numJobsPerApplication = 1;
        int numApplicationsToKeepInHistory = 2;
        int numApplicationsBeforeHsStarted = 4;
        int numApplicationsAfterHsStarted = 2;
        int numApplicationsToRemoveUponHsStart =
                numApplicationsBeforeHsStarted - numApplicationsToKeepInHistory;
        List<Tuple2<ApplicationID, Set<JobID>>> expectedApplicationAndJobIdsToKeep =
                new LinkedList<>();
        for (int i = 0; i < numApplicationsBeforeHsStarted; i++) {
            ArchivedApplication archivedApplication = mockApplicationArchive(numJobsPerApplication);
            ApplicationID applicationId = archivedApplication.getApplicationId();
            List<JobID> jobIds =
                    archivedApplication.getJobs().values().stream()
                            .map(ExecutionGraphInfo::getJobId)
                            .collect(Collectors.toList());
            if (i >= numApplicationsToRemoveUponHsStart) {
                expectedApplicationAndJobIdsToKeep.add(
                        new Tuple2<>(applicationId, new HashSet<>(jobIds)));
            }
        }

        // one for application itself, numJobsPerApplication for jobs
        int numArchivesRatio = 1 + numJobsPerApplication;
        CountDownLatch numArchivesCreatedInitially =
                new CountDownLatch(numApplicationsToKeepInHistory * numArchivesRatio);
        // jobs in applications that exceed the size limit are not read by the fetcher at all,
        // so there is no need to delete these jobs.
        CountDownLatch numArchivesDeletedInitially =
                new CountDownLatch(numApplicationsToRemoveUponHsStart);
        CountDownLatch numArchivesCreatedTotal =
                new CountDownLatch(
                        (numApplicationsBeforeHsStarted
                                        - numApplicationsToRemoveUponHsStart
                                        + numApplicationsAfterHsStarted)
                                * numArchivesRatio);
        CountDownLatch numArchivesDeletedTotal =
                new CountDownLatch(
                        numApplicationsToRemoveUponHsStart
                                + numApplicationsAfterHsStarted * numArchivesRatio);
        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS.defaultValue());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_RETAINED_APPLICATIONS,
                numApplicationsToKeepInHistory);
        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            throw new RuntimeException("Should not call");
                        },
                        (event) -> {
                            if (event.getType()
                                    == HistoryServerApplicationArchiveFetcher.ArchiveEventType
                                            .CREATED) {
                                numArchivesCreatedInitially.countDown();
                                numArchivesCreatedTotal.countDown();
                            } else if (event.getType()
                                    == HistoryServerApplicationArchiveFetcher.ArchiveEventType
                                            .DELETED) {
                                numArchivesDeletedInitially.countDown();
                                numArchivesDeletedTotal.countDown();
                            }
                        });
        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numArchivesCreatedInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesDeletedInitially.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getApplicationAndJobIdsFromApplicationOverview(baseUrl))
                    .isEqualTo(
                            expectedApplicationAndJobIdsToKeep.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    tuple -> tuple.f0, tuple -> tuple.f1)));
            for (int i = numApplicationsBeforeHsStarted;
                    i < numApplicationsBeforeHsStarted + numApplicationsAfterHsStarted;
                    i++) {
                expectedApplicationAndJobIdsToKeep.remove(0);
                ArchivedApplication archivedApplication =
                        mockApplicationArchive(numJobsPerApplication);
                ApplicationID applicationId = archivedApplication.getApplicationId();
                List<JobID> jobIds =
                        archivedApplication.getJobs().values().stream()
                                .map(ExecutionGraphInfo::getJobId)
                                .collect(Collectors.toList());
                expectedApplicationAndJobIdsToKeep.add(
                        new Tuple2<>(applicationId, new HashSet<>(jobIds)));
                // avoid executing too fast, resulting in the same creation time of archive files
                Thread.sleep(50);
            }

            assertThat(numArchivesCreatedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(numArchivesDeletedTotal.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getApplicationAndJobIdsFromApplicationOverview(baseUrl))
                    .isEqualTo(
                            expectedApplicationAndJobIdsToKeep.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    tuple -> tuple.f0, tuple -> tuple.f1)));
        } finally {
            hs.stop();
        }
    }

    @Test
    void testFailIfApplicationHistorySizeLimitIsZero() {
        assertThatThrownBy(() -> startHistoryServerWithApplicationSizeLimit(0))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testFailIfApplicationHistorySizeLimitIsLessThanMinusOne() {
        assertThatThrownBy(() -> startHistoryServerWithApplicationSizeLimit(-2))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    private void startHistoryServerWithApplicationSizeLimit(int maxHistorySize)
            throws IOException, FlinkException, InterruptedException {
        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_APPLICATIONS
                                .defaultValue());
        historyServerConfig.set(
                HistoryServerOptions.HISTORY_SERVER_RETAINED_APPLICATIONS, maxHistorySize);
        new HistoryServer(historyServerConfig).start();
    }

    @Test
    void testCleanExpiredApplication() throws Exception {
        runApplicationArchiveExpirationTest(true);
    }

    @Test
    void testRemainExpiredApplication() throws Exception {
        runApplicationArchiveExpirationTest(false);
    }

    private void runApplicationArchiveExpirationTest(boolean cleanupExpiredApplications)
            throws Exception {
        int numExpiredApplications = cleanupExpiredApplications ? 1 : 0;
        int numApplications = 3;
        int numJobsPerApplication = 1;

        Map<ApplicationID, Set<JobID>> expectedApplicationAndJobIds =
                new HashMap<>(numApplications);
        for (int i = 0; i < numApplications; i++) {
            ArchivedApplication archivedApplication = mockApplicationArchive(numJobsPerApplication);
            ApplicationID applicationId = archivedApplication.getApplicationId();
            List<JobID> jobIds =
                    archivedApplication.getJobs().values().stream()
                            .map(ExecutionGraphInfo::getJobId)
                            .collect(Collectors.toList());
            expectedApplicationAndJobIds.put(applicationId, new HashSet<>(jobIds));
        }

        // one for application itself, numJobsPerApplication for jobs
        int numArchivesRatio = 1 + numJobsPerApplication;
        CountDownLatch numExpectedArchives = new CountDownLatch(numApplications * numArchivesRatio);
        CountDownLatch firstArchiveExpiredLatch =
                new CountDownLatch(numExpiredApplications * numArchivesRatio);
        CountDownLatch allArchivesExpiredLatch =
                new CountDownLatch(
                        cleanupExpiredApplications ? numApplications * numArchivesRatio : 0);

        Configuration historyServerConfig =
                createTestConfiguration(
                        HistoryServerOptions.HISTORY_SERVER_CLEANUP_EXPIRED_JOBS.defaultValue(),
                        cleanupExpiredApplications);
        HistoryServer hs =
                new HistoryServer(
                        historyServerConfig,
                        (event) -> {
                            throw new RuntimeException("Should not call");
                        },
                        (event) -> {
                            if (event.getType()
                                    == HistoryServerApplicationArchiveFetcher.ArchiveEventType
                                            .CREATED) {
                                numExpectedArchives.countDown();
                            } else if (event.getType()
                                    == HistoryServerApplicationArchiveFetcher.ArchiveEventType
                                            .DELETED) {
                                firstArchiveExpiredLatch.countDown();
                                allArchivesExpiredLatch.countDown();
                            }
                        });
        try {
            hs.start();
            String baseUrl = "http://localhost:" + hs.getWebPort();
            assertThat(numExpectedArchives.await(10L, TimeUnit.SECONDS)).isTrue();
            assertThat(getApplicationAndJobIdsFromApplicationOverview(baseUrl))
                    .isEqualTo(expectedApplicationAndJobIds);
            ApplicationID applicationIdToDelete =
                    expectedApplicationAndJobIds.keySet().stream()
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Expected at least one application"));
            if (cleanupExpiredApplications) {
                expectedApplicationAndJobIds.remove(applicationIdToDelete);
            }
            // trigger another fetch and delete one archive from jm
            // we fetch again to probabilistically cause a concurrent deletion
            hs.fetchArchives();
            deleteApplicationArchiveDir(applicationIdToDelete);

            assertThat(firstArchiveExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            // check that archive is still/no longer present in hs
            assertThat(getApplicationAndJobIdsFromApplicationOverview(baseUrl))
                    .isEqualTo(expectedApplicationAndJobIds);
            for (ApplicationID remainingApplicationId : expectedApplicationAndJobIds.keySet()) {
                deleteApplicationArchiveDir(remainingApplicationId);
            }
            assertThat(allArchivesExpiredLatch.await(10L, TimeUnit.SECONDS)).isTrue();
            assertFilesCleanedUp(cleanupExpiredApplications);
        } finally {
            hs.stop();
        }
    }

    private Map<ApplicationID, Set<JobID>> getApplicationAndJobIdsFromApplicationOverview(
            String baseUrl) throws Exception {
        Set<ApplicationID> applicationIds =
                getApplicationsOverview(baseUrl).getApplications().stream()
                        .map(ApplicationDetails::getApplicationId)
                        .collect(Collectors.toSet());
        Map<ApplicationID, Set<JobID>> applicationAndJobIds = new HashMap<>(applicationIds.size());
        for (ApplicationID applicationId : applicationIds) {
            Set<JobID> jobIds =
                    getApplicationDetails(baseUrl, applicationId).getJobs().stream()
                            .map(JobDetails::getJobId)
                            .collect(Collectors.toSet());
            applicationAndJobIds.put(applicationId, jobIds);
        }
        return applicationAndJobIds;
    }

    private static MultipleApplicationsDetails getApplicationsOverview(String baseUrl)
            throws Exception {
        Tuple2<Integer, String> response =
                HttpUtils.getFromHTTP(baseUrl + ApplicationsOverviewHeaders.URL);
        return OBJECT_MAPPER.readValue(response.f1, MultipleApplicationsDetails.class);
    }

    private static ApplicationDetailsInfo getApplicationDetails(
            String baseUrl, ApplicationID applicationId) throws Exception {
        Tuple2<Integer, String> response =
                HttpUtils.getFromHTTP(
                        baseUrl
                                + ApplicationDetailsHeaders.URL.replace(
                                        ':' + ApplicationIDPathParameter.KEY,
                                        applicationId.toString()));
        return OBJECT_MAPPER.readValue(response.f1, ApplicationDetailsInfo.class);
    }

    private ArchivedApplication mockApplicationArchive(int numJobs) throws IOException {
        ArchivedApplication archivedApplication = createArchivedApplication(numJobs);
        ApplicationID applicationId = archivedApplication.getApplicationId();
        ArchivedJson archivedApplicationsOverview =
                new ArchivedJson(
                        ApplicationsOverviewHeaders.URL,
                        new MultipleApplicationsDetails(
                                Collections.singleton(
                                        ApplicationDetails.fromArchivedApplication(
                                                archivedApplication))));
        ArchivedJson archivedApplicationDetails =
                new ArchivedJson(
                        ApplicationDetailsHeaders.URL.replace(
                                ':' + ApplicationIDPathParameter.KEY, applicationId.toString()),
                        ApplicationDetailsInfo.fromArchivedApplication(archivedApplication));
        // set cluster id to application id to simplify the test
        clusterConfig.set(ClusterOptions.CLUSTER_ID, applicationId.toString());
        FsJsonArchivist.writeArchivedJsons(
                ArchivePathUtils.getApplicationArchivePath(clusterConfig, applicationId),
                Arrays.asList(archivedApplicationsOverview, archivedApplicationDetails));

        Map<JobID, ExecutionGraphInfo> jobs = archivedApplication.getJobs();
        for (Map.Entry<JobID, ExecutionGraphInfo> jobEntry : jobs.entrySet()) {
            mockJobArchive(jobEntry.getValue(), applicationId);
        }
        return archivedApplication;
    }

    private void mockJobArchive(
            ExecutionGraphInfo executionGraphInfo, @Nullable ApplicationID applicationId)
            throws IOException {
        JobID jobId = executionGraphInfo.getJobId();
        ArchivedJson archivedJobsOverview =
                new ArchivedJson(
                        JobsOverviewHeaders.URL,
                        new MultipleJobsDetails(
                                Collections.singleton(
                                        JobDetails.createDetailsForJob(executionGraphInfo))));
        FsJsonArchivist.writeArchivedJsons(
                ArchivePathUtils.getJobArchivePath(clusterConfig, jobId, applicationId),
                Collections.singletonList(archivedJobsOverview));
    }

    private ArchivedApplication createArchivedApplication(int numJobs) {
        ApplicationID applicationId = ApplicationID.generate();
        Map<JobID, ExecutionGraphInfo> jobs = new HashMap<>(numJobs);
        for (int i = 0; i < numJobs; i++) {
            ExecutionGraphInfo executionGraphInfo = createExecutionGraphInfo();
            jobs.put(executionGraphInfo.getJobId(), executionGraphInfo);
        }
        return new ArchivedApplication(
                applicationId,
                "test-application",
                ApplicationState.FINISHED,
                new long[ApplicationState.values().length],
                jobs);
    }

    private ExecutionGraphInfo createExecutionGraphInfo() {
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        JobID.generate(), "test-job", JobStatus.FINISHED, null, null, null, 0));
    }

    private void deleteApplicationArchiveDir(ApplicationID applicationId) throws IOException {
        // set cluster id to application id to simplify the test
        clusterConfig.set(ClusterOptions.CLUSTER_ID, applicationId.toString());
        org.apache.flink.core.fs.Path applicationArchiveDir =
                ArchivePathUtils.getApplicationArchivePath(clusterConfig, applicationId)
                        .getParent();
        applicationArchiveDir.getFileSystem().delete(applicationArchiveDir, true);
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
