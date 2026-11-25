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
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.ArchivePathUtils;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleApplicationsDetails;
import org.apache.flink.runtime.rest.messages.ApplicationsOverviewHeaders;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ClusterOptions.CLUSTER_ID;
import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.EAGER;
import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.LAZY;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.CREATED;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.DETAIL_PARSING;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.OVERVIEW_CREATED;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.OBJECT_MAPPER;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.RETAIN_ALL;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createJobArchive;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createRefreshLocation;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.waitForArchiveLoaded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link HistoryServerApplicationArchiveFetcher}. Only application-specific behaviours
 * that are NOT covered by {@link HistoryServerArchiveFetcherTest} are tested here.
 */
@ExtendWith(ParameterizedTestExtension.class)
class HistoryServerApplicationArchiveFetcherTest {

    private static final String DEFAULT_CLUSTER_ID = CLUSTER_ID.defaultValue();

    @TempDir File remoteArchiveRootPath;
    @TempDir File localArchiveRootPath;

    @Parameter public ArchiveStorageFactory<Object> storageFactory;

    private ArchiveStorage<Object> archiveStorage;

    private ConcurrentHashMap<String, ArchiveMetaInfo> jobMetaInfoCache;
    private ConcurrentHashMap<String, ArchiveMetaInfo> applicationMetaInfoCache;
    private List<HistoryServerArchiveFetcher.ArchiveEvent> archiveEvents;

    @Parameters(name = "storageFactory={0}")
    private static Collection<ArchiveStorageFactory<?>> storageFactories() {
        ArchiveStorageFactory<File> fileArchiveStorageFactory = FileArchiveStorage::new;
        ArchiveStorageFactory<String> rocksDBStorageFactory = RocksDBArchiveStorage::new;
        return List.of(fileArchiveStorageFactory, rocksDBStorageFactory);
    }

    /** Creates an {@link ArchiveStorage} instance under the given temporary directory. */
    @FunctionalInterface
    interface ArchiveStorageFactory<T> {
        ArchiveStorage<T> create(File tempDir) throws Exception;
    }

    @BeforeEach
    void setUp() throws Exception {
        jobMetaInfoCache = new ConcurrentHashMap<>();
        applicationMetaInfoCache = new ConcurrentHashMap<>();
        archiveEvents = new ArrayList<>();
        archiveStorage = storageFactory.create(localArchiveRootPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        archiveEvents.clear();
        if (archiveStorage != null) {
            archiveStorage.close();
            archiveStorage = null;
        }
    }

    /**
     * Create an application archive at {@code remoteArchiveRootPath/<CLUSTER_ID>/applications/
     * <applicationId>/} which contains an {@code application-summary} archive (representing the
     * application overview) and one job archive per job in {@code jobs/}.
     *
     * <pre>{@code
     * remoteArchiveRootPath/
     * ├── <cluster-id-1>/
     * │   └── applications/
     * │       ├── <application-id-1>/
     * │       │   ├── application-summary
     * │       │   └── jobs/
     * │       │       ├── <job-id-1>
     * │       │       └── ...
     * │       └── ...
     * }</pre>
     *
     * @param applicationId application id (must be a hex string parseable by {@link
     *     ApplicationID#fromHexString})
     * @param jobIds job ids whose archives are to be placed under the {@code jobs/} subdir
     * @return the application archive directory path
     */
    private Path createApplicationArchive(String applicationId, List<JobID> jobIds)
            throws Exception {
        File applicationDir =
                new File(
                        remoteArchiveRootPath,
                        DEFAULT_CLUSTER_ID
                                + "/"
                                + ArchivePathUtils.APPLICATIONS_DIR
                                + "/"
                                + applicationId);
        Files.createDirectories(applicationDir.toPath());

        // Write application-summary archive (the application overview entry).
        ApplicationID appId = ApplicationID.fromHexString(applicationId);
        Map<String, Integer> jobInfo = new HashMap<>();
        jobInfo.put("FINISHED", jobIds.size());
        ApplicationDetails applicationDetails =
                new ApplicationDetails(appId, "test-app", 0L, 1L, 1L, "FINISHED", jobInfo);
        String applicationOverviewJson =
                OBJECT_MAPPER.writeValueAsString(
                        new MultipleApplicationsDetails(Collections.singleton(applicationDetails)));
        ArchivedJson applicationOverviewArchive =
                new ArchivedJson(ApplicationsOverviewHeaders.URL, applicationOverviewJson);

        // mock a simple /applications/<applicationid>.json
        String applicationJson = "{\"id\":\"" + applicationId + "\"}";
        List<ArchivedJson> archives = new ArrayList<>();
        archives.add(applicationOverviewArchive);
        archives.add(new ArchivedJson("/applications/" + applicationId, applicationJson));

        Path applicationSummaryPath =
                new Path(
                        applicationDir.toURI().toString(),
                        ArchivePathUtils.APPLICATION_ARCHIVE_NAME);
        FsJsonArchivist.writeArchivedJsons(applicationSummaryPath, archives);

        // Write job archives under jobs/ subdir.
        File jobsDir = new File(applicationDir, ArchivePathUtils.JOBS_DIR);
        Files.createDirectories(jobsDir.toPath());
        for (JobID jobId : jobIds) {
            createJobArchive(jobsDir, jobId, true);
        }

        return new Path(applicationDir.toURI().toString());
    }

    private HistoryServerApplicationArchiveFetcher<Object> createApplicationArchiveFetcher(
            boolean cleanupExpired, ArchiveStorage<Object> storage) throws Exception {
        List<HistoryServer.RefreshLocation> refreshDirs =
                Collections.singletonList(createRefreshLocation(remoteArchiveRootPath));
        return new HistoryServerApplicationArchiveFetcher<>(
                refreshDirs,
                localArchiveRootPath,
                event -> archiveEvents.add(event),
                cleanupExpired,
                RETAIN_ALL,
                storage,
                jobMetaInfoCache,
                applicationMetaInfoCache,
                4);
    }

    private static String newApplicationId() {
        return new ApplicationID().toHexString();
    }

    // =========================================================================
    // EAGER MODE TESTS
    //
    // localArchiveRootPath/
    // ├── application-overviews/
    // │   └── application-id-1.json
    // │   └── ...
    // ├── applications/
    // │   └── overview.json
    // │   └── application-id-1.json
    // │   └── ...
    // ├── overviews/
    // │   └── job-id-1.json
    // │   └── ...
    // ├── jobs/
    // │   └── overview.json
    // │   └── job-id-1.json
    // │   └── job-id-1/
    // │       ├── detail.json
    // │       └── ...
    // =========================================================================

    @TestTemplate
    void testEagerModeLoadsAllAppsAndJobsSync() throws Exception {
        int numApps = 2;
        int numJobsPerApp = 2;
        List<String> appIds = new ArrayList<>();
        Map<String, List<JobID>> appToJobs = new HashMap<>();
        for (int i = 0; i < numApps; i++) {
            String appId = newApplicationId();
            appIds.add(appId);
            List<JobID> jobIds = new ArrayList<>();
            for (int j = 0; j < numJobsPerApp; j++) {
                jobIds.add(JobID.generate());
            }
            appToJobs.put(appId, jobIds);
            createApplicationArchive(appId, jobIds);
        }

        HistoryServerApplicationArchiveFetcher<Object> fetcher =
                createApplicationArchiveFetcher(false, archiveStorage);
        fetcher.fetchArchives(EAGER);

        // N (apps) + N*M (jobs) events, all CREATED
        assertThat(archiveEvents).hasSize(numApps + numApps * numJobsPerApp);
        for (HistoryServerArchiveFetcher.ArchiveEvent event : archiveEvents) {
            assertThat(event.getType()).isEqualTo(CREATED);
        }

        for (String appId : appIds) {
            assertThat(archiveStorage.exists("application-overviews/" + appId + ".json")).isTrue();
            assertThat(archiveStorage.exists("applications/" + appId + ".json")).isTrue();
            for (JobID jobId : appToJobs.get(appId)) {
                assertThat(archiveStorage.exists("overviews/" + jobId + ".json")).isTrue();
                assertThat(archiveStorage.exists("jobs/" + jobId + ".json")).isTrue();
                assertThat(archiveStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
            }
        }
        assertThat(archiveStorage.exists("jobs/overview.json")).isTrue();
        assertThat(archiveStorage.exists("applications/overview.json")).isTrue();
    }

    // =========================================================================
    // LAZY MODE TESTS
    // =========================================================================

    @TestTemplate
    void testLazyModeApplicationSyncJobDetailAsync() throws Exception {
        String appId = newApplicationId();
        JobID jobId = JobID.generate();
        createApplicationArchive(appId, Collections.singletonList(jobId));

        HistoryServerTestUtils.BlockingArchiveStorage<Object> blockingStorage =
                new HistoryServerTestUtils.BlockingArchiveStorage<>(
                        archiveStorage, "jobs/" + jobId + "/config");
        // Replace the field so that close() in tearDown() releases the underlying storage too.
        archiveStorage = blockingStorage;

        HistoryServerApplicationArchiveFetcher<Object> fetcher =
                createApplicationArchiveFetcher(false, blockingStorage);
        fetcher.fetchArchives(LAZY);

        // The application-level event is CREATED (application is loaded synchronously);
        // the embedded job-level event is OVERVIEW_CREATED (detail is loaded asynchronously).
        List<HistoryServerArchiveFetcher.ArchiveEventType> eventTypes =
                archiveEvents.stream()
                        .map(HistoryServerArchiveFetcher.ArchiveEvent::getType)
                        .collect(Collectors.toList());
        assertThat(eventTypes).containsExactlyInAnyOrder(CREATED, OVERVIEW_CREATED);

        // localArchiveRootPath/
        // ├── application-overviews/
        // │   └── application-id-1.json
        // │   └── ...
        // ├── applications/
        // │   └── overview.json
        // │   └── application-id-1.json
        // │   └── ...
        // ├── overviews/
        // │   └── job-id-1.json
        // │   └── ...
        // ├── jobs/
        // │   └── overview.json
        // │   └── job-id-1.json
        // │   └── job-id-1/
        // │       ├── detail.json
        // │       └── ...

        // Phase 1: detail not yet written
        boolean asyncReached = blockingStorage.asyncStartLatch.await(10, TimeUnit.SECONDS);
        assertThat(asyncReached).isTrue();
        assertThat(blockingStorage.exists("jobs/" + jobId + "/config.json")).isFalse();
        // application-summary content has been written synchronously
        assertThat(blockingStorage.exists("application-overviews/" + appId + ".json")).isTrue();
        assertThat(blockingStorage.exists("applications/" + appId + ".json")).isTrue();
        assertThat(blockingStorage.exists("applications/overview.json")).isTrue();
        assertThat(blockingStorage.exists("overviews/" + jobId + ".json")).isTrue();
        assertThat(blockingStorage.exists("jobs/" + jobId + ".json")).isTrue();
        assertThat(blockingStorage.exists("jobs/overview.json")).isTrue();

        // Phase 2: release async task and wait for completion
        blockingStorage.releaseLatch.countDown();
        waitForArchiveLoaded(jobMetaInfoCache, jobId.toString());
        assertThat(blockingStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
        assertThat(jobMetaInfoCache.get(jobId.toString()).getEventType()).isEqualTo(CREATED);
    }

    @TestTemplate
    void testMissingApplicationSummaryFileThrows() throws Exception {
        String appId = newApplicationId();
        // Create the directory layout WITHOUT writing the application-summary file.
        File applicationDir =
                new File(
                        remoteArchiveRootPath,
                        DEFAULT_CLUSTER_ID + "/" + ArchivePathUtils.APPLICATIONS_DIR + "/" + appId);
        Files.createDirectories(new File(applicationDir, ArchivePathUtils.JOBS_DIR).toPath());

        HistoryServerApplicationArchiveFetcher<Object> fetcher =
                createApplicationArchiveFetcher(false, archiveStorage);
        Path refreshPath = new Path(remoteArchiveRootPath.toURI().toString());
        Path applicationPath = new Path(applicationDir.toURI().toString());

        assertThatThrownBy(() -> fetcher.processArchive(appId, applicationPath, refreshPath))
                .hasMessageContaining(
                        "Application archive "
                                + new Path(
                                        applicationPath, ArchivePathUtils.APPLICATION_ARCHIVE_NAME)
                                + " does not exist.");

        // Cache should NOT contain a successful entry for this application.
        assertThat(applicationMetaInfoCache.containsKey(appId)).isFalse();
    }

    // =========================================================================
    // ApplicationArchiveMetaInfoCache STATE TRANSITION TESTS
    // =========================================================================

    @TestTemplate
    void testApplicationArchiveStateTransition() throws Exception {
        String appId = newApplicationId();
        JobID jobId = JobID.generate();
        Path applicationArchivePath =
                createApplicationArchive(appId, Collections.singletonList(jobId));

        HistoryServerTestUtils.BlockingArchiveStorage<Object> blockingStorage =
                new HistoryServerTestUtils.BlockingArchiveStorage<>(
                        archiveStorage, "jobs/" + jobId + "/config");
        archiveStorage = blockingStorage;

        HistoryServerApplicationArchiveFetcher<Object> fetcher =
                createApplicationArchiveFetcher(false, blockingStorage);
        Path refreshPath = new Path(remoteArchiveRootPath.toURI().toString());

        fetcher.fetchArchives(LAZY);

        // Phase 1: application is loaded synchronously -> CREATED;
        assertThat(applicationMetaInfoCache.get(appId).getEventType()).isEqualTo(CREATED);

        // the embedded job's detail loading is blocked at the storage -> DETAIL_PARSING.
        boolean asyncReached = blockingStorage.asyncStartLatch.await(10, TimeUnit.SECONDS);
        assertThat(asyncReached).isTrue();
        assertThat(jobMetaInfoCache.get(jobId.toString()).getEventType()).isEqualTo(DETAIL_PARSING);

        // try to call lazyProcessJobArchive again, should return CREATED.
        List<HistoryServerArchiveFetcher.ArchiveEvent> callAgainEvent =
                fetcher.lazyProcessArchive(appId, applicationArchivePath, refreshPath);
        assertThat(callAgainEvent.get(0).getType()).isEqualTo(CREATED);
        assertThat(applicationMetaInfoCache.get(appId).getEventType()).isEqualTo(CREATED);

        // Phase 2: execute the asynchronous task
        blockingStorage.releaseLatch.countDown();
        waitForArchiveLoaded(jobMetaInfoCache, jobId.toString());
        assertThat(jobMetaInfoCache.get(jobId.toString()).getEventType()).isEqualTo(CREATED);

        // Phase 3: cleanup all archives
        Map<Path, Set<String>> archivesToRemove = new HashMap<>();
        archivesToRemove.put(refreshPath, Collections.singleton(appId));
        fetcher.cleanupExpiredArchives(archivesToRemove);

        assertThat(applicationMetaInfoCache.containsKey(appId)).isFalse();
        assertThat(jobMetaInfoCache.containsKey(jobId.toString())).isFalse();
    }
}
