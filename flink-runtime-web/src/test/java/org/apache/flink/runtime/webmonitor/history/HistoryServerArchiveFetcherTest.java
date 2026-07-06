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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.EAGER;
import static org.apache.flink.configuration.HistoryServerOptions.HistoryServerArchiveLoadMode.LAZY;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.CREATED;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.DETAIL_PARSING;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.OVERVIEW_CREATED;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.OBJECT_MAPPER;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.RETAIN_ALL;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createJobArchive;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createLegacyArchive;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.createRefreshLocation;
import static org.apache.flink.runtime.webmonitor.history.HistoryServerTestUtils.waitForArchiveLoaded;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HistoryServerArchiveFetcher}. */
@ExtendWith(ParameterizedTestExtension.class)
class HistoryServerArchiveFetcherTest {

    @TempDir File remoteArchiveRootPath;
    @TempDir File localArchiveRootPath;

    @Parameter public ArchiveStorageFactory<Object> storageFactory;

    private ArchiveStorage<Object> archiveStorage;
    private ConcurrentHashMap<String, ArchiveMetaInfo> archiveMetaInfoCache;
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
        archiveMetaInfoCache = new ConcurrentHashMap<>();
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
     * Create {@link HistoryServerArchiveFetcher} instance with a custom {@link ArchiveStorage}.
     *
     * @param refreshDir archive scan directory
     * @param cleanupExpiredJobs whether to enable expired archive cleanup
     * @param storage the archive storage to use
     */
    private HistoryServerArchiveFetcher<?> createArchiveFetcher(
            File refreshDir, boolean cleanupExpiredJobs, ArchiveStorage<?> storage)
            throws Exception {
        List<HistoryServer.RefreshLocation> refreshDirs =
                Collections.singletonList(createRefreshLocation(refreshDir));
        return new HistoryServerArchiveFetcher<>(
                refreshDirs,
                localArchiveRootPath,
                event -> archiveEvents.add(event),
                cleanupExpiredJobs,
                RETAIN_ALL,
                storage,
                archiveMetaInfoCache,
                4);
    }

    // =========================================================================
    // EAGER MODE TESTS
    // =========================================================================

    @TestTemplate
    void testEagerModeLoadsAllFilesSync() throws Exception {
        int numJobs = 3;
        List<JobID> jobIds = new ArrayList<>();
        for (int i = 0; i < numJobs; i++) {
            JobID jobId = JobID.generate();
            jobIds.add(jobId);
            createJobArchive(remoteArchiveRootPath, jobId, true);
        }

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, archiveStorage);
        fetcher.fetchArchives(EAGER);

        assertThat(archiveEvents).hasSize(numJobs);
        for (HistoryServerArchiveFetcher.ArchiveEvent event : archiveEvents) {
            assertThat(event.getType()).isEqualTo(CREATED);
        }
        for (JobID jobId : jobIds) {
            assertThat(archiveStorage.exists("overviews/" + jobId + ".json")).isTrue();
            assertThat(archiveStorage.exists("jobs/" + jobId + ".json")).isTrue();
            assertThat(archiveStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
        }
        assertThat(archiveStorage.exists("jobs/overview.json")).isTrue();
    }

    // =========================================================================
    // LAZY MODE TESTS
    // =========================================================================

    @TestTemplate
    void testLazyModeDetailNotWrittenBeforeAsyncTaskCompleted() throws Exception {
        JobID jobId = JobID.generate();
        createJobArchive(remoteArchiveRootPath, jobId, true);

        HistoryServerTestUtils.BlockingArchiveStorage<Object> blockingStorage =
                new HistoryServerTestUtils.BlockingArchiveStorage<>(
                        archiveStorage, "jobs/" + jobId + "/config");
        // Replace the field so that close() in tearDown() releases the underlying storage too.
        archiveStorage = blockingStorage;

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, blockingStorage);

        fetcher.fetchArchives(LAZY);

        // Phase 1: overview archive is loaded synchronously, but detail archive haven't loaded yet.
        boolean asyncReached = blockingStorage.asyncStartLatch.await(10, TimeUnit.SECONDS);
        assertThat(asyncReached).isTrue();
        assertThat(archiveEvents.get(0).getType()).isEqualTo(OVERVIEW_CREATED);
        assertThat(blockingStorage.exists("overviews/" + jobId + ".json")).isTrue();
        assertThat(blockingStorage.exists("jobs/" + jobId + ".json")).isTrue();
        assertThat(blockingStorage.exists("jobs/overview.json")).isTrue();
        assertThat(blockingStorage.exists("jobs/" + jobId + "/config.json")).isFalse();

        // Phase 2: execute the asynchronous task
        blockingStorage.releaseLatch.countDown();
        waitForArchiveLoaded(archiveMetaInfoCache, jobId.toString());

        assertThat(archiveMetaInfoCache.get(jobId.toString()).getEventType()).isEqualTo(CREATED);
        assertThat(blockingStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
    }

    @TestTemplate
    void testLazyModeLoadsAllFilesCompleted() throws Exception {
        int numJobs = 5;
        List<JobID> jobIds = new ArrayList<>();
        for (int i = 0; i < numJobs; i++) {
            JobID jobId = JobID.generate();
            jobIds.add(jobId);
            createJobArchive(remoteArchiveRootPath, jobId, true);
        }

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, archiveStorage);
        fetcher.fetchArchives(LAZY);

        assertThat(archiveEvents).hasSize(numJobs);
        for (HistoryServerArchiveFetcher.ArchiveEvent event : archiveEvents) {
            assertThat(event.getType()).isEqualTo(OVERVIEW_CREATED);
        }

        for (JobID jobId : jobIds) {
            waitForArchiveLoaded(archiveMetaInfoCache, jobId.toString());
        }

        // when async task completed, all detail archives should be loaded.
        for (JobID jobId : jobIds) {
            assertThat(archiveMetaInfoCache.get(jobId.toString()).getEventType())
                    .isEqualTo(CREATED);
            assertThat(archiveStorage.exists("jobs/" + jobId + "/config.json")).isTrue();
        }
    }

    @TestTemplate
    void testLazyProcessJobArchiveThrowsForEmptyArchive() throws Exception {
        JobID jobId = JobID.generate();
        Path archivePath = new Path(remoteArchiveRootPath.toURI().toString(), jobId.toString());
        FsJsonArchivist.writeArchivedJsons(archivePath, Collections.emptyList());

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, archiveStorage);

        assertThatThrownBy(() -> fetcher.lazyProcessJobArchive(jobId.toString(), archivePath))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Archive of job " + jobId + " is empty");

        assertThat(archiveMetaInfoCache.containsKey(jobId.toString())).isFalse();
    }

    // =========================================================================
    // ArchiveMetaInfoCache STATE TRANSITION TESTS
    // =========================================================================

    /** STATE: PENDING -> OVERVIEW_PARSING -> OVERVIEW_CREATED -> DETAIL_PARSING -> CREATED. */
    @TestTemplate
    void testArchiveStateTransition() throws Exception {
        JobID jobIdWithDetail = JobID.generate();
        Path jobWithDetailArchivePath =
                createJobArchive(remoteArchiveRootPath, jobIdWithDetail, true);
        JobID jobIdWithoutDetail = JobID.generate();
        createJobArchive(remoteArchiveRootPath, jobIdWithoutDetail, false);

        HistoryServerTestUtils.BlockingArchiveStorage<Object> blockingStorage =
                new HistoryServerTestUtils.BlockingArchiveStorage<>(
                        archiveStorage, "jobs/" + jobIdWithDetail + "/config");
        archiveStorage = blockingStorage;

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, blockingStorage);

        fetcher.fetchArchives(LAZY);

        assertThat(archiveMetaInfoCache.size()).isEqualTo(2);
        assertThat(archiveMetaInfoCache.containsKey(jobIdWithoutDetail.toString())).isTrue();
        assertThat(archiveMetaInfoCache.get(jobIdWithoutDetail.toString()).getEventType())
                .isEqualTo(OVERVIEW_CREATED);

        assertThat(archiveMetaInfoCache.containsKey(jobIdWithDetail.toString())).isTrue();
        // Phase 1: overview archive is loaded synchronously, but detail archive haven't loaded yet.
        boolean asyncReached = blockingStorage.asyncStartLatch.await(10, TimeUnit.SECONDS);
        assertThat(asyncReached).isTrue();
        assertThat(archiveMetaInfoCache.get(jobIdWithDetail.toString()).getEventType())
                .isEqualTo(DETAIL_PARSING);

        // try to call lazyProcessJobArchive again, should return DETAIL_PARSING.
        HistoryServerArchiveFetcher.ArchiveEvent callAgainEvent =
                fetcher.lazyProcessJobArchive(jobIdWithDetail.toString(), jobWithDetailArchivePath);
        assertThat(callAgainEvent.getType()).isEqualTo(DETAIL_PARSING);

        // Phase 2: execute the asynchronous task
        blockingStorage.releaseLatch.countDown();
        waitForArchiveLoaded(archiveMetaInfoCache, jobIdWithDetail.toString());
        assertThat(archiveMetaInfoCache.get(jobIdWithDetail.toString()).getEventType())
                .isEqualTo(CREATED);

        // Phase 3: remove all archives
        Map<Path, Set<String>> archivesToRemove = new HashMap<>();
        Set<String> archiveIdsToRemove = new HashSet<>();
        archiveIdsToRemove.add(jobIdWithDetail.toString());
        archiveIdsToRemove.add(jobIdWithoutDetail.toString());
        archivesToRemove.put(
                new Path(remoteArchiveRootPath.toURI().toString()), archiveIdsToRemove);
        fetcher.cleanupExpiredArchives(archivesToRemove);
        assertThat(archiveMetaInfoCache.size()).isEqualTo(0);
    }

    // =========================================================================
    // Other Tests
    // =========================================================================

    /**
     * This test reads {@code jobs/overview.json} directly from the local file system, so it only
     * applies to the {@link FileArchiveStorage} backend.
     */
    @TestTemplate
    void testUpdateJobOverview() throws Exception {
        JobID job1 = JobID.generate();
        JobID job2 = JobID.generate();
        createJobArchive(remoteArchiveRootPath, job1, true);
        createJobArchive(remoteArchiveRootPath, job2, true);

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, true, archiveStorage);
        fetcher.fetchArchives(EAGER);

        Object overviewObject = archiveStorage.getEntry("jobs/overview.json");
        String overviewContent = archiveStorage.readArchiveContent(overviewObject);
        MultipleJobsDetails overview =
                OBJECT_MAPPER.readValue(overviewContent, MultipleJobsDetails.class);

        assertThat(overview.getJobs()).hasSize(2);
        Set<String> jobIds = new HashSet<>();
        for (JobDetails jobDetails : overview.getJobs()) {
            jobIds.add(jobDetails.getJobId().toString());
        }
        assertThat(jobIds).containsExactlyInAnyOrder(job1.toString(), job2.toString());

        // remove job1
        new File(remoteArchiveRootPath, job1.toString()).delete();
        fetcher.fetchArchives(EAGER);

        // verify overview now only contains job2
        overviewObject = archiveStorage.getEntry("jobs/overview.json");
        overviewContent = archiveStorage.readArchiveContent(overviewObject);
        overview = OBJECT_MAPPER.readValue(overviewContent, MultipleJobsDetails.class);
        assertThat(overview.getJobs()).hasSize(1);
        assertThat(overview.getJobs().iterator().next().getJobId()).isEqualTo(job2);
    }

    @TestTemplate
    void testLegacyJobOverviewMigration() throws Exception {
        JobID jobId = createLegacyArchive(remoteArchiveRootPath.toPath(), false);

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, false, archiveStorage);
        fetcher.fetchArchives(LAZY);

        assertThat(archiveEvents).hasSize(1);
        assertThat(archiveEvents.get(0).getType()).isEqualTo(OVERVIEW_CREATED);

        assertThat(archiveStorage.exists("overviews/" + jobId + ".json")).isTrue();
        assertThat(archiveStorage.exists("jobs/overview.json")).isTrue();
    }
}
