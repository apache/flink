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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoryServerArchiveFetcher}. */
@ExtendWith(ParameterizedTestExtension.class)
class HistoryServerArchiveFetcherTest {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    @TempDir File remoteArchiveRootPath;
    @TempDir File localArchiveRootPath;

    @Parameter public ArchiveStorageFactory<Object> storageFactory;

    private ArchiveStorage<Object> archiveStorage;

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
        archiveStorage = storageFactory.create(localArchiveRootPath);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (archiveStorage != null) {
            archiveStorage.close();
        }
    }

    private HistoryServerArchiveFetcher<?> createArchiveFetcher(
            File refreshDir, boolean cleanupExpiredJobs, ArchiveStorage<?> storage)
            throws Exception {
        Path path = new Path(refreshDir.toURI().toString());
        return new HistoryServerArchiveFetcher<>(
                Collections.singletonList(
                        new HistoryServer.RefreshLocation(path, path.getFileSystem())),
                localArchiveRootPath,
                ignored -> {},
                cleanupExpiredJobs,
                (file, index) -> true,
                storage);
    }

    private static void createJobArchive(File root, JobID jobId) throws Exception {
        JobDetails job =
                new JobDetails(
                        jobId,
                        "test-job",
                        0L,
                        1L,
                        1L,
                        JobStatus.FINISHED,
                        1L,
                        new int[ExecutionState.values().length],
                        0);
        String json =
                OBJECT_MAPPER.writeValueAsString(
                        new MultipleJobsDetails(Collections.singleton(job)));
        FsJsonArchivist.writeArchivedJsons(
                new Path(root.toURI().toString(), jobId.toString()),
                Collections.singleton(new ArchivedJson(JobsOverviewHeaders.URL, json)));
    }

    @TestTemplate
    void testUpdateJobOverviewSkipsMalformedEntryInsteadOfFailingEverything() throws Exception {
        JobID goodJob = JobID.generate();
        createJobArchive(remoteArchiveRootPath, goodJob);

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, true, archiveStorage);
        fetcher.fetchArchives();

        // inject a malformed per-job overview entry alongside the good one
        archiveStorage.putArchiveContent("overviews/malformed-job.json", "{not valid json");

        fetcher.updateJobOverview();

        Object overviewObject = archiveStorage.getEntry("jobs/overview.json");
        String overviewContent = archiveStorage.readArchiveContent(overviewObject);
        MultipleJobsDetails overview =
                OBJECT_MAPPER.readValue(overviewContent, MultipleJobsDetails.class);

        assertThat(overview.getJobs()).hasSize(1);
        assertThat(overview.getJobs().iterator().next().getJobId()).isEqualTo(goodJob);
    }

    @TestTemplate
    void testUpdateJobOverviewDoesNotWipeGoodOverviewWhenAllEntriesAreMalformed() throws Exception {
        JobID job = JobID.generate();
        createJobArchive(remoteArchiveRootPath, job);

        HistoryServerArchiveFetcher<?> fetcher =
                createArchiveFetcher(remoteArchiveRootPath, true, archiveStorage);
        fetcher.fetchArchives();

        Object overviewObjectBefore = archiveStorage.getEntry("jobs/overview.json");
        String overviewContentBefore = archiveStorage.readArchiveContent(overviewObjectBefore);

        // corrupt the only per-job overview entry, simulating e.g. an incompatible archive
        // written by a different Flink version
        archiveStorage.putArchiveContent("overviews/" + job + ".json", "{not valid json anymore");

        fetcher.updateJobOverview();

        // the previously written combined overview must be preserved, not replaced by an empty
        // one
        Object overviewObjectAfter = archiveStorage.getEntry("jobs/overview.json");
        String overviewContentAfter = archiveStorage.readArchiveContent(overviewObjectAfter);
        assertThat(overviewContentAfter).isEqualTo(overviewContentBefore);
    }
}
