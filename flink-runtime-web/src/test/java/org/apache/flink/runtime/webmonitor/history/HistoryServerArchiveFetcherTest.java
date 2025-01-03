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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the HistoryServerArchiveFetcher. */
public class HistoryServerArchiveFetcherTest {

    @Test
    void testMostRecentlyViewedCacheEvictedWhenFull(@TempDir Path tmpDir)
            throws IOException, InterruptedException {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        int numJobs = 5;
        int localCacheSize = 3;
        int mostRecentlyViewedCacheSize = 1;

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs, webDir.toFile(), (event) -> {}, true, -1);

        for (int i = 0; i < numJobs; i++) {
            String jobID = JobID.generate().toString();
            buildRemoteStorageJobDirectory(tempDir, jobID);
        }

        Map<Integer, String> jobIdByCreation =
                retrieveRemoteArchivesByModificationTime(refreshFS, tempDirFs);

        // Fetching the remote archives and storing them in the local cache asynchronously
        archiveFetcher.fetchArchives();

        // Confirming that the size of the local cache is based on the asynchronous fetch cache
        // limit
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(localCacheSize);
        for (int i = 0; i < localCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Validating that the job has been fetched from the remote job archive and that the
        // cache is still within the right size of localCacheSize + remoteFetchCacheSize;
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(3))).isTrue();
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(localCacheSize + mostRecentlyViewedCacheSize);

        // Forcing a background refresh to evict the previously remotely fetched job
        archiveFetcher.fetchArchives();

        // Validating that the previously remotely fetched job has been evicted and the new
        // job has taken its place
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(4))).isTrue();
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(3))).isFalse();
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(localCacheSize + mostRecentlyViewedCacheSize);
    }

    @Test
    void testGeneralCacheEviction(@TempDir Path tmpDir) throws IOException, InterruptedException {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        int numJobs = 5;
        int generalCacheSize = 3;
        int mostRecentlyViewedCacheSize = 1;

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs, webDir.toFile(), (event) -> {}, true, -1);

        for (int i = 0; i < numJobs; i++) {
            String jobID = JobID.generate().toString();
            buildRemoteStorageJobDirectory(tempDir, jobID);
        }

        Map<Integer, String> jobIdByCreation =
                retrieveRemoteArchivesByModificationTime(refreshFS, tempDirFs);

        // Fetching the remote archives and storing them in the local cache
        archiveFetcher.fetchArchives();

        // Confirming that the size of the local cache is based on the asynchronous fetch cache
        // limit
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize);
        for (int i = 0; i < generalCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Adding another remote directory to indicate a newer job, we sleep for a short period
        // of time to ensure a more recent timestamp
        Thread.sleep(1000);
        String latestJobID = JobID.generate().toString();
        buildRemoteStorageJobDirectory(tempDir, latestJobID);

        archiveFetcher.fetchArchives();

        // Validating that the cache has not exceeded the async refresh cache limit and that
        // the new job is cached
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize);
        assertThat(findJobIdInGeneralCache(webDir, latestJobID)).isTrue();

        // Validating that the oldest record was evicted from the cache
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(2))).isFalse();
    }

    @Test
    void testMostRecentlyViewedCacheNotEvictedByPeriodicFetch(@TempDir Path tmpDir)
            throws IOException, InterruptedException {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        int numJobs = 5;
        int generalCacheSize = 3;
        int mostRecentlyViewedCacheSize = 1;

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs, webDir.toFile(), (event) -> {}, true, -1);

        for (int i = 0; i < numJobs; i++) {
            String jobID = JobID.generate().toString();
            buildRemoteStorageJobDirectory(tempDir, jobID);
        }

        Map<Integer, String> jobIdByCreation =
                retrieveRemoteArchivesByModificationTime(refreshFS, tempDirFs);

        // Fetching the remote archives and storing them in the local cache
        archiveFetcher.fetchArchives();

        // Confirming that the size of the local cache is based on the asynchronous fetch cache
        // limit
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize);
        for (int i = 0; i < generalCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Adding another remote directory to indicate a newer job, we sleep for a short period
        // of time to ensure a more recent timestamp
        Thread.sleep(1000);
        String latestJobID = JobID.generate().toString();
        buildRemoteStorageJobDirectory(tempDir, latestJobID);

        // triggering an asynchronous fetch to update the cache
        archiveFetcher.fetchArchives();

        // Validating that the cache has not exceeded the async refresh cache limit and that
        // the new job is cached and that the most recently viewed job is still present
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(generalCacheSize + mostRecentlyViewedCacheSize);
        assertThat(findJobIdInGeneralCache(webDir, latestJobID)).isTrue();
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(3))).isTrue();

        // Validating that the oldest record was evicted from the cache
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(2))).isFalse();
    }

    @Test
    void testGeneralAndMostRecentlyViewedCacheEviction(@TempDir Path tmpDir)
            throws IOException, InterruptedException {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        int numJobs = 5;
        int generalCacheSize = 3;
        int mostRecentlyViewedCacheSize = 1;

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs, webDir.toFile(), (event) -> {}, true, -1);

        for (int i = 0; i < numJobs; i++) {
            String jobID = JobID.generate().toString();
            buildRemoteStorageJobDirectory(tempDir, jobID);
        }

        Map<Integer, String> jobIdByCreation =
                retrieveRemoteArchivesByModificationTime(refreshFS, tempDirFs);

        // Fetching the remote archives and store them in the local cache
        archiveFetcher.fetchArchives();

        // Confirming that the size of the local cache is based on the asynchronous fetch cache
        // limit
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize);
        for (int i = 0; i < generalCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Validating that the cache size contains both the asynchronous and remote user fetch
        // records
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(generalCacheSize + mostRecentlyViewedCacheSize);
        for (int i = 0; i < generalCacheSize + mostRecentlyViewedCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Adding another remote directory to indicate a newer job, we sleep for a short period
        // of time to ensure a more recent timestamp
        Thread.sleep(1000);
        String latestJobID = JobID.generate().toString();
        buildRemoteStorageJobDirectory(tempDir, latestJobID);

        archiveFetcher.fetchArchives();

        // Validating that the correct remotely fetched job has been evicted
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(4))).isTrue();
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(3))).isFalse();

        // Validating that the cache does not exceed the total cache limit
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(generalCacheSize + mostRecentlyViewedCacheSize);
        assertThat(findJobIdInGeneralCache(webDir, latestJobID)).isTrue();

        // Validating that the oldest record which was not remotely fetched was evicted from the
        // cache
        assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(2))).isFalse();
    }

    @Test
    void testMostRecentlyViewedCacheUpdatesBasedOnFetchingByIdFromLocalCache(@TempDir Path tmpDir)
            throws IOException, InterruptedException {
        final Path webDir = Files.createDirectory(tmpDir.resolve("webDir"));
        final Path tempDir = Files.createDirectory(tmpDir.resolve("tmpDir"));
        final org.apache.flink.core.fs.Path tempDirFs =
                new org.apache.flink.core.fs.Path(tempDir.toUri());

        List<HistoryServer.RefreshLocation> refreshDirs = new ArrayList<>();
        FileSystem refreshFS = tempDirFs.getFileSystem();
        refreshDirs.add(new HistoryServer.RefreshLocation(tempDirFs, refreshFS));

        int numJobs = 5;
        int generalCacheSize = 3;
        int mostRecentlyViewedCacheSize = 2;

        HistoryServerArchiveFetcher archiveFetcher =
                new HistoryServerArchiveFetcher(
                        refreshDirs, webDir.toFile(), (event) -> {}, true, -1);

        for (int i = 0; i < numJobs; i++) {
            String jobID = JobID.generate().toString();
            buildRemoteStorageJobDirectory(tempDir, jobID);
        }

        Map<Integer, String> jobIdByCreation =
                retrieveRemoteArchivesByModificationTime(refreshFS, tempDirFs);

        // Fetching the remote archives and store them in the local cache
        archiveFetcher.fetchArchives();

        // Confirming that the size of the local cache is based on the asynchronous fetch cache
        // limit
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize);
        for (int i = 0; i < generalCacheSize; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Validating that the cache size contains both the asynchronous and remote user fetch
        // records
        assertThat(getNumJobsInGeneralCache(webDir)).isEqualTo(generalCacheSize + 1);
        for (int i = 0; i < generalCacheSize + 1; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }

        // Adding another remote directory to indicate a newer job, we sleep for a short period
        // of time to ensure a more recent timestamp
        Thread.sleep(1000);
        String latestJobID = JobID.generate().toString();
        buildRemoteStorageJobDirectory(tempDir, latestJobID);

        // Doing asynchronous to retrieve the latest job. Because the job that would have
        // otherwise would have been evicted was fetched via a user request, it should not
        // be evicted from the archives
        archiveFetcher.fetchArchives();

        // Validating that the cache does not exceed the total cache limit and that the
        // correct jobs are stored in the cache
        assertThat(getNumJobsInGeneralCache(webDir))
                .isEqualTo(generalCacheSize + mostRecentlyViewedCacheSize);
        assertThat(findJobIdInGeneralCache(webDir, latestJobID)).isTrue();
        for (int i = 0; i < generalCacheSize + 1; i++) {
            assertThat(findJobIdInGeneralCache(webDir, jobIdByCreation.get(i))).isTrue();
        }
    }

    private int getNumJobsInGeneralCache(Path cacheDir) {
        File f = new File(cacheDir.toFile(), "/jobs");
        return f.listFiles().length - 1;
    }

    private boolean findJobIdInGeneralCache(Path cacheDir, String jobID) throws IOException {
        File jobPath = new File(cacheDir.toFile(), "/jobs/" + jobID + ".json");
        return jobPath.exists();
    }

    private void buildRemoteStorageJobDirectory(Path refreshDir, String jobID) throws IOException {
        String json =
                "{\n"
                        + "   \"archive\": [\n"
                        + "      {\n"
                        + "         \"path\": \"/jobs/"
                        + jobID
                        + "\",\n"
                        + "         \"json\": \"{\\\"jid\\\":\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"WordCount\\\",\\\"isStoppable\\\":false,"
                        + "\\\"state\\\":\\\"FINISHED\\\",\\\"start-time\\\":1705527079656,\\\"end-time\\\":1705527080059,"
                        + "\\\"duration\\\":403,\\\"maxParallelism\\\":-1,\\\"now\\\":1705527080104,\\\"timestamps\\\":{\\\"FAILED\\\":"
                        + "0,\\\"FINISHED\\\":1705527080059,\\\"CANCELLING\\\":0,\\\"CANCELED\\\":0,\\\"INITIALIZING\\\":"
                        + "1705527079656,\\\"CREATED\\\":1705527079708,\\\"RUNNING\\\":1705527079763,\\\"RESTARTING\\\":"
                        + "0,\\\"SUSPENDED\\\":0,\\\"FAILING\\\":0,\\\"RECONCILING\\\":0},\\\"vertices\\\":[{\\\"id\\\":"
                        + "\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"name\\\":\\\"Source: in-memory-input -> tokenizer\\\","
                        + "\\\"maxParallelism\\\":128,\\\"parallelism\\\":1,\\\"status\\\":\\\"FINISHED\\\","
                        + "\\\"start-time\\\":1705527079881,\\\"end-time\\\":1705527080046,\\\"duration\\\":165,"
                        + "\\\"tasks\\\":{\\\"CANCELED\\\":0,\\\"FAILED\\\":0,\\\"FINISHED\\\":1,\\\"DEPLOYING\\\":"
                        + "0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":0,\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,"
                        + "\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"metrics\\\":{\\\"read-bytes\\\":0,"
                        + "\\\"read-bytes-complete\\\":true,\\\"write-bytes\\\":4047,\\\"write-bytes-complete\\\":true,"
                        + "\\\"read-records\\\":0,\\\"read-records-complete\\\":true,\\\"write-records\\\":287,"
                        + "\\\"write-records-complete\\\":true,\\\"accumulated-backpressured-time\\\":0,"
                        + "\\\"accumulated-idle-time\\\":0,\\\"accumulated-busy-time\\\":\\\"NaN\\\"}},{\\\"id\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"counter -> Sink: print-sink\\\",\\\"maxParallelism\\\":"
                        + "128,\\\"parallelism\\\":1,\\\"status\\\":\\\"FINISHED\\\",\\\"start-time\\\":"
                        + "1705527079885,\\\"end-time\\\":1705527080057,\\\"duration\\\":172,\\\"tasks\\\":{\\\"CANCELED\\\":"
                        + "0,\\\"FAILED\\\":0,\\\"FINISHED\\\":1,\\\"DEPLOYING\\\":0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":"
                        + "0,\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"metrics\\\":"
                        + "{\\\"read-bytes\\\":4060,\\\"read-bytes-complete\\\":true,\\\"write-bytes\\\":0,"
                        + "\\\"write-bytes-complete\\\":true,\\\"read-records\\\":287,\\\"read-records-complete\\\":true,"
                        + "\\\"write-records\\\":0,\\\"write-records-complete\\\":true,\\\"accumulated-backpressured-time\\\":"
                        + "0,\\\"accumulated-idle-time\\\":1,\\\"accumulated-busy-time\\\":15.0}}],\\\"status-counts\\\":{\\\"CANCELED\\\":"
                        + "0,\\\"FAILED\\\":0,\\\"FINISHED\\\":2,\\\"DEPLOYING\\\":0,\\\"RUNNING\\\":0,\\\"INITIALIZING\\\":0,"
                        + "\\\"SCHEDULED\\\":0,\\\"CANCELING\\\":0,\\\"RECONCILING\\\":0,\\\"CREATED\\\":0},\\\"plan\\\":{\\\"jid\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"name\\\":\\\"WordCount\\\",\\\"type\\\":\\\"STREAMING\\\",\\\"nodes\\\":[{\\\"id\\\":"
                        + "\\\""
                        + jobID
                        + "\\\",\\\"parallelism\\\":1,\\\"operator\\\":\\\"\\\",\\\"operator_strategy\\\":\\\"\\\","
                        + "\\\"description\\\":\\\"counter<br/>+- Sink: print-sink<br/>\\\",\\\"inputs\\\":[{\\\"num\\\":0,"
                        + "\\\"id\\\":\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"ship_strategy\\\":\\\"HASH\\\","
                        + "\\\"exchange\\\":\\\"pipelined_bounded\\\"}],\\\"optimizer_properties\\\":{}},{\\\"id\\\":"
                        + "\\\"cbc357ccb763df2852fee8c4fc7d55f2\\\",\\\"parallelism\\\":1,\\\"operator\\\":\\\"\\\","
                        + "\\\"operator_strategy\\\":\\\"\\\",\\\"description\\\":\\\"Source: in-memory-input<br/>+- tokenizer"
                        + "<br/>\\\",\\\"optimizer_properties\\\":{}}]}\"\n"
                        + "      }\n"
                        + "   ]\n"
                        + "}";
        Path remoteJobArchive = Files.createFile(refreshDir.resolve(jobID));
        remoteJobArchive.toFile().setLastModified(System.currentTimeMillis());
        FileUtils.writeStringToFile(remoteJobArchive.toFile(), json);
    }

    private Map<Integer, String> retrieveRemoteArchivesByModificationTime(
            FileSystem refreshFS, org.apache.flink.core.fs.Path tempDirFs) throws IOException {
        FileStatus[] remoteFiles = ArchiveFetcher.listArchives(refreshFS, tempDirFs);

        return IntStream.range(0, remoteFiles.length)
                .boxed()
                .collect(
                        Collectors.toMap(
                                i -> i,
                                i -> remoteFiles[i].getPath().getName(),
                                (a, b) -> b,
                                LinkedHashMap::new));
    }
}
