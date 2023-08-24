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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.highavailability.JobResultStoreContractTest.DUMMY_JOB_RESULT_ENTRY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the internal {@link FileSystemJobResultStore} mechanisms. */
@ExtendWith(TestLoggerExtension.class)
public class FileSystemJobResultStoreFileOperationsTest {

    private static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

    private final ManuallyTriggeredScheduledExecutor manuallyTriggeredExecutor =
            new ManuallyTriggeredScheduledExecutor();

    private FileSystemJobResultStore fileSystemJobResultStore;

    @TempDir File temporaryFolder;

    private Path basePath;

    @BeforeEach
    public void setupTest() throws IOException {
        basePath = new Path(temporaryFolder.toURI());
        fileSystemJobResultStore =
                new FileSystemJobResultStore(
                        basePath.getFileSystem(), basePath, false, manuallyTriggeredExecutor);
    }

    @Test
    public void testValidEntryPathCreation() {
        final Path entryParent =
                fileSystemJobResultStore.constructEntryPath("random-name").getParent();
        assertThat(entryParent)
                .extracting(FileSystemJobResultStoreFileOperationsTest::stripSucceedingSlash)
                .isEqualTo(stripSucceedingSlash(basePath));
    }

    private static String stripSucceedingSlash(Path path) {
        final String uriStr = path.toUri().toString();
        if (uriStr.charAt(uriStr.length() - 1) == '/') {
            return uriStr.substring(0, uriStr.length() - 1);
        }

        return uriStr;
    }

    @Test
    public void testHasValidJobResultStoreEntryExtension() {
        assertThat(
                        FileSystemJobResultStore.hasValidJobResultStoreEntryExtension(
                                "test" + FileSystemJobResultStore.FILE_EXTENSION))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test.txt", "", "test.zip"})
    public void testHasInvalidJobResultStoreEntryExtension(String filename) {
        assertThat(FileSystemJobResultStore.hasValidJobResultStoreEntryExtension(filename))
                .isFalse();
    }

    @Test
    public void testHasValidDirtyJobResultStoreEntryExtension() {
        assertThat(
                        FileSystemJobResultStore.hasValidDirtyJobResultStoreEntryExtension(
                                "test" + FileSystemJobResultStore.DIRTY_FILE_EXTENSION))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test.json", "test.txt", "", "test.zip"})
    public void testHasInvalidDirtyJobResultStoreEntryExtension(String filename) {
        assertThat(FileSystemJobResultStore.hasValidDirtyJobResultStoreEntryExtension(filename))
                .isFalse();
    }

    @Test
    public void testBaseDirectoryCreationOnResultStoreInitialization() throws Exception {
        final File emptyBaseDirectory = new File(temporaryFolder.getPath(), "empty-temp-dir");
        final Path basePath = new Path(emptyBaseDirectory.getPath());
        assertThat(emptyBaseDirectory).doesNotExist();

        fileSystemJobResultStore =
                new FileSystemJobResultStore(
                        basePath.getFileSystem(), basePath, false, manuallyTriggeredExecutor);
        // Result store operations are creating the base directory on-the-fly
        assertThat(emptyBaseDirectory).doesNotExist();
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        assertThat(emptyBaseDirectory).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(emptyBaseDirectory).exists().isDirectory();
    }

    @Test
    public void testStoreDirtyJobResultCreatesFile() throws Exception {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(getCleanResultIdsFromFileSystem()).isEmpty();
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).exists().isFile().isNotEmpty();
    }

    @Test
    public void testStoreCleanJobResultCreatesFile() throws Exception {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        CompletableFuture<Void> markCleanAsync =
                fileSystemJobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThat(getCleanResultIdsFromFileSystem())
                .doesNotContain(DUMMY_JOB_RESULT_ENTRY.getJobId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markCleanAsync).eventuallySucceeds();
        assertThat(getCleanResultIdsFromFileSystem())
                .containsExactlyInAnyOrder(DUMMY_JOB_RESULT_ENTRY.getJobId());
    }

    @Test
    public void testStoreCleanJobResultDeletesDirtyFile() {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).exists().isFile().isNotEmpty();

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemJobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
    }

    @Test
    public void testCleanDirtyJobResultTwiceIsIdempotent() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        CompletableFuture<Void> cleanResultAsync =
                fileSystemJobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(cleanResultAsync).eventuallySucceeds();
        final byte[] cleanFileData =
                FileUtils.readAllBytes(expectedCleanFile(DUMMY_JOB_RESULT_ENTRY).toPath());

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemJobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedCleanFile(DUMMY_JOB_RESULT_ENTRY))
                .as(
                        "Marking the same job %s as clean should be idempotent.",
                        DUMMY_JOB_RESULT_ENTRY.getJobId())
                .hasBinaryContent(cleanFileData);
    }

    /**
     * Tests that, when the job result store is configured to delete on commit, both the clean and
     * the dirty files for a job entry are deleted when the result is marked as clean.
     */
    @Test
    public void testDeleteOnCommit() throws IOException {
        Path path = new Path(temporaryFolder.toURI());
        fileSystemJobResultStore =
                new FileSystemJobResultStore(
                        path.getFileSystem(), path, true, manuallyTriggeredExecutor);

        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).exists().isFile().isNotEmpty();

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemJobResultStore.markResultAsCleanAsync(DUMMY_JOB_RESULT_ENTRY.getJobId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
        assertThat(expectedCleanFile(DUMMY_JOB_RESULT_ENTRY)).doesNotExist();
    }

    @Test
    public void testVersionSerialization() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        final File dirtyFile = expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY);
        final FileSystemJobResultStore.JsonJobResultEntry deserializedEntry =
                MAPPER.readValue(dirtyFile, FileSystemJobResultStore.JsonJobResultEntry.class);
        assertThat(dirtyFile).isFile().content().containsPattern("\"version\":1");
        assertThat(deserializedEntry.getVersion()).isEqualTo(1);
    }

    @Test
    public void testJobResultSerializationDeserialization() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemJobResultStore.createDirtyResultAsync(DUMMY_JOB_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        final File dirtyFile = expectedDirtyFile(DUMMY_JOB_RESULT_ENTRY);
        final FileSystemJobResultStore.JsonJobResultEntry deserializedEntry =
                MAPPER.readValue(dirtyFile, FileSystemJobResultStore.JsonJobResultEntry.class);
        final JobResult deserializedJobResult = deserializedEntry.getJobResult();
        assertThat(deserializedJobResult)
                .extracting(JobResult::getJobId)
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobId());
        assertThat(deserializedJobResult)
                .extracting(JobResult::getApplicationStatus)
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobResult().getApplicationStatus());
        assertThat(deserializedJobResult)
                .extracting(JobResult::getNetRuntime)
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobResult().getNetRuntime());
        assertThat(deserializedJobResult)
                .extracting(JobResult::getSerializedThrowable)
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobResult().getSerializedThrowable());
        assertThat(deserializedJobResult)
                .extracting(JobResult::getAccumulatorResults)
                .isEqualTo(DUMMY_JOB_RESULT_ENTRY.getJobResult().getAccumulatorResults());
    }

    private List<JobID> getCleanResultIdsFromFileSystem() throws IOException {
        final List<JobID> cleanResults = new ArrayList<>();

        final File[] cleanFiles =
                temporaryFolder.listFiles(
                        (dir, name) ->
                                !FileSystemJobResultStore.hasValidDirtyJobResultStoreEntryExtension(
                                        name));
        assert cleanFiles != null;
        for (File cleanFile : cleanFiles) {
            final FileSystemJobResultStore.JsonJobResultEntry entry =
                    MAPPER.readValue(cleanFile, FileSystemJobResultStore.JsonJobResultEntry.class);
            cleanResults.add(entry.getJobResult().getJobId());
        }

        return cleanResults;
    }

    /**
     * Generates the expected path for a dirty entry given a job entry.
     *
     * @param entry The job ID to construct the expected dirty path from.
     * @return The expected dirty file.
     */
    private File expectedDirtyFile(JobResultEntry entry) {
        return new File(
                temporaryFolder.toURI().getPath(),
                entry.getJobId().toString() + FileSystemJobResultStore.DIRTY_FILE_EXTENSION);
    }

    /**
     * Generates the expected path for a clean entry given a job entry.
     *
     * @param entry The job entry to construct the expected clean path from.
     * @return The expected clean file.
     */
    private File expectedCleanFile(JobResultEntry entry) {
        return new File(
                temporaryFolder.toURI().getPath(),
                entry.getJobId().toString() + FileSystemJobResultStore.FILE_EXTENSION);
    }
}
