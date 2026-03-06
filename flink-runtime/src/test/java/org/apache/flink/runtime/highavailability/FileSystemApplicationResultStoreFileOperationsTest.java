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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.FlinkAssertions;
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

import static org.apache.flink.runtime.highavailability.ApplicationResultStoreContractTest.DUMMY_APPLICATION_RESULT_ENTRY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the internal {@link FileSystemApplicationResultStore} mechanisms. */
@ExtendWith(TestLoggerExtension.class)
public class FileSystemApplicationResultStoreFileOperationsTest {

    private static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

    private final ManuallyTriggeredScheduledExecutor manuallyTriggeredExecutor =
            new ManuallyTriggeredScheduledExecutor();

    private FileSystemApplicationResultStore fileSystemApplicationResultStore;

    @TempDir File temporaryFolder;

    private Path basePath;

    @BeforeEach
    public void setupTest() throws IOException {
        basePath = new Path(temporaryFolder.toURI());
        fileSystemApplicationResultStore =
                new FileSystemApplicationResultStore(
                        basePath.getFileSystem(), basePath, false, manuallyTriggeredExecutor);
    }

    @Test
    public void testValidEntryPathCreation() {
        final Path entryParent =
                fileSystemApplicationResultStore.constructEntryPath("random-name").getParent();
        assertThat(entryParent)
                .extracting(
                        FileSystemApplicationResultStoreFileOperationsTest::stripSucceedingSlash)
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
    public void testHasValidApplicationResultStoreEntryExtension() {
        assertThat(
                        FileSystemApplicationResultStore
                                .hasValidApplicationResultStoreEntryExtension(
                                        "test" + FileSystemApplicationResultStore.FILE_EXTENSION))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test.txt", "", "test.zip"})
    public void testHasInvalidApplicationResultStoreEntryExtension(String filename) {
        assertThat(
                        FileSystemApplicationResultStore
                                .hasValidApplicationResultStoreEntryExtension(filename))
                .isFalse();
    }

    @Test
    public void testHasValidDirtyApplicationResultStoreEntryExtension() {
        assertThat(
                        FileSystemApplicationResultStore
                                .hasValidDirtyApplicationResultStoreEntryExtension(
                                        "test"
                                                + FileSystemApplicationResultStore
                                                        .DIRTY_FILE_EXTENSION))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test.json", "test.txt", "", "test.zip"})
    public void testHasInvalidDirtyApplicationResultStoreEntryExtension(String filename) {
        assertThat(
                        FileSystemApplicationResultStore
                                .hasValidDirtyApplicationResultStoreEntryExtension(filename))
                .isFalse();
    }

    @Test
    public void testBaseDirectoryCreationOnResultStoreInitialization() throws Exception {
        final File emptyBaseDirectory = new File(temporaryFolder.getPath(), "empty-temp-dir");
        final Path basePath = new Path(emptyBaseDirectory.getPath());
        assertThat(emptyBaseDirectory).doesNotExist();

        fileSystemApplicationResultStore =
                new FileSystemApplicationResultStore(
                        basePath.getFileSystem(), basePath, false, manuallyTriggeredExecutor);
        // Result store operations are creating the base directory on-the-fly
        assertThat(emptyBaseDirectory).doesNotExist();
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        assertThat(emptyBaseDirectory).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(emptyBaseDirectory).exists().isDirectory();
    }

    @Test
    public void testStoreDirtyApplicationResultCreatesFile() throws Exception {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(getCleanResultIdsFromFileSystem()).isEmpty();
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY))
                .exists()
                .isFile()
                .isNotEmpty();
    }

    @Test
    public void testStoreCleanApplicationResultCreatesFile() throws Exception {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        CompletableFuture<Void> markCleanAsync =
                fileSystemApplicationResultStore.markResultAsCleanAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        assertThat(getCleanResultIdsFromFileSystem())
                .doesNotContain(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markCleanAsync).eventuallySucceeds();
        assertThat(getCleanResultIdsFromFileSystem())
                .containsExactlyInAnyOrder(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
    }

    @Test
    public void testStoreCleanApplicationResultDeletesDirtyFile() {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY))
                .exists()
                .isFile()
                .isNotEmpty();

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemApplicationResultStore.markResultAsCleanAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
    }

    @Test
    public void testCleanDirtyApplicationResultTwiceIsIdempotent() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        CompletableFuture<Void> cleanResultAsync =
                fileSystemApplicationResultStore.markResultAsCleanAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(cleanResultAsync).eventuallySucceeds();
        final byte[] cleanFileData =
                FileUtils.readAllBytes(expectedCleanFile(DUMMY_APPLICATION_RESULT_ENTRY).toPath());

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemApplicationResultStore.markResultAsCleanAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedCleanFile(DUMMY_APPLICATION_RESULT_ENTRY))
                .as(
                        "Marking the same application %s as clean should be idempotent.",
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId())
                .hasBinaryContent(cleanFileData);
    }

    /**
     * Tests that, when the application result store is configured to delete on commit, both the
     * clean and the dirty files for an application entry are deleted when the result is marked as
     * clean.
     */
    @Test
    public void testDeleteOnCommit() throws IOException {
        Path path = new Path(temporaryFolder.toURI());
        fileSystemApplicationResultStore =
                new FileSystemApplicationResultStore(
                        path.getFileSystem(), path, true, manuallyTriggeredExecutor);

        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY))
                .exists()
                .isFile()
                .isNotEmpty();

        CompletableFuture<Void> markResultAsCleanAsync =
                fileSystemApplicationResultStore.markResultAsCleanAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(markResultAsCleanAsync).eventuallySucceeds();
        assertThat(expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
        assertThat(expectedCleanFile(DUMMY_APPLICATION_RESULT_ENTRY)).doesNotExist();
    }

    @Test
    public void testVersionSerialization() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        final File dirtyFile = expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY);
        final FileSystemApplicationResultStore.JsonApplicationResultEntry deserializedEntry =
                MAPPER.readValue(
                        dirtyFile,
                        FileSystemApplicationResultStore.JsonApplicationResultEntry.class);
        assertThat(dirtyFile).isFile().content().containsPattern("\"version\":1");
        assertThat(deserializedEntry.getVersion()).isEqualTo(1);
    }

    @Test
    public void testApplicationResultSerializationDeserialization() throws IOException {
        CompletableFuture<Void> dirtyResultAsync =
                fileSystemApplicationResultStore.createDirtyResultAsync(
                        DUMMY_APPLICATION_RESULT_ENTRY);
        manuallyTriggeredExecutor.triggerAll();
        FlinkAssertions.assertThatFuture(dirtyResultAsync).eventuallySucceeds();
        final File dirtyFile = expectedDirtyFile(DUMMY_APPLICATION_RESULT_ENTRY);
        final FileSystemApplicationResultStore.JsonApplicationResultEntry deserializedEntry =
                MAPPER.readValue(
                        dirtyFile,
                        FileSystemApplicationResultStore.JsonApplicationResultEntry.class);
        final ApplicationResult deserializedApplicationResult =
                deserializedEntry.getApplicationResult();
        assertThat(deserializedApplicationResult)
                .extracting(ApplicationResult::getApplicationId)
                .isEqualTo(DUMMY_APPLICATION_RESULT_ENTRY.getApplicationId());
        assertThat(deserializedApplicationResult)
                .extracting(ApplicationResult::getApplicationState)
                .isEqualTo(
                        DUMMY_APPLICATION_RESULT_ENTRY
                                .getApplicationResult()
                                .getApplicationState());
    }

    private List<ApplicationID> getCleanResultIdsFromFileSystem() throws IOException {
        final List<ApplicationID> cleanResults = new ArrayList<>();

        final File[] cleanFiles =
                temporaryFolder.listFiles(
                        (dir, name) ->
                                !FileSystemApplicationResultStore
                                        .hasValidDirtyApplicationResultStoreEntryExtension(name));
        assert cleanFiles != null;
        for (File cleanFile : cleanFiles) {
            final FileSystemApplicationResultStore.JsonApplicationResultEntry entry =
                    MAPPER.readValue(
                            cleanFile,
                            FileSystemApplicationResultStore.JsonApplicationResultEntry.class);
            cleanResults.add(entry.getApplicationResult().getApplicationId());
        }

        return cleanResults;
    }

    /**
     * Generates the expected path for a dirty entry given an application entry.
     *
     * @param entry The application ID to construct the expected dirty path from.
     * @return The expected dirty file.
     */
    private File expectedDirtyFile(ApplicationResultEntry entry) {
        return new File(
                temporaryFolder.toURI().getPath(),
                entry.getApplicationId().toString()
                        + FileSystemApplicationResultStore.DIRTY_FILE_EXTENSION);
    }

    /**
     * Generates the expected path for a clean entry given an application entry.
     *
     * @param entry The application entry to construct the expected clean path from.
     * @return The expected clean file.
     */
    private File expectedCleanFile(ApplicationResultEntry entry) {
        return new File(
                temporaryFolder.toURI().getPath(),
                entry.getApplicationId().toString()
                        + FileSystemApplicationResultStore.FILE_EXTENSION);
    }
}
