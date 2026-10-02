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

package org.apache.flink.fs.azurefs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AzureDataLakeFileSystem}. */
class AzureDataLakeFileSystemTest {

    private static final int DEFAULT_BUFFER_SIZE = 256 * 1024;
    private static final long DEFAULT_WRITE_REQUEST_SIZE = 8 * 1024 * 1024L;

    private static final URI TEST_URI =
            URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net");

    private static final AzureDataLakeFileSystem DEFAULT_FS =
            createTestFileSystem(new TestingDataLakeStorageOperations());

    private static AzureDataLakeFileSystem createTestFileSystem(
            final TestingDataLakeStorageOperations storageOperations) {
        return AzureDataLakeFileSystem.builder(storageOperations, TEST_URI)
                .readBufferSize(DEFAULT_BUFFER_SIZE)
                .writeRequestSize(DEFAULT_WRITE_REQUEST_SIZE)
                .build();
    }

    // --- getUri ---

    @Test
    void shouldReturnUri() {
        assertThat(DEFAULT_FS.getUri()).isEqualTo(TEST_URI);
    }

    // --- getWorkingDirectory ---

    @Test
    void shouldReturnWorkingDirectory() {
        assertThat(DEFAULT_FS.getWorkingDirectory()).isEqualTo(new Path(TEST_URI));
    }

    // --- getHomeDirectory ---

    @Test
    void shouldReturnHomeDirectory() {
        assertThat(DEFAULT_FS.getHomeDirectory()).isEqualTo(new Path(TEST_URI));
    }

    // --- isDistributedFS ---

    @Test
    void shouldBeDistributedFileSystem() {
        assertThat(DEFAULT_FS.isDistributedFS()).isTrue();
    }

    // --- createRecoverableWriter ---

    @Test
    void shouldThrowOnCreateRecoverableWriter() {
        assertThatThrownBy(DEFAULT_FS::createRecoverableWriter)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Recoverable writer is not supported");
    }

    // --- getFileBlockLocations ---

    @Test
    void shouldReturnSingleBlockLocation() {
        final long fileSize = 1024L;
        final FileStatus status =
                AzureDataLakeFileStatus.forFile(
                        new Path(TEST_URI + "/test.txt"), fileSize, fileSize, 0L);

        final BlockLocation[] locations = DEFAULT_FS.getFileBlockLocations(status, 0, fileSize);

        assertThat(locations).hasSize(1);
        assertThat(locations[0].getLength()).isEqualTo(fileSize);
        assertThat(locations[0].getOffset()).isZero();
    }

    // --- toRelativePathString ---

    static Stream<Arguments> toRelativePathStringCases() {
        return Stream.of(
                Arguments.of(
                        new Path("abfss://c@a.dfs.core.windows.net/dir/file.txt"), "dir/file.txt"),
                Arguments.of(new Path("abfss://c@a.dfs.core.windows.net/file.txt"), "file.txt"),
                Arguments.of(new Path("abfss://c@a.dfs.core.windows.net/"), ""),
                Arguments.of(
                        new Path("abfss://c@a.dfs.core.windows.net/a/b/c/d.parquet"),
                        "a/b/c/d.parquet"),
                Arguments.of(new Path("/some/path"), "some/path"),
                Arguments.of(new Path("relative/path"), "relative/path"));
    }

    @ParameterizedTest
    @MethodSource("toRelativePathStringCases")
    void shouldConvertToRelativePathString(final Path input, final String expected) {
        assertThat(AzureDataLakeFileSystem.toRelativePathString(input)).isEqualTo(expected);
    }

    // --- getFileSystemNameFromUri ---

    @Test
    void shouldExtractFileSystemNameFromUserInfo() {
        final URI uri = URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net/path");
        assertThat(AzureDataLakeFileSystem.getFileSystemNameFromUri(uri)).isEqualTo("mycontainer");
    }

    @Test
    void shouldThrowWhenNoUserInfoInUri() {
        final URI uri = URI.create("abfss://myaccount.dfs.core.windows.net/path");
        assertThatThrownBy(() -> AzureDataLakeFileSystem.getFileSystemNameFromUri(uri))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("container name");
    }

    // --- Constructor validation ---

    @ParameterizedTest
    @MethodSource("invalidConstructorArgs")
    void shouldRejectInvalidConstructorArgs(
            final DataLakeStorageOperations ops,
            final URI uri,
            final Class<? extends Exception> expectedExceptionType) {
        assertThatThrownBy(
                        () ->
                                AzureDataLakeFileSystem.builder(ops, uri)
                                        .readBufferSize(DEFAULT_BUFFER_SIZE)
                                        .writeRequestSize(DEFAULT_WRITE_REQUEST_SIZE)
                                        .build())
                .isInstanceOf(expectedExceptionType);
    }

    private static Stream<Arguments> invalidConstructorArgs() {
        return Stream.of(
                // null storage operations
                Arguments.of(null, TEST_URI, NullPointerException.class),
                // null URI
                Arguments.of(
                        new TestingDataLakeStorageOperations(), null, NullPointerException.class),
                // URI without container
                Arguments.of(
                        new TestingDataLakeStorageOperations(),
                        URI.create("abfss://myaccount.dfs.core.windows.net/path"),
                        IllegalArgumentException.class));
    }

    // --- getFileStatus for root ---

    @Test
    void shouldReturnDirectoryStatusForRootPath() throws Exception {
        final Path rootPath = new Path("abfss://mycontainer@myaccount.dfs.core.windows.net/");

        final FileStatus status = DEFAULT_FS.getFileStatus(rootPath);

        assertThat(status.isDir()).isTrue();
        assertThat(status.getLen()).isZero();
    }

    // --- getFileStatus (non-root paths) ---

    @Test
    void shouldReturnFileStatusForExistingFile() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final long fileSize = 1024L;
        final OffsetDateTime modTime = OffsetDateTime.now();
        delegate.addFile("dir/file.txt", new byte[(int) fileSize], modTime);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/dir/file.txt");

        final FileStatus status = fs.getFileStatus(path);

        assertThat(status.isDir()).isFalse();
        assertThat(status.getLen()).isEqualTo(fileSize);
        assertThat(status.getPath()).isEqualTo(path);
        assertThat(status.getModificationTime()).isEqualTo(modTime.toInstant().toEpochMilli());
    }

    @Test
    void shouldReturnDirectoryStatusForExistingDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime modTime = OffsetDateTime.now();
        delegate.addDirectory("mydir", modTime);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/mydir");

        final FileStatus status = fs.getFileStatus(path);

        assertThat(status.isDir()).isTrue();
        assertThat(status.getLen()).isZero();
        assertThat(status.getPath()).isEqualTo(path);
        assertThat(status.getModificationTime()).isEqualTo(modTime.toInstant().toEpochMilli());
    }

    @Test
    void shouldThrowFileNotFoundExceptionWhenPathDoesNotExist() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/nonexistent");

        assertThatThrownBy(() -> fs.getFileStatus(path))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("File not found");
    }

    @Test
    void shouldThrowIOExceptionWhenAzureReturnsOtherError() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectError("somefile", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/somefile");

        assertThatThrownBy(() -> fs.getFileStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to get file status");
    }

    @Test
    void shouldThrowIOExceptionWhenLastModifiedIsNull() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("nulltime", new byte[10], null);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/nulltime");

        assertThatThrownBy(() -> fs.getFileStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("null modification time");
    }

    // --- open ---

    @Test
    void shouldOpenFileForReadingAndReturnNonNullStream() throws Exception {
        // Verifies that open() succeeds for a registered file and returns a non-null stream.
        // Stream data content is validated at the ObjectStorageInputStreamBase level.
        final byte[] content = {1, 2, 3, 4, 5};
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("data.bin", content, OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/data.bin");

        try (FSDataInputStream stream = fs.open(path)) {
            assertThat(stream).isNotNull();
            // Stream is lazily initialized; getPos() verifies it is in a valid initial state.
            assertThat(stream.getPos()).isEqualTo(0L);
        }
    }

    @Test
    void shouldThrowWhenOpeningDirectory() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addDirectory("mydir", OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/mydir");

        assertThatThrownBy(() -> fs.open(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot open directory");
    }

    @Test
    void shouldThrowFileNotFoundExceptionWhenOpeningNonexistentFile() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/nonexistent");

        assertThatThrownBy(() -> fs.open(path)).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void shouldThrowIOExceptionOnOtherAzureErrorDuringOpen() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectError("failfile", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/failfile");

        assertThatThrownBy(() -> fs.open(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to open file");
    }

    // --- create ---

    @Test
    void shouldCreateFileAndReturnOutputStream() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/newfile.bin");

        try (FSDataOutputStream stream = fs.create(path, WriteMode.OVERWRITE)) {
            assertThat(stream).isNotNull();
            assertThat(stream.getPos()).isEqualTo(0L);
        }
    }

    /**
     * Error-path cases for {@link AzureDataLakeFileSystem#create}. The key distinction: 409/412 map
     * to "File already exists" only with {@link WriteMode#NO_OVERWRITE}; with {@link
     * WriteMode#OVERWRITE} the same codes fall through to the generic error path.
     */
    static Stream<Arguments> createErrorCases() {
        return Stream.of(
                // NO_OVERWRITE + conflict codes → "File already exists"
                Arguments.of(WriteMode.NO_OVERWRITE, 409, "File already exists"),
                Arguments.of(WriteMode.NO_OVERWRITE, 412, "File already exists"),
                // NO_OVERWRITE + non-conflict codes → generic error
                Arguments.of(WriteMode.NO_OVERWRITE, 500, "Failed to create file"),
                // OVERWRITE + any error → generic "Failed to create file"
                Arguments.of(WriteMode.OVERWRITE, 409, "Failed to create file"),
                Arguments.of(WriteMode.OVERWRITE, 500, "Failed to create file"));
    }

    @ParameterizedTest
    @MethodSource("createErrorCases")
    void shouldMapCreateErrorToCorrectIOException(
            final WriteMode writeMode, final int statusCode, final String expectedMessage) {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectCreateError("file.bin", statusCode);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/file.bin");

        assertThatThrownBy(() -> fs.create(path, writeMode))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(expectedMessage);
    }

    // --- listStatus ---

    @Test
    void shouldListDirectoryContents() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addDirectory("dir", now);
        delegate.addFile("dir/file1.txt", new byte[100], now);
        delegate.addFile("dir/file2.txt", new byte[200], now);
        delegate.addDirectory("dir/subdir", now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/dir");

        final FileStatus[] statuses = fs.listStatus(path);

        assertThat(statuses).hasSize(3);
        assertThat(statuses)
                .extracting(FileStatus::getPath)
                .extracting(Path::getName)
                .containsExactlyInAnyOrder("file1.txt", "file2.txt", "subdir");
    }

    @Test
    void shouldReturnEmptyArrayForEmptyDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addDirectory("emptydir", OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/emptydir");

        final FileStatus[] statuses = fs.listStatus(path);

        assertThat(statuses).isEmpty();
    }

    @Test
    void shouldReturnSingleElementArrayWhenListingRegularFile() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addFile("somefile", new byte[50], now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/somefile");

        final FileStatus[] statuses = fs.listStatus(path);

        assertThat(statuses).hasSize(1);
        assertThat(statuses[0].isDir()).isFalse();
        assertThat(statuses[0].getLen()).isEqualTo(50);
    }

    @Test
    void shouldThrowFileNotFoundExceptionWhenListingNonexistentPath() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/nonexistent");

        assertThatThrownBy(() -> fs.listStatus(path)).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void shouldThrowIOExceptionOnAzureErrorDuringInitialGetProperties() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectError("failpath", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/failpath");

        assertThatThrownBy(() -> fs.listStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to get path properties");
    }

    @Test
    void shouldListRootDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addFile("rootfile", new byte[10], now);
        delegate.addDirectory("rootdir", now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/");

        final FileStatus[] statuses = fs.listStatus(path);

        assertThat(statuses).hasSize(2);
    }

    @Test
    void shouldThrowFileNotFoundExceptionWhenListPathsReturns404() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectListError("", 404);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/");

        assertThatThrownBy(() -> fs.listStatus(path))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("Directory not found");
    }

    @Test
    void shouldThrowIOExceptionWhenListPathsReturnsNon404Error() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectListError("", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/");

        assertThatThrownBy(() -> fs.listStatus(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to list directory");
    }

    // --- delete ---

    @Test
    void shouldDeleteFile() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("deleteme", new byte[10], OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/deleteme");

        final boolean deleted = fs.delete(path, false);

        assertThat(deleted).isTrue();
        assertThat(delegate.exists("deleteme")).isFalse();
    }

    @Test
    void shouldDeleteEmptyDirectoryNonRecursive() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addDirectory("emptydir", OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/emptydir");

        final boolean deleted = fs.delete(path, false);

        assertThat(deleted).isTrue();
        assertThat(delegate.exists("emptydir")).isFalse();
    }

    @Test
    void shouldDeleteNonEmptyDirectoryRecursive() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addDirectory("parent", now);
        delegate.addFile("parent/child.txt", new byte[10], now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/parent");

        final boolean deleted = fs.delete(path, true);

        assertThat(deleted).isTrue();
        assertThat(delegate.exists("parent")).isFalse();
        assertThat(delegate.exists("parent/child.txt")).isFalse();
    }

    @Test
    void shouldThrowWhenDeletingNonEmptyDirectoryNonRecursive() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addDirectory("parent", now);
        delegate.addFile("parent/child.txt", new byte[10], now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/parent");

        assertThatThrownBy(() -> fs.delete(path, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not empty");
    }

    @Test
    void shouldReturnFalseWhenDeletingNonexistentPath() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/nonexistent");

        final boolean deleted = fs.delete(path, false);

        assertThat(deleted).isFalse();
    }

    @Test
    void shouldThrowIOExceptionOnAzureErrorDuringDelete() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("failfile", new byte[10], OffsetDateTime.now());
        delegate.injectDeleteError("failfile", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/failfile");

        assertThatThrownBy(() -> fs.delete(path, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to delete");
    }

    @Test
    void shouldRethrowNonConflictErrorOnNonRecursiveDirectoryDelete() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addDirectory("dir", OffsetDateTime.now());
        delegate.injectDeleteError("dir", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/dir");

        assertThatThrownBy(() -> fs.delete(path, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to delete");
    }

    // --- mkdirs ---

    @Test
    void shouldCreateDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/newdir");

        final boolean created = fs.mkdirs(path);

        assertThat(created).isTrue();
        assertThat(delegate.exists("newdir")).isTrue();
    }

    @Test
    void shouldSucceedWhenMkdirsOnExistingDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addDirectory("existingdir", OffsetDateTime.now());
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);

        assertThat(fs.mkdirs(new Path(TEST_URI + "/existingdir"))).isTrue();
    }

    @ParameterizedTest
    @MethodSource("mkdirsOnExistingFileCases")
    void shouldThrowWhenMkdirsOnExistingFile(final String filePath, final String mkdirsPath)
            throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile(filePath, new byte[10], OffsetDateTime.now());
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);

        assertThatThrownBy(() -> fs.mkdirs(new Path(TEST_URI + "/" + mkdirsPath)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("path exists as a file");
    }

    private static Stream<Arguments> mkdirsOnExistingFileCases() {
        return Stream.of(
                Arguments.of("existingfile", "existingfile"),
                Arguments.of("dir/existingfile", "dir/existingfile"));
    }

    @Test
    void shouldThrowIOExceptionOnAzureErrorDuringMkdirs() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.injectError("faildir", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path path = new Path(TEST_URI + "/faildir");

        assertThatThrownBy(() -> fs.mkdirs(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to create directory");
    }

    // --- rename ---

    @Test
    void shouldRenameFile() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("oldname", new byte[10], OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/oldname");
        final Path dst = new Path(TEST_URI + "/newname");

        final boolean renamed = fs.rename(src, dst);

        assertThat(renamed).isTrue();
        assertThat(delegate.exists("oldname")).isFalse();
        assertThat(delegate.exists("newname")).isTrue();
    }

    @Test
    void shouldRenameFileIntoExistingDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addFile("file.txt", new byte[10], now);
        delegate.addDirectory("targetdir", now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/file.txt");
        final Path dst = new Path(TEST_URI + "/targetdir");

        final boolean renamed = fs.rename(src, dst);

        assertThat(renamed).isTrue();
        assertThat(delegate.exists("file.txt")).isFalse();
        assertThat(delegate.exists("targetdir/file.txt")).isTrue();
    }

    @Test
    void shouldRenameDirectory() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addDirectory("olddir", now);
        delegate.addFile("olddir/file.txt", new byte[10], now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/olddir");
        final Path dst = new Path(TEST_URI + "/newdir");

        final boolean renamed = fs.rename(src, dst);

        assertThat(renamed).isTrue();
        assertThat(delegate.exists("olddir")).isFalse();
        assertThat(delegate.exists("newdir")).isTrue();
        assertThat(delegate.exists("newdir/file.txt")).isTrue();
    }

    @Test
    void shouldCreateParentDirectoryWhenRenamingIfNotExists() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("file.txt", new byte[10], OffsetDateTime.now());

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/file.txt");
        final Path dst = new Path(TEST_URI + "/newdir/file.txt");

        final boolean renamed = fs.rename(src, dst);

        assertThat(renamed).isTrue();
        assertThat(delegate.exists("newdir")).isTrue();
        assertThat(delegate.exists("newdir/file.txt")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("renameOverExistingFileCases")
    void shouldReturnFalseWhenRenamingOverExistingFile(final String srcPath, final String dstPath)
            throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final OffsetDateTime now = OffsetDateTime.now();
        delegate.addFile(srcPath, new byte[10], now);
        delegate.addFile(dstPath, new byte[5], now);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);

        final boolean renamed =
                fs.rename(new Path(TEST_URI + "/" + srcPath), new Path(TEST_URI + "/" + dstPath));

        assertThat(renamed).isFalse();
        assertThat(delegate.exists(srcPath)).isTrue();
        assertThat(delegate.exists(dstPath)).isTrue();
    }

    private static Stream<Arguments> renameOverExistingFileCases() {
        return Stream.of(
                Arguments.of("src.txt", "dst.txt"), Arguments.of("dir/src.txt", "dir/dst.txt"));
    }

    @Test
    void shouldThrowWhenDstLookupFailsWithNon404Error() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("source", new byte[10], OffsetDateTime.now());
        delegate.injectError("dest", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/source");
        final Path dst = new Path(TEST_URI + "/dest");

        assertThatThrownBy(() -> fs.rename(src, dst))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to rename");
    }

    @Test
    void shouldReturnFalseWhenRenamingNonexistentPath() throws Exception {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/nonexistent");
        final Path dst = new Path(TEST_URI + "/destination");

        final boolean renamed = fs.rename(src, dst);

        assertThat(renamed).isFalse();
    }

    @Test
    void shouldThrowIOExceptionOnAzureErrorDuringRename() {
        final TestingDataLakeStorageOperations delegate = new TestingDataLakeStorageOperations();
        delegate.addFile("source", new byte[10], OffsetDateTime.now());
        delegate.injectError("source", 500);

        final AzureDataLakeFileSystem fs = createTestFileSystem(delegate);
        final Path src = new Path(TEST_URI + "/source");
        final Path dst = new Path(TEST_URI + "/dest");

        assertThatThrownBy(() -> fs.rename(src, dst))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to rename");
    }

    // --- Root path guards ---

    private static final Path ROOT_PATH =
            new Path("abfss://mycontainer@myaccount.dfs.core.windows.net/");

    private static final Path NON_ROOT_PATH =
            new Path("abfss://mycontainer@myaccount.dfs.core.windows.net/file");

    @Test
    void shouldThrowWhenDeletingRootPath() {
        assertThatThrownBy(() -> DEFAULT_FS.delete(ROOT_PATH, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot delete root directory");
    }

    static Stream<Arguments> renameRootPathCases() {
        return Stream.of(
                Arguments.of(ROOT_PATH, NON_ROOT_PATH), Arguments.of(NON_ROOT_PATH, ROOT_PATH));
    }

    @ParameterizedTest
    @MethodSource("renameRootPathCases")
    void shouldThrowWhenRenamingRootPath(final Path src, final Path dst) {
        assertThatThrownBy(() -> DEFAULT_FS.rename(src, dst))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot rename root directory");
    }

    @Test
    void shouldReturnTrueWhenMkdirsOnRootPath() throws Exception {
        assertThat(DEFAULT_FS.mkdirs(ROOT_PATH)).isTrue();
    }
}
