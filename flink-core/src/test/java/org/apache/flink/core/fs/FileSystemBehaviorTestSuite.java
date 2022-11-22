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

package org.apache.flink.core.fs;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Common tests for the behavior of {@link FileSystem} methods. */
public abstract class FileSystemBehaviorTestSuite {

    private static final Random RND = new Random();

    /** The cached file system instance. */
    private FileSystem fs;

    /** The cached base path. */
    private Path basePath;

    // ------------------------------------------------------------------------
    //  FileSystem-specific methods
    // ------------------------------------------------------------------------

    /** Gets an instance of the {@code FileSystem} to be tested. */
    protected abstract FileSystem getFileSystem() throws Exception;

    /** Gets the base path in the file system under which tests will place their temporary files. */
    protected abstract Path getBasePath() throws Exception;

    /** Gets the kind of the file system (file system, object store, ...). */
    protected abstract FileSystemKind getFileSystemKind();

    // ------------------------------------------------------------------------
    //  Init / Cleanup
    // ------------------------------------------------------------------------

    @BeforeEach
    void prepare() throws Exception {
        fs = getFileSystem();
        basePath = new Path(getBasePath(), randomName());
        fs.mkdirs(basePath);
    }

    @AfterEach
    void cleanup() throws Exception {
        fs.delete(basePath, true);
    }

    // ------------------------------------------------------------------------
    //  Suite of Tests
    // ------------------------------------------------------------------------

    // --- file system kind

    @Test
    void testFileSystemKind() {
        assertThat(fs.getKind()).isEqualTo(getFileSystemKind());
    }

    // --- access and scheme

    @Test
    void testPathAndScheme() throws Exception {
        assertThat(fs.getUri()).isEqualTo(getBasePath().getFileSystem().getUri());
        assertThat(fs.getUri().getScheme()).isEqualTo(getBasePath().toUri().getScheme());
    }

    @Test
    void testHomeDirScheme() {
        assertThat(fs.getHomeDirectory().toUri().getScheme()).isEqualTo(fs.getUri().getScheme());
    }

    @Test
    void testWorkDirScheme() {
        assertThat(fs.getWorkingDirectory().toUri().getScheme()).isEqualTo(fs.getUri().getScheme());
    }

    // --- exists

    @Test
    void testFileExists() throws IOException {
        final Path filePath = createRandomFileInDirectory(basePath);
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testFileDoesNotExist() throws IOException {
        assertThat(fs.exists(new Path(basePath, randomName()))).isFalse();
    }

    // --- delete

    @Test
    void testExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), false);
    }

    @Test
    void testExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), true);
    }

    @Test
    void testNotExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), false);
    }

    @Test
    void testNotExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), true);
    }

    @Test
    void testExistingEmptyDirectoryDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, false);
    }

    @Test
    void testExistingEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, true);
    }

    private void testSuccessfulDeletion(Path path, boolean recursionEnabled) throws IOException {
        fs.delete(path, recursionEnabled);
        assertThat(fs.exists(path)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        assertThatThrownBy(() -> fs.delete(directoryPath, false)).isInstanceOf(IOException.class);
        assertThat(fs.exists(directoryPath)).isTrue();
        assertThat(fs.exists(filePath)).isTrue();
    }

    @Test
    void testExistingNonEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        fs.delete(directoryPath, true);
        assertThat(fs.exists(directoryPath)).isFalse();
        assertThat(fs.exists(filePath)).isFalse();
    }

    @Test
    void testExistingNonEmptyDirectoryWithSubDirRecursiveDeletion() throws IOException {
        final Path level1SubDirWithFile = new Path(basePath, randomName());
        final Path fileInLevel1Subdir = createRandomFileInDirectory(level1SubDirWithFile);
        final Path level2SubDirWithFile = new Path(level1SubDirWithFile, randomName());
        final Path fileInLevel2Subdir = createRandomFileInDirectory(level2SubDirWithFile);

        testSuccessfulDeletion(level1SubDirWithFile, true);
        assertThat(fs.exists(fileInLevel1Subdir)).isFalse();
        assertThat(fs.exists(level2SubDirWithFile)).isFalse();
        assertThat(fs.exists(fileInLevel2Subdir)).isFalse();
    }

    // --- mkdirs

    @Test
    void testMkdirsReturnsTrueWhenCreatingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());
        assertThat(fs.mkdirs(directory)).isTrue();

        if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
            assertThat(fs.exists(directory)).isTrue();
        }
    }

    @Test
    void testMkdirsCreatesParentDirectories() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory =
                new Path(new Path(new Path(basePath, randomName()), randomName()), randomName());
        assertThat(fs.mkdirs(directory)).isTrue();

        if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
            assertThat(fs.exists(directory)).isTrue();
        }
    }

    @Test
    void testMkdirsReturnsTrueForExistingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());

        // make sure the directory exists
        createRandomFileInDirectory(directory);

        assertThat(fs.mkdirs(directory)).isTrue();
    }

    @Test
    protected void testMkdirsFailsForExistingFile() throws Exception {
        // test is not defined for object stores, they have no proper notion
        // of directories
        assumeNotObjectStore();

        final Path file = new Path(getBasePath(), randomName());
        createFile(file);

        try {
            fs.mkdirs(file);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    @Test
    void testMkdirsFailsWithExistingParentFile() throws Exception {
        // test is not defined for object stores, they have no proper notion
        // of directories
        assumeNotObjectStore();

        final Path file = new Path(getBasePath(), randomName());
        createFile(file);

        final Path dirUnderFile = new Path(file, randomName());
        try {
            fs.mkdirs(dirUnderFile);
            fail("should fail with an IOException");
        } catch (IOException e) {
            // good!
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static String randomName() {
        return StringUtils.getRandomString(RND, 16, 16, 'a', 'z');
    }

    private void createFile(Path file) throws IOException {
        try (FSDataOutputStream out = fs.create(file, WriteMode.NO_OVERWRITE)) {
            out.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        }
    }

    private Path createRandomFileInDirectory(Path directory) throws IOException {
        fs.mkdirs(directory);
        final Path filePath = new Path(directory, randomName());
        createFile(filePath);

        return filePath;
    }

    private void assumeNotObjectStore() {
        assumeThat(getFileSystemKind() != FileSystemKind.OBJECT_STORE)
                .describedAs("Test does not apply to object stores")
                .isTrue();
    }
}
