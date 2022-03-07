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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public abstract FileSystem getFileSystem() throws Exception;

    /** Gets the base path in the file system under which tests will place their temporary files. */
    public abstract Path getBasePath() throws Exception;

    /** Gets the kind of the file system (file system, object store, ...). */
    public abstract FileSystemKind getFileSystemKind();

    // ------------------------------------------------------------------------
    //  Init / Cleanup
    // ------------------------------------------------------------------------

    @Before
    public void prepare() throws Exception {
        fs = getFileSystem();
        basePath = new Path(getBasePath(), randomName());
        fs.mkdirs(basePath);
    }

    @After
    public void cleanup() throws Exception {
        fs.delete(basePath, true);
    }

    // ------------------------------------------------------------------------
    //  Suite of Tests
    // ------------------------------------------------------------------------

    // --- file system kind

    @Test
    public void testFileSystemKind() {
        assertEquals(getFileSystemKind(), fs.getKind());
    }

    // --- access and scheme

    @Test
    public void testPathAndScheme() throws Exception {
        assertEquals(fs.getUri(), getBasePath().getFileSystem().getUri());
        assertEquals(fs.getUri().getScheme(), getBasePath().toUri().getScheme());
    }

    @Test
    public void testHomeAndWorkDir() {
        assertEquals(fs.getUri().getScheme(), fs.getWorkingDirectory().toUri().getScheme());
        assertEquals(fs.getUri().getScheme(), fs.getHomeDirectory().toUri().getScheme());
    }
    // --- exists

    @Test
    public void testFileExists() throws IOException {
        final Path filePath = createRandomFileInDirectory(basePath);
        assertTrue(fs.exists(filePath));
    }

    @Test
    public void testFileDoesNotExist() throws IOException {
        assertFalse(fs.exists(new Path(basePath, randomName())));
    }

    // --- delete

    @Test
    public void testExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), false);
    }

    @Test
    public void testExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(createRandomFileInDirectory(basePath), true);
    }

    @Test
    public void testNotExistingFileDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), false);
    }

    @Test
    public void testNotExistingFileRecursiveDeletion() throws IOException {
        testSuccessfulDeletion(new Path(basePath, randomName()), true);
    }

    @Test
    public void testExistingEmptyDirectoryDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, false);
    }

    @Test
    public void testExistingEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path path = new Path(basePath, randomName());
        fs.mkdirs(path);
        testSuccessfulDeletion(path, true);
    }

    private void testSuccessfulDeletion(Path path, boolean recursionEnabled) throws IOException {
        fs.delete(path, recursionEnabled);
        assertFalse(fs.exists(path));
    }

    @Test
    public void testExistingNonEmptyDirectoryDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        assertThrows(IOException.class, () -> fs.delete(directoryPath, false));
        assertTrue(fs.exists(directoryPath));
        assertTrue(fs.exists(filePath));
    }

    @Test
    public void testExistingNonEmptyDirectoryRecursiveDeletion() throws IOException {
        final Path directoryPath = new Path(basePath, randomName());
        final Path filePath = createRandomFileInDirectory(directoryPath);

        fs.delete(directoryPath, true);
        assertFalse(fs.exists(directoryPath));
        assertFalse(fs.exists(filePath));
    }

    @Test
    public void testExistingNonEmptyDirectoryWithSubDirRecursiveDeletion() throws IOException {
        final Path level1SubDirWithFile = new Path(basePath, randomName());
        final Path fileInLevel1Subdir = createRandomFileInDirectory(level1SubDirWithFile);
        final Path level2SubDirWithFile = new Path(level1SubDirWithFile, randomName());
        final Path fileInLevel2Subdir = createRandomFileInDirectory(level2SubDirWithFile);

        testSuccessfulDeletion(level1SubDirWithFile, true);
        assertFalse(fs.exists(fileInLevel1Subdir));
        assertFalse(fs.exists(level2SubDirWithFile));
        assertFalse(fs.exists(fileInLevel2Subdir));
    }

    // --- mkdirs

    @Test
    public void testMkdirsReturnsTrueWhenCreatingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());
        assertTrue(fs.mkdirs(directory));

        if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
            assertTrue(fs.exists(directory));
        }
    }

    @Test
    public void testMkdirsCreatesParentDirectories() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory =
                new Path(new Path(new Path(basePath, randomName()), randomName()), randomName());
        assertTrue(fs.mkdirs(directory));

        if (getFileSystemKind() != FileSystemKind.OBJECT_STORE) {
            assertTrue(fs.exists(directory));
        }
    }

    @Test
    public void testMkdirsReturnsTrueForExistingDirectory() throws Exception {
        // this test applies to object stores as well, as rely on the fact that they
        // return true when things are not bad

        final Path directory = new Path(basePath, randomName());

        // make sure the directory exists
        createRandomFileInDirectory(directory);

        assertTrue(fs.mkdirs(directory));
    }

    @Test
    public void testMkdirsFailsForExistingFile() throws Exception {
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
    public void testMkdirsFailsWithExistingParentFile() throws Exception {
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
        Assume.assumeTrue(
                "Test does not apply to object stores",
                getFileSystemKind() != FileSystemKind.OBJECT_STORE);
    }
}
