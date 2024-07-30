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

package org.apache.flink.core.fs.local;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * This class tests the functionality of the {@link LocalFileSystem} class in its components. In
 * particular, file/directory access, creation, deletion, read, write is tested.
 */
class LocalFileSystemTest {

    @TempDir private static java.nio.file.Path tempFolder;

    /** This test checks the functionality of the {@link LocalFileSystem} class. */
    @Test
    void testLocalFilesystem() throws Exception {
        final File tempdir =
                new File(TempDirUtils.newFolder(tempFolder), UUID.randomUUID().toString());

        final File testfile1 = new File(tempdir, UUID.randomUUID().toString());
        final File testfile2 = new File(tempdir, UUID.randomUUID().toString());

        final Path pathtotestfile1 = new Path(testfile1.toURI().getPath());
        final Path pathtotestfile2 = new Path(testfile2.toURI().getPath());

        final LocalFileSystem lfs = new LocalFileSystem();

        final Path pathtotmpdir = new Path(tempdir.toURI().getPath());

        /*
         * check that lfs can see/create/delete/read directories
         */

        // check that dir is not existent yet
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir.mkdirs()).isTrue();

        // check that local file system recognizes file..
        assertThat(lfs.exists(pathtotmpdir)).isTrue();
        final FileStatus localstatus1 = lfs.getFileStatus(pathtotmpdir);

        // check that lfs recognizes directory..
        assertThat(localstatus1.isDir()).isTrue();

        // get status for files in this (empty) directory..
        final FileStatus[] statusforfiles = lfs.listStatus(pathtotmpdir);

        // no files in there.. hence, must be zero
        assertThat(statusforfiles).isEmpty();

        // check that lfs can delete directory..
        lfs.delete(pathtotmpdir, true);

        // double check that directory is not existent anymore..
        assertThat(lfs.exists(pathtotmpdir)).isFalse();
        assertThat(tempdir).doesNotExist();

        // re-create directory..
        lfs.mkdirs(pathtotmpdir);

        // creation successful?
        assertThat(tempdir).exists();

        /*
         * check that lfs can create/read/write from/to files properly and read meta information..
         */

        // create files.. one ""natively"", one using lfs
        final FSDataOutputStream lfsoutput1 = lfs.create(pathtotestfile1, WriteMode.NO_OVERWRITE);
        assertThat(testfile2.createNewFile()).isTrue();

        // does lfs create files? does lfs recognize created files?
        assertThat(testfile1).exists();
        assertThat(lfs.exists(pathtotestfile2)).isTrue();

        // test that lfs can write to files properly
        final byte[] testbytes = {1, 2, 3, 4, 5};
        lfsoutput1.write(testbytes);
        lfsoutput1.close();

        assertThat(testfile1).hasSize(5L);

        byte[] testbytestest = new byte[5];
        try (FileInputStream fisfile1 = new FileInputStream(testfile1)) {
            assertThat(fisfile1.read(testbytestest)).isEqualTo(testbytestest.length);
        }

        assertThat(testbytestest).containsExactly(testbytes);

        // does lfs see the correct file length?
        assertThat(testfile1).hasSize(lfs.getFileStatus(pathtotestfile1).getLen());

        // as well, when we call the listStatus (that is intended for directories?)
        assertThat(testfile1).hasSize(lfs.listStatus(pathtotestfile1)[0].getLen());

        // test that lfs can read files properly
        final FileOutputStream fosfile2 = new FileOutputStream(testfile2);
        fosfile2.write(testbytes);
        fosfile2.close();

        testbytestest = new byte[5];
        final FSDataInputStream lfsinput2 = lfs.open(pathtotestfile2);
        assertThat(lfsinput2.read(testbytestest)).isEqualTo(5);
        lfsinput2.close();
        assertThat(testbytestest).containsExactly(testbytes);

        // does lfs see two files?
        assertThat(lfs.listStatus(pathtotmpdir)).hasSize(2);

        // do we get exactly one blocklocation per file? no matter what start and len we provide
        assertThat(lfs.getFileBlockLocations(lfs.getFileStatus(pathtotestfile1), 0, 0).length)
                .isOne();

        /*
         * can lfs delete files / directories?
         */
        assertThat(lfs.delete(pathtotestfile1, false)).isTrue();

        // and can lfs also delete directories recursively?
        assertThat(lfs.delete(pathtotmpdir, true)).isTrue();

        assertThat(tempdir).doesNotExist();
    }

    @Test
    void testRenamePath() throws IOException {
        final File rootDirectory = TempDirUtils.newFolder(tempFolder);

        // create a file /root/src/B/test.csv
        final File srcDirectory = new File(new File(rootDirectory, "src"), "B");
        assertThat(srcDirectory.mkdirs()).isTrue();

        final File srcFile = new File(srcDirectory, "test.csv");
        assertThat(srcFile.createNewFile()).isTrue();

        // Move/rename B and its content to /root/dst/A
        final File destDirectory = new File(new File(rootDirectory, "dst"), "B");
        final File destFile = new File(destDirectory, "test.csv");

        final Path srcDirPath = new Path(srcDirectory.toURI());
        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destDirPath = new Path(destDirectory.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        FileSystem fs = FileSystem.getLocalFileSystem();

        // pre-conditions: /root/src/B exists but /root/dst/B does not
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destDirPath)).isFalse();

        // do the move/rename: /root/src/B -> /root/dst/
        assertThat(fs.rename(srcDirPath, destDirPath)).isTrue();

        // post-conditions: /root/src/B doesn't exists, /root/dst/B/test.csv has been created
        assertThat(fs.exists(destFilePath)).isTrue();
        assertThat(fs.exists(srcDirPath)).isFalse();

        // re-create source file and test overwrite
        assertThat(srcDirectory.mkdirs()).isTrue();
        assertThat(srcFile.createNewFile()).isTrue();

        // overwrite the destination file
        assertThat(fs.rename(srcFilePath, destFilePath)).isTrue();

        // post-conditions: now only the src file has been moved
        assertThat(fs.exists(srcFilePath)).isFalse();
        assertThat(fs.exists(srcDirPath)).isTrue();
        assertThat(fs.exists(destFilePath)).isTrue();
    }

    @Test
    void testRenameNonExistingFile() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        File tmpDir = TempDirUtils.newFolder(tempFolder);
        final File srcFile = new File(tmpDir, "someFile.txt");
        final File destFile = new File(tmpDir, "target");

        final Path srcFilePath = new Path(srcFile.toURI());
        final Path destFilePath = new Path(destFile.toURI());

        // this cannot succeed because the source file does not exist
        assertThat(fs.rename(srcFilePath, destFilePath)).isFalse();
    }

    @Test
    @Tag("FailsInGHAContainerWithRootUser")
    @Disabled
    void testRenameFileWithNoAccess() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        final File srcFile = TempDirUtils.newFile(tempFolder, "someFile.txt");
        final File destFile = new File(TempDirUtils.newFolder(tempFolder), "target");

        // we need to make the file non-modifiable so that the rename fails
        assumeThat(srcFile.getParentFile().setWritable(false, false)).isTrue();
        assumeThat(srcFile.setWritable(false, false)).isTrue();

        try {
            final Path srcFilePath = new Path(srcFile.toURI());
            final Path destFilePath = new Path(destFile.toURI());

            // this cannot succeed because the source folder has no permission to remove the file
            assertThat(fs.rename(srcFilePath, destFilePath)).isFalse();
        } finally {
            // make sure we give permission back to ensure proper cleanup

            //noinspection ResultOfMethodCallIgnored
            srcFile.getParentFile().setWritable(true, false);
            //noinspection ResultOfMethodCallIgnored
            srcFile.setWritable(true, false);
        }
    }

    @Test
    void testRenameToNonEmptyTargetDir() throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();

        // a source folder with a file
        final File srcFolder = TempDirUtils.newFolder(tempFolder);
        final File srcFile = new File(srcFolder, "someFile.txt");
        assertThat(srcFile.createNewFile()).isTrue();

        // a non-empty destination folder
        final File dstFolder = TempDirUtils.newFolder(tempFolder);
        final File dstFile = new File(dstFolder, "target");
        assertThat(dstFile.createNewFile()).isTrue();

        // this cannot succeed because the destination folder is not empty
        assertThat(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI()))).isFalse();

        // retry after deleting the occupying target file
        assertThat(dstFile.delete()).isTrue();
        assertThat(fs.rename(new Path(srcFolder.toURI()), new Path(dstFolder.toURI()))).isTrue();
        assertThat(new File(dstFolder, srcFile.getName())).exists();
    }

    @Test
    void testKind() {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        assertThat(fs.getKind()).isEqualTo(FileSystemKind.FILE_SYSTEM);
    }

    @Test
    void testConcurrentMkdirs() throws Exception {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final File root = TempDirUtils.newFolder(tempFolder);
        final int directoryDepth = 10;
        final int concurrentOperations = 10;

        final Collection<File> targetDirectories =
                createTargetDirectories(root, directoryDepth, concurrentOperations);

        final ExecutorService executor = Executors.newFixedThreadPool(concurrentOperations);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(concurrentOperations);

        try {
            final Collection<CompletableFuture<Void>> mkdirsFutures =
                    new ArrayList<>(concurrentOperations);
            for (File targetDirectory : targetDirectories) {
                final CompletableFuture<Void> mkdirsFuture =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        cyclicBarrier.await();
                                        assertThat(fs.mkdirs(Path.fromLocalFile(targetDirectory)))
                                                .isEqualTo(true);
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executor);

                mkdirsFutures.add(mkdirsFuture);
            }

            final CompletableFuture<Void> allFutures =
                    CompletableFuture.allOf(
                            mkdirsFutures.toArray(new CompletableFuture[concurrentOperations]));

            allFutures.get();
        } finally {
            final long timeout = 10000L;
            ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, executor);
        }
    }

    /** This test verifies the issue https://issues.apache.org/jira/browse/FLINK-18612. */
    @Test
    void testCreatingFileInCurrentDirectoryWithRelativePath() throws IOException {
        FileSystem fs = FileSystem.getLocalFileSystem();

        Path filePath = new Path("local_fs_test_" + RandomStringUtils.randomAlphanumeric(16));
        try (FSDataOutputStream outputStream = fs.create(filePath, WriteMode.OVERWRITE)) {
            // Do nothing.
        } finally {
            for (int i = 0; i < 10 && fs.exists(filePath); ++i) {
                fs.delete(filePath, true);
            }
        }
    }

    @Test
    void testFlushMethodFailsOnClosedOutputStream() {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(FSDataOutputStream::flush));
    }

    @Test
    void testWriteIntegerMethodFailsOnClosedOutputStream() {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(os -> os.write(0)));
    }

    @Test
    void testWriteBytesMethodFailsOnClosedOutputStream() {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(os -> os.write(new byte[0])));
    }

    @Test
    void testWriteBytesSubArrayMethodFailsOnClosedOutputStream() {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(
                        () ->
                                testMethodCallFailureOnClosedStream(
                                        os -> os.write(new byte[0], 0, 0)));
    }

    @Test
    void testGetPosMethodFailsOnClosedOutputStream() {
        assertThatExceptionOfType(ClosedChannelException.class)
                .isThrownBy(() -> testMethodCallFailureOnClosedStream(FSDataOutputStream::getPos));
    }

    private void testMethodCallFailureOnClosedStream(
            ThrowingConsumer<FSDataOutputStream, IOException> callback) throws IOException {
        final FileSystem fs = FileSystem.getLocalFileSystem();
        final FSDataOutputStream outputStream =
                fs.create(
                        new Path(tempFolder.toString(), "close_fs_test_" + UUID.randomUUID()),
                        WriteMode.OVERWRITE);
        outputStream.close();
        callback.accept(outputStream);
    }

    private Collection<File> createTargetDirectories(
            File root, int directoryDepth, int numberDirectories) {
        final StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < directoryDepth; i++) {
            stringBuilder.append('/').append(i);
        }

        final Collection<File> targetDirectories = new ArrayList<>(numberDirectories);

        for (int i = 0; i < numberDirectories; i++) {
            targetDirectories.add(new File(root, stringBuilder.toString() + '/' + i));
        }

        return targetDirectories;
    }
}
