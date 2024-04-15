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

package org.apache.flink.api.common.io;

import org.apache.flink.api.common.io.FileInputFormat.FileBaseStatistics;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.TestFileUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.IntValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

/** Tests for the FileInputFormat */
class FileInputFormatTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testGetPathWithoutSettingFirst() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        assertThat(format.getFilePath()).as("Path should be null.").isNull();
    }

    @Test
    void testGetPathsWithoutSettingFirst() {
        final DummyFileInputFormat format = new DummyFileInputFormat();

        Path[] paths = format.getFilePaths();
        assertThat(paths).as("Paths should not be null.").isNotNull();
        assertThat(paths).as("Paths should be empty.").isEmpty();
    }

    @Test
    void testToStringWithoutPathSet() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        assertThat(format.toString())
                .as("The toString() should be correct.")
                .isEqualTo("File Input (unknown file)");
    }

    @Test
    void testSetPathsNull() {
        assertThatThrownBy(() -> new MultiDummyFileInputFormat().setFilePaths((String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetPathNullString() {
        assertThatThrownBy(() -> new DummyFileInputFormat().setFilePath((String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetPathNullPath() {
        assertThatThrownBy(() -> new DummyFileInputFormat().setFilePath((Path) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetPathsOnePathNull() {
        assertThatThrownBy(
                        () ->
                                new MultiDummyFileInputFormat()
                                        .setFilePaths("/an/imaginary/path", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetPathsEmptyArray() {
        assertThatThrownBy(() -> new MultiDummyFileInputFormat().setFilePaths(new String[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetPath() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath("/some/imaginary/path");
        assertThat("/some/imaginary/path").isEqualTo(format.getFilePath().toString());
    }

    @Test
    void testSetPathOnMulti() {
        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        format.setFilePath(myPath);
        final Path[] filePaths = format.getFilePaths();

        assertThat(filePaths).hasSize(1);
        assertThat(filePaths[0].toUri().toString()).isEqualTo(myPath);

        // ensure backwards compatibility
        assertThat(format.filePath.toUri().toString()).isEqualTo(myPath);
    }

    @Test
    void testSetPathsSingleWithMulti() {
        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        format.setFilePaths(myPath);
        final Path[] filePaths = format.getFilePaths();

        assertThat(filePaths).hasSize(1);
        assertThat(filePaths[0].toUri().toString()).isEqualTo(myPath);

        // ensure backwards compatibility
        assertThat(format.filePath.toUri().toString()).isEqualTo(myPath);
    }

    @Test
    void testSetPathsMulti() {
        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        final String myPath2 = "/an/imaginary/path2";

        format.setFilePaths(myPath, myPath2);
        final Path[] filePaths = format.getFilePaths();

        assertThat(filePaths).hasSize(2);
        assertThat(filePaths[0].toUri().toString()).isEqualTo(myPath);
        assertThat(filePaths[1].toUri().toString()).isEqualTo(myPath2);
    }

    @Test
    void testMultiPathSetOnSinglePathIF() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        final String myPath2 = "/an/imaginary/path2";

        assertThatThrownBy(() -> format.setFilePaths(myPath, myPath2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testMultiPathSetOnSinglePathIF2() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        final String myPath2 = "/an/imaginary/path2";

        // format.setFilePaths(new Path(myPath), new Path(myPath2));
        assertThatThrownBy(() -> format.setFilePaths(new Path(myPath), new Path(myPath2)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSinglePathGetOnMultiPathIF() {
        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        final String myPath = "/an/imaginary/path";
        final String myPath2 = "/an/imaginary/path2";

        format.setFilePaths(myPath, myPath2);
        assertThatThrownBy(format::getFilePath).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testSetFileViaConfiguration() {
        final DummyFileInputFormat format = new DummyFileInputFormat();
        final String filePath = "file:///some/none/existing/directory/";
        Configuration conf = new Configuration();
        conf.setString("input.file.path", filePath);
        format.configure(conf);

        assertThat(format.getFilePath()).isEqualTo(new Path(filePath));
    }

    @Test
    void testSetFileViaConfigurationEmptyPath() {
        assertThatThrownBy(
                        () -> {
                            final DummyFileInputFormat format = new DummyFileInputFormat();
                            final String filePath = null;
                            Configuration conf = new Configuration();
                            conf.setString("input.file.path", filePath);
                            format.configure(conf);
                        })
                .isInstanceOf(RuntimeException.class);
    }

    // ------------------------------------------------------------------------
    //  Input Splits
    // ------------------------------------------------------------------------

    @Test
    void testCreateInputSplitSingleFile() throws IOException {
        String tempFile = TestFileUtils.createTempFile("Hello World");
        FileInputFormat<IntValue> fif = new DummyFileInputFormat();
        fif.setFilePath(tempFile);

        fif.configure(new Configuration());
        FileInputSplit[] splits = fif.createInputSplits(2);

        assertThat(splits).hasSize(2);
        assertThat(splits[0].getPath().toString()).isEqualTo(tempFile);
        assertThat(splits[1].getPath().toString()).isEqualTo(tempFile);
    }

    @Test
    void testCreateInputSplitMultiFiles() throws IOException {
        String tempFile1 = TestFileUtils.createTempFile(21);
        String tempFile2 = TestFileUtils.createTempFile(22);
        String tempFile3 = TestFileUtils.createTempFile(23);
        FileInputFormat<IntValue> fif = new MultiDummyFileInputFormat();
        fif.setFilePaths(tempFile1, tempFile2, tempFile3);

        fif.configure(new Configuration());
        FileInputSplit[] splits = fif.createInputSplits(3);

        int numSplitsFile1 = 0;
        int numSplitsFile2 = 0;
        int numSplitsFile3 = 0;

        assertThat(splits).hasSize(3);
        for (FileInputSplit fis : splits) {
            assertThat(fis.getStart()).isZero();
            if (fis.getPath().toString().equals(tempFile1)) {
                numSplitsFile1++;
                assertThat(fis.getLength()).isEqualTo(21);
            } else if (fis.getPath().toString().equals(tempFile2)) {
                numSplitsFile2++;
                assertThat(fis.getLength()).isEqualTo(22);
            } else if (fis.getPath().toString().equals(tempFile3)) {
                numSplitsFile3++;
                assertThat(fis.getLength()).isEqualTo(23);
            } else {
                fail("Got split for unknown file.");
            }
        }

        assertThat(numSplitsFile1).isOne();
        assertThat(numSplitsFile2).isOne();
        assertThat(numSplitsFile3).isOne();
    }

    // ------------------------------------------------------------------------
    //  Statistics
    // ------------------------------------------------------------------------

    @Test
    void testGetStatisticsNonExistingFile() throws IOException {

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath("file:///some/none/existing/directory/");
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats).as("The file statistics should be null.").isNull();
    }

    @Test
    void testGetStatisticsOneFileNoCachedVersion() throws IOException {

        final long SIZE = 1024 * 500;
        String tempFile = TestFileUtils.createTempFile(SIZE);

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(SIZE);
    }

    @Test
    void testGetStatisticsMultipleFilesNoCachedVersion() throws IOException {

        final long SIZE1 = 2077;
        final long SIZE2 = 31909;
        final long SIZE3 = 10;
        final long TOTAL = SIZE1 + SIZE2 + SIZE3;

        String tempDir =
                TestFileUtils.createTempFileDir(
                        TempDirUtils.newFolder(temporaryFolder), SIZE1, SIZE2, SIZE3);

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempDir);
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(TOTAL);
    }

    @Test
    void testGetStatisticsOneFileWithCachedVersion() throws IOException {

        final long SIZE = 50873;
        final long FAKE_SIZE = 10065;

        String tempFile = TestFileUtils.createTempFile(SIZE);

        DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(SIZE);

        format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics newStats = format.getStatistics(stats);
        assertThat(stats).as("Statistics object was changed").isSameAs(newStats);

        // insert fake stats with the correct modification time. the call should return the fake
        // stats
        format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics fakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime(),
                        FAKE_SIZE,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics latest = format.getStatistics(fakeStats);
        assertThat(latest.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(FAKE_SIZE);

        // insert fake stats with the expired modification time. the call should return new
        // accurate stats
        format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        FileBaseStatistics outDatedFakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime() - 1,
                        FAKE_SIZE,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
        assertThat(reGathered.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(SIZE);
    }

    @Test
    void testGetStatisticsMultipleFilesWithCachedVersion() throws IOException {

        FileSystem fs = FileSystem.getLocalFileSystem();

        final long SIZE1 = 2077;
        final long SIZE2 = 31909;
        final long SIZE3 = 10;
        final long TOTAL = SIZE1 + SIZE2 + SIZE3;
        final long FAKE_SIZE = 10065;

        File tempDirFile = TempDirUtils.newFolder(temporaryFolder);
        String tempDir = tempDirFile.getAbsolutePath();
        String f1 = TestFileUtils.createTempFileInDirectory(tempDir, SIZE1);
        long modTime1 = fs.getFileStatus(new Path(f1)).getModificationTime();
        String f2 = TestFileUtils.createTempFileInDirectory(tempDir, SIZE2);
        long modTime2 = fs.getFileStatus(new Path(f2)).getModificationTime();
        String f3 = TestFileUtils.createTempFileInDirectory(tempDir, SIZE3);
        long modTime3 = fs.getFileStatus(new Path(f3)).getModificationTime();

        DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempDir);
        format.configure(new Configuration());

        FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(TOTAL);

        format = new DummyFileInputFormat();
        format.setFilePath(tempDir);
        format.configure(new Configuration());

        FileBaseStatistics newStats = format.getStatistics(stats);
        assertThat(stats).as("Statistics object was changed").isSameAs(newStats);

        // insert fake stats with the correct modification time. the call should return the fake
        // stats
        format = new DummyFileInputFormat();
        format.setFilePath(tempDir);
        format.configure(new Configuration());

        FileBaseStatistics fakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime(),
                        FAKE_SIZE,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics latest = format.getStatistics(fakeStats);
        assertThat(latest.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(FAKE_SIZE);

        // insert fake stats with the correct modification time. the call should return the fake
        // stats
        format = new DummyFileInputFormat();
        format.setFilePath(tempDir);
        format.configure(new Configuration());

        FileBaseStatistics outDatedFakeStats =
                new FileBaseStatistics(
                        Math.min(Math.min(modTime1, modTime2), modTime3) - 1,
                        FAKE_SIZE,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
        assertThat(reGathered.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(TOTAL);
    }

    // -- Multiple Files -- //

    @Test
    void testGetStatisticsMultipleNonExistingFile() throws IOException {
        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        format.setFilePaths(
                "file:///some/none/existing/directory/", "file:///another/non/existing/directory/");
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats).as("The file statistics should be null.").isNull();
    }

    @Test
    void testGetStatisticsMultipleOneFileNoCachedVersion() throws IOException {
        final long size1 = 1024 * 500;
        String tempFile = TestFileUtils.createTempFile(size1);

        final long size2 = 1024 * 505;
        String tempFile2 = TestFileUtils.createTempFile(size2);

        final long totalSize = size1 + size2;

        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        format.setFilePaths(tempFile, tempFile2);
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(totalSize);
    }

    @Test
    void testGetStatisticsMultipleFilesMultiplePathsNoCachedVersion() throws IOException {
        final long size1 = 2077;
        final long size2 = 31909;
        final long size3 = 10;
        final long totalSize123 = size1 + size2 + size3;

        String tempDir =
                TestFileUtils.createTempFileDir(
                        TempDirUtils.newFolder(temporaryFolder), size1, size2, size3);

        final long size4 = 2051;
        final long size5 = 31902;
        final long size6 = 15;
        final long totalSize456 = size4 + size5 + size6;
        String tempDir2 =
                TestFileUtils.createTempFileDir(
                        TempDirUtils.newFolder(temporaryFolder), size4, size5, size6);

        final MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        format.setFilePaths(tempDir, tempDir2);
        format.configure(new Configuration());

        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(totalSize123 + totalSize456);
    }

    @Test
    void testGetStatisticsMultipleOneFileWithCachedVersion() throws IOException {
        FileSystem fs = FileSystem.getLocalFileSystem();

        final long size1 = 50873;
        final long fakeSize = 10065;
        String tempFile1 = TestFileUtils.createTempFile(size1);
        final long lastModTime1 = fs.getFileStatus(new Path(tempFile1)).getModificationTime();

        final long size2 = 52573;
        String tempFile2 = TestFileUtils.createTempFile(size2);
        final long lastModTime2 = fs.getFileStatus(new Path(tempFile2)).getModificationTime();

        final long sizeTotal = size1 + size2;

        MultiDummyFileInputFormat format = new MultiDummyFileInputFormat();
        format.setFilePaths(tempFile1, tempFile2);
        format.configure(new Configuration());

        FileBaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(sizeTotal);

        format = new MultiDummyFileInputFormat();
        format.setFilePath(tempFile1);
        format.configure(new Configuration());

        FileBaseStatistics newStats = format.getStatistics(stats);
        assertThat(stats).as("Statistics object was changed").isSameAs(newStats);

        // insert fake stats with the correct modification time. the call should return the fake
        // stats
        format = new MultiDummyFileInputFormat();
        format.setFilePath(tempFile1);
        format.configure(new Configuration());

        FileBaseStatistics fakeStats =
                new FileBaseStatistics(
                        stats.getLastModificationTime(),
                        fakeSize,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics latest = format.getStatistics(fakeStats);
        assertThat(latest.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(fakeSize);

        // insert fake stats with the expired modification time. the call should return new accurate
        // stats
        format = new MultiDummyFileInputFormat();
        format.setFilePaths(tempFile1, tempFile2);
        format.configure(new Configuration());

        FileBaseStatistics outDatedFakeStats =
                new FileBaseStatistics(
                        Math.min(lastModTime1, lastModTime2) - 1,
                        fakeSize,
                        BaseStatistics.AVG_RECORD_BYTES_UNKNOWN);
        BaseStatistics reGathered = format.getStatistics(outDatedFakeStats);
        assertThat(reGathered.getTotalInputSize())
                .as("The file size from the statistics is wrong.")
                .isEqualTo(sizeTotal);
    }

    // ------------------------------------------------------------------------
    //  Unsplittable input files
    // ------------------------------------------------------------------------

    // ---- Tests for compressed files  ---------

    /**
     * Create directory with compressed files and see if it creates a split for each file. Each
     * split has to start from the beginning.
     */
    @Test
    void testFileInputFormatWithCompression() throws IOException {

        String tempFile =
                TestFileUtils.createTempFileDirForProvidedFormats(
                        TempDirUtils.newFolder(temporaryFolder),
                        FileInputFormat.getSupportedCompressionFormats());
        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());
        FileInputSplit[] splits = format.createInputSplits(2);
        final Set<String> supportedCompressionFormats =
                FileInputFormat.getSupportedCompressionFormats();
        assertThat(splits).hasSameSizeAs(supportedCompressionFormats);
        for (FileInputSplit split : splits) {
            assertThat(split.getLength())
                    .isEqualTo(
                            FileInputFormat.READ_WHOLE_SPLIT_FLAG); // unsplittable compressed files
            // have this size as a
            // flag for "read whole file"
            assertThat(split.getStart()).isZero(); // always read from the beginning.
        }

        // test if this also works for "mixed" directories
        TestFileUtils.createTempFileInDirectory(
                tempFile.replace("file:", ""),
                "this creates a test file with a random extension (at least not .deflate)");

        final DummyFileInputFormat formatMixed = new DummyFileInputFormat();
        formatMixed.setFilePath(tempFile);
        formatMixed.configure(new Configuration());
        FileInputSplit[] splitsMixed = formatMixed.createInputSplits(2);
        assertThat(splitsMixed).hasSize(supportedCompressionFormats.size() + 1);
        for (FileInputSplit split : splitsMixed) {
            final String extension =
                    FileInputFormat.extractFileExtension(split.getPath().getName());
            if (supportedCompressionFormats.contains(extension)) {
                assertThat(split.getLength())
                        .isEqualTo(
                                FileInputFormat.READ_WHOLE_SPLIT_FLAG); // unsplittable compressed
                // files have this size as a
                // flag for "read whole file"
                assertThat(split.getStart()).isZero(); // always read from the beginning.
            } else {
                assertThat(split.getStart()).isEqualTo(0L);
                assertThat(split.getLength() > 0).as("split size not correct").isTrue();
            }
        }
    }

    /**
     * Some FileInputFormats don't use FileInputFormat#createSplits (that would detect that the file
     * is non-splittable and deal with reading boundaries correctly), they all create splits
     * manually from FileSourceSplit. If input files are compressed, ensure that the size of the
     * split is not the compressed file size and that the compression decorator is called.
     */
    @Test
    void testFileInputFormatWithCompressionFromFileSource() throws IOException {

        String tempFile =
                TestFileUtils.createTempFileDirForProvidedFormats(
                        TempDirUtils.newFolder(temporaryFolder),
                        FileInputFormat.getSupportedCompressionFormats());
        DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(tempFile);
        format.configure(new Configuration());

        // manually create a FileInputSplit per file as FileSource would do
        // see org.apache.flink.connector.file.table.DeserializationSchemaAdapter.Reader()
        List<FileInputSplit> splits = manuallyCreateSplits(tempFile);
        final Set<String> supportedCompressionFormats =
                FileInputFormat.getSupportedCompressionFormats();
        // one file per compression format, one split per file
        assertThat(splits).hasSameSizeAs(supportedCompressionFormats);
        for (FileInputSplit split : splits) {
            assertThat(split.getStart()).isZero(); // always read from the beginning.
            format.open(split);
            assertThat(format.compressedRead).isTrue();
            assertThat(format.getSplitLength())
                    .isEqualTo(
                            FileInputFormat.READ_WHOLE_SPLIT_FLAG); // unsplittable compressed files
            // have this size
            // as flag for "read whole file"
        }
    }

    /**
     * Simulates splits created by org.apache.flink.connector.file.src.FileSource (one split per
     * file with length = size of the file). For compressed file, the input format should override
     * it when it detects that the file is unsplittable in {@link
     * FileInputFormat#open(FileInputSplit)}.
     */
    private List<FileInputSplit> manuallyCreateSplits(String pathString) throws IOException {
        List<FileInputSplit> splits = new ArrayList<>();
        final Path path = new Path(pathString);
        final FileSystem fs = path.getFileSystem();
        for (FileStatus file : fs.listStatus(path)) {
            // split created like in DeserializationSchemaAdapter.Reader()
            splits.add(new FileInputSplit(0, file.getPath(), 0, file.getLen(), null));
        }
        return splits;
    }
    // ------------------------------------------------------------------------
    //  Ignored Files
    // ------------------------------------------------------------------------

    @Test
    void testIgnoredUnderscoreFiles() throws IOException {

        final String contents = "CONTENTS";

        // create some accepted, some ignored files

        File child1 = TempDirUtils.newFile(temporaryFolder, "dataFile1.txt");
        File child2 = TempDirUtils.newFile(temporaryFolder, "another_file.bin");
        File luigiFile = TempDirUtils.newFile(temporaryFolder, "_luigi");
        File success = TempDirUtils.newFile(temporaryFolder, "_SUCCESS");

        createTempFiles(
                contents.getBytes(ConfigConstants.DEFAULT_CHARSET),
                child1,
                child2,
                luigiFile,
                success);

        // test that only the valid files are accepted

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(temporaryFolder.toFile().getPath());
        format.configure(new Configuration());
        FileInputSplit[] splits = format.createInputSplits(1);

        assertThat(splits).hasSize(2);

        final URI uri1 = splits[0].getPath().toUri();
        final URI uri2 = splits[1].getPath().toUri();

        final URI childUri1 = child1.toURI();
        final URI childUri2 = child2.toURI();

        assertThat(
                        (uri1.equals(childUri1) && uri2.equals(childUri2))
                                || (uri1.equals(childUri2) && uri2.equals(childUri1)))
                .isTrue();
    }

    @Test
    void testExcludeFiles() throws IOException {

        final String contents = "CONTENTS";

        // create some accepted, some ignored files

        File child1 = TempDirUtils.newFile(temporaryFolder, "dataFile1.txt");
        File child2 = TempDirUtils.newFile(temporaryFolder, "another_file.bin");

        File[] files = {child1, child2};

        createTempFiles(contents.getBytes(ConfigConstants.DEFAULT_CHARSET), files);

        // test that only the valid files are accepted

        Configuration configuration = new Configuration();

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(temporaryFolder.toFile().getPath());
        format.configure(configuration);
        format.setFilesFilter(
                new GlobFilePathFilter(
                        Collections.singletonList("**"),
                        Collections.singletonList("**/another_file.bin")));
        FileInputSplit[] splits = format.createInputSplits(1);

        assertThat(splits).hasSize(1);

        final URI uri1 = splits[0].getPath().toUri();

        final URI childUri1 = child1.toURI();

        assertThat(childUri1).isEqualTo(uri1);
    }

    @Test
    void testReadMultiplePatterns() throws Exception {
        final String contents = "CONTENTS";

        // create some accepted, some ignored files

        File child1 = TempDirUtils.newFile(temporaryFolder, "dataFile1.txt");
        File child2 = TempDirUtils.newFile(temporaryFolder, "another_file.bin");
        createTempFiles(contents.getBytes(ConfigConstants.DEFAULT_CHARSET), child1, child2);

        // test that only the valid files are accepted

        Configuration configuration = new Configuration();

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(temporaryFolder.toFile().getPath());
        format.configure(configuration);
        format.setFilesFilter(
                new GlobFilePathFilter(
                        Collections.singletonList("**"),
                        Arrays.asList("**/another_file.bin", "**/dataFile1.txt")));
        FileInputSplit[] splits = format.createInputSplits(1);

        assertThat(splits.length).isZero();
    }

    @Test
    void testGetStatsIgnoredUnderscoreFiles() throws IOException {

        final int SIZE = 2048;
        final long TOTAL = 2 * SIZE;

        // create two accepted and two ignored files
        File child1 = TempDirUtils.newFile(temporaryFolder, "dataFile1.txt");
        File child2 = TempDirUtils.newFile(temporaryFolder, "another_file.bin");
        File luigiFile = TempDirUtils.newFile(temporaryFolder, "_luigi");
        File success = TempDirUtils.newFile(temporaryFolder, "_SUCCESS");

        createTempFiles(new byte[SIZE], child1, child2, luigiFile, success);

        final DummyFileInputFormat format = new DummyFileInputFormat();
        format.setFilePath(temporaryFolder.toFile().getPath());
        format.configure(new Configuration());

        // check that only valid files are used for statistics computation
        BaseStatistics stats = format.getStatistics(null);
        assertThat(stats.getTotalInputSize()).isEqualTo(TOTAL);
    }

    // ------------------------------------------------------------------------
    //  Stream Decoration
    // ------------------------------------------------------------------------

    @Test
    void testDecorateInputStream() throws IOException {
        // create temporary file with 3 blocks
        final File tempFile = File.createTempFile("input-stream-decoration-test", "tmp");
        tempFile.deleteOnExit();
        final int blockSize = 8;
        final int numBlocks = 3;
        FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
        for (int i = 0; i < blockSize * numBlocks; i++) {
            fileOutputStream.write(new byte[] {(byte) i});
        }
        fileOutputStream.close();

        final Configuration config = new Configuration();

        final FileInputFormat<byte[]> inputFormat = new MyDecoratedInputFormat();
        inputFormat.setFilePath(tempFile.toURI().toString());

        inputFormat.configure(config);
        inputFormat.openInputFormat();

        FileInputSplit[] inputSplits = inputFormat.createInputSplits(3);

        byte[] bytes = null;
        byte prev = 0;
        for (FileInputSplit inputSplit : inputSplits) {
            inputFormat.open(inputSplit);
            while (!inputFormat.reachedEnd()) {
                if ((bytes = inputFormat.nextRecord(bytes)) != null) {
                    assertThat(bytes).isEqualTo(new byte[] {--prev});
                }
            }
        }

        inputFormat.closeInputFormat();
    }

    // ------------------------------------------------------------------------

    private void createTempFiles(byte[] contents, File... files) throws IOException {
        for (File child : files) {
            child.deleteOnExit();

            try (BufferedOutputStream out =
                    new BufferedOutputStream(Files.newOutputStream(child.toPath()))) {
                out.write(contents);
            }
        }
    }

    private static class DummyFileInputFormat extends FileInputFormat<IntValue> {
        private static final long serialVersionUID = 1L;
        private boolean compressedRead = false;

        @Override
        public boolean reachedEnd() {
            return true;
        }

        @Override
        public IntValue nextRecord(IntValue record) {
            return null;
        }

        @Override
        public void open(FileInputSplit split) throws IOException {
            compressedRead = false;
            super.open(split);
        }

        @Override
        protected FSDataInputStream decorateInputStream(
                FSDataInputStream inputStream, FileInputSplit fileSplit) {
            compressedRead =
                    getInflaterInputStreamFactory(
                                    extractFileExtension(fileSplit.getPath().getName()))
                            != null;
            return inputStream;
        }
    }

    private class MultiDummyFileInputFormat extends DummyFileInputFormat {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean supportsMultiPaths() {
            return true;
        }
    }

    private static final class MyDecoratedInputFormat extends FileInputFormat<byte[]> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean reachedEnd() throws IOException {
            return this.stream.getPos() >= this.splitStart + this.splitLength;
        }

        @Override
        public byte[] nextRecord(byte[] reuse) throws IOException {
            int read = this.stream.read();
            if (read == -1) throw new IllegalStateException();
            return new byte[] {(byte) read};
        }

        @Override
        protected FSDataInputStream decorateInputStream(
                FSDataInputStream inputStream, FileInputSplit fileSplit) throws Throwable {
            inputStream = super.decorateInputStream(inputStream, fileSplit);
            return new InputStreamFSInputWrapper(new InvertedInputStream(inputStream));
        }
    }

    private static final class InvertedInputStream extends InputStream {

        private final InputStream originalStream;

        private InvertedInputStream(InputStream originalStream) {
            this.originalStream = originalStream;
        }

        @Override
        public int read() throws IOException {
            int read = this.originalStream.read();
            return read == -1 ? -1 : (~read & 0xFF);
        }

        @Override
        public int available() throws IOException {
            return this.originalStream.available();
        }
    }
}
