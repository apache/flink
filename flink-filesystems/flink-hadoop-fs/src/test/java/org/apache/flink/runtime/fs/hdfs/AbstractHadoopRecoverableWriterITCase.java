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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Abstract integration test class for implementations of hadoop recoverable writer. */
public abstract class AbstractHadoopRecoverableWriterITCase {
    // ----------------------- Test Specific configuration -----------------------

    private static final Random RND = new Random();

    protected static Path basePath;

    private static FileSystem fileSystem;

    // this is set for every test @BeforeEach
    protected Path basePathForTest;

    // ----------------------- Test Data to be used -----------------------

    private static final String testData1 = "THIS IS A TEST 1.";
    private static final String testData2 = "THIS IS A TEST 2.";
    private static final String testData3 = "THIS IS A TEST 3.";

    protected static final String BIG_CHUNK_DATA_PATTERN = testData1;
    protected static String bigDataChunk;

    // ----------------------- Test Lifecycle -----------------------

    protected static boolean skipped = true;

    @TempDir protected static File tempFolder;

    @AfterAll
    static void cleanUp() throws Exception {
        if (!skipped) {
            getFileSystem().delete(basePath, true);
        }
        FileSystem.initialize(new Configuration());
    }

    @BeforeEach
    void prepare() throws Exception {
        basePathForTest = new Path(basePath, StringUtils.getRandomString(RND, 16, 16, 'a', 'z'));

        cleanupLocalDir();
    }

    protected abstract String getLocalTmpDir() throws Exception;

    protected abstract String getIncompleteObjectName(
            RecoverableWriter.ResumeRecoverable recoverable);

    private void cleanupLocalDir() throws Exception {
        final String defaultTmpDir = getLocalTmpDir();
        final java.nio.file.Path defaultTmpPath = Paths.get(defaultTmpDir);

        if (Files.exists(defaultTmpPath)) {
            try (Stream<java.nio.file.Path> files = Files.list(defaultTmpPath)) {
                files.forEach(
                        p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            }
        } else {
            Files.createDirectory(defaultTmpPath);
        }
    }

    @AfterEach
    void cleanupAndCheckTmpCleanup() throws Exception {
        final String defaultTmpDir = getLocalTmpDir();
        final java.nio.file.Path localTmpDir = Paths.get(defaultTmpDir);

        // delete local tmp dir.
        assertThat(Files.exists(localTmpDir)).isTrue();
        try (Stream<java.nio.file.Path> files = Files.list(localTmpDir)) {
            assertThat(files).isEmpty();
        }
        Files.delete(localTmpDir);

        // delete also object store dir.
        getFileSystem().delete(basePathForTest, true);
    }

    protected static FileSystem getFileSystem() throws Exception {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(basePath.toUri());
        }
        return fileSystem;
    }

    // ----------------------- Test Normal Execution -----------------------

    @Test
    void testCloseWithNoData() throws Exception {
        final RecoverableWriter writer = getRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);

        stream.closeForCommit().commit();
    }

    @Test
    void testCommitAfterNormalClose() throws Exception {
        final RecoverableWriter writer = getRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(bytesOf(testData1));
        stream.closeForCommit().commit();

        assertThat(getContentsOfFile(path)).isEqualTo(testData1);
    }

    @Test
    void testCommitAfterPersist() throws Exception {
        final RecoverableWriter writer = getRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(bytesOf(testData1));
        stream.persist();

        stream.write(bytesOf(testData2));
        stream.closeForCommit().commit();

        assertThat(getContentsOfFile(path)).isEqualTo(testData1 + testData2);
    }

    @Test
    void testCleanupRecoverableState() throws Exception {
        final RecoverableWriter writer = getRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(bytesOf(testData1));
        RecoverableWriter.ResumeRecoverable recoverable = stream.persist();

        stream.closeForCommit().commit();

        // still the data is there as we have not deleted them from the tmp object
        final String content =
                getContentsOfFile(new Path('/' + getIncompleteObjectName(recoverable)));

        assertThat(content).isEqualTo(testData1);

        boolean successfullyDeletedState = writer.cleanupRecoverableState(recoverable);
        assertThat(successfullyDeletedState).isTrue();

        assertThatThrownBy(
                        () -> {
                            int retryTimes = 10;
                            final long delayMs = 1000;
                            // Because the s3 is eventually consistency the s3 file might still be
                            // found after we delete
                            // it.
                            // So we try multi-times to verify that the file was deleted at last.
                            while (retryTimes > 0) {
                                // this should throw the exception as we deleted the file.
                                getContentsOfFile(
                                        new Path('/' + getIncompleteObjectName(recoverable)));
                                retryTimes--;
                                Thread.sleep(delayMs);
                            }
                        })
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testCallingDeleteObjectTwiceDoesNotThroughException() throws Exception {
        final RecoverableWriter writer = getRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(bytesOf(testData1));
        RecoverableWriter.ResumeRecoverable recoverable = stream.persist();

        stream.closeForCommit().commit();

        // still the data is there as we have not deleted them from the tmp object
        final String content =
                getContentsOfFile(new Path('/' + getIncompleteObjectName(recoverable)));

        assertThat(content).isEqualTo(testData1);

        boolean successfullyDeletedState = writer.cleanupRecoverableState(recoverable);
        assertThat(successfullyDeletedState).isTrue();

        boolean unsuccessfulDeletion = writer.cleanupRecoverableState(recoverable);
        assertThat(unsuccessfulDeletion).isFalse();
    }

    // ----------------------- Test Recovery -----------------------

    @Test
    void testCommitAfterRecovery() throws Exception {
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableWriter initWriter = getRecoverableWriter();

        final RecoverableFsDataOutputStream stream = initWriter.open(path);
        stream.write(bytesOf(testData1));

        stream.persist();
        stream.persist();

        // and write some more data
        stream.write(bytesOf(testData2));

        final RecoverableWriter.CommitRecoverable recoverable =
                stream.closeForCommit().getRecoverable();

        final byte[] serializedRecoverable =
                initWriter.getCommitRecoverableSerializer().serialize(recoverable);

        // get a new serializer from a new writer to make sure that no pre-initialized state leaks
        // in.
        final RecoverableWriter newWriter = getRecoverableWriter();

        final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> deserializer =
                newWriter.getCommitRecoverableSerializer();
        final RecoverableWriter.CommitRecoverable recoveredRecoverable =
                deserializer.deserialize(deserializer.getVersion(), serializedRecoverable);

        final RecoverableFsDataOutputStream.Committer committer =
                newWriter.recoverForCommit(recoveredRecoverable);
        committer.commitAfterRecovery();

        assertThat(getContentsOfFile(path)).isEqualTo(testData1 + testData2);
    }

    private static final String INIT_EMPTY_PERSIST = "EMPTY";
    private static final String INTERM_WITH_STATE_PERSIST = "INTERM-STATE";
    private static final String INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST = "INTERM-IMEDIATE";
    private static final String FINAL_WITH_EXTRA_STATE = "FINAL";

    @Test
    void testRecoverWithEmptyState() throws Exception {
        testResumeAfterMultiplePersistWithSmallData(INIT_EMPTY_PERSIST, testData3);
    }

    @Test
    void testRecoverWithState() throws Exception {
        testResumeAfterMultiplePersistWithSmallData(
                INTERM_WITH_STATE_PERSIST, testData1 + testData3);
    }

    @Test
    void testRecoverFromIntermWithoutAdditionalState() throws Exception {
        testResumeAfterMultiplePersistWithSmallData(
                INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST, testData1 + testData3);
    }

    @Test
    void testRecoverAfterMultiplePersistsState() throws Exception {
        testResumeAfterMultiplePersistWithSmallData(
                FINAL_WITH_EXTRA_STATE, testData1 + testData2 + testData3);
    }

    @Test
    void testRecoverWithStateWithMultiPart() throws Exception {
        testResumeAfterMultiplePersistWithMultiPartUploads(
                INTERM_WITH_STATE_PERSIST, bigDataChunk + bigDataChunk);
    }

    @Test
    void testRecoverFromIntermWithoutAdditionalStateWithMultiPart() throws Exception {
        testResumeAfterMultiplePersistWithMultiPartUploads(
                INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST, bigDataChunk + bigDataChunk);
    }

    @Test
    void testRecoverAfterMultiplePersistsStateWithMultiPart() throws Exception {
        testResumeAfterMultiplePersistWithMultiPartUploads(
                FINAL_WITH_EXTRA_STATE, bigDataChunk + bigDataChunk + bigDataChunk);
    }

    private void testResumeAfterMultiplePersistWithSmallData(
            final String persistName, final String expectedFinalContents) throws Exception {
        testResumeAfterMultiplePersist(
                persistName, expectedFinalContents, testData1, testData2, testData3);
    }

    private void testResumeAfterMultiplePersistWithMultiPartUploads(
            final String persistName, final String expectedFinalContents) throws Exception {
        testResumeAfterMultiplePersist(
                persistName, expectedFinalContents, bigDataChunk, bigDataChunk, bigDataChunk);
    }

    private void testResumeAfterMultiplePersist(
            final String persistName,
            final String expectedFinalContents,
            final String firstItemToWrite,
            final String secondItemToWrite,
            final String thirdItemToWrite)
            throws Exception {

        final Path path = new Path(basePathForTest, "part-0");
        final RecoverableWriter initWriter = getRecoverableWriter();

        final Map<String, RecoverableWriter.ResumeRecoverable> recoverables = new HashMap<>(4);
        try (final RecoverableFsDataOutputStream stream = initWriter.open(path)) {
            recoverables.put(INIT_EMPTY_PERSIST, stream.persist());

            stream.write(bytesOf(firstItemToWrite));

            recoverables.put(INTERM_WITH_STATE_PERSIST, stream.persist());
            recoverables.put(INTERM_WITH_NO_ADDITIONAL_STATE_PERSIST, stream.persist());

            // and write some more data
            stream.write(bytesOf(secondItemToWrite));

            recoverables.put(FINAL_WITH_EXTRA_STATE, stream.persist());
        }

        final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> serializer =
                initWriter.getResumeRecoverableSerializer();
        final byte[] serializedRecoverable = serializer.serialize(recoverables.get(persistName));

        // get a new serializer from a new writer to make sure that no pre-initialized state leaks
        // in.
        final RecoverableWriter newWriter = getRecoverableWriter();
        final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> deserializer =
                newWriter.getResumeRecoverableSerializer();
        final RecoverableWriter.ResumeRecoverable recoveredRecoverable =
                deserializer.deserialize(serializer.getVersion(), serializedRecoverable);

        final RecoverableFsDataOutputStream recoveredStream =
                newWriter.recover(recoveredRecoverable);
        recoveredStream.write(bytesOf(thirdItemToWrite));
        recoveredStream.closeForCommit().commit();

        assertThat(getContentsOfFile(path)).isEqualTo(expectedFinalContents);
    }

    // -------------------------- Test Utilities --------------------------

    protected String getContentsOfFile(Path path) throws Exception {
        final StringBuilder builder = new StringBuilder();
        try (FSDataInputStream inStream = getFileSystem().open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
        }
        return builder.toString();
    }

    // ----------------------- Test utilities -----------------------

    protected static String createBigDataChunk(String pattern, long size) {
        final StringBuilder stringBuilder = new StringBuilder();

        int sampleLength = bytesOf(pattern).length;
        int repeats = MathUtils.checkedDownCast(size) / sampleLength + 100;

        for (int i = 0; i < repeats; i++) {
            stringBuilder.append(pattern);
        }
        return stringBuilder.toString();
    }

    protected static byte[] bytesOf(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    protected RecoverableWriter getRecoverableWriter() throws Exception {
        return getFileSystem().createRecoverableWriter();
    }
}
