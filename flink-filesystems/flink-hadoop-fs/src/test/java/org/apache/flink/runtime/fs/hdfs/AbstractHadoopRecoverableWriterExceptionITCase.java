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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Abstract integration test class for implementations of hadoop recoverable writer when exception
 * thrown.
 */
public abstract class AbstractHadoopRecoverableWriterExceptionITCase {

    // ----------------------- Test Specific configuration -----------------------

    private static final Random RND = new Random();

    protected static Path basePath;

    protected static FileSystem fileSystem;

    // this is set for every test @BeforeEach
    protected Path basePathForTest;

    // ----------------------- Test Data to be used -----------------------

    private static final String testData1 = "THIS IS A TEST 1.";
    private static final String testData2 = "THIS IS A TEST 2.";
    private static final String testData3 = "THIS IS A TEST 3.";

    protected static boolean skipped = true;

    @TempDir protected static File tempFolder;

    @AfterAll
    static void cleanUp() throws Exception {
        if (!skipped) {
            getFileSystem().delete(basePath, true);
        }
        FileSystem.initialize(new Configuration());
    }

    protected abstract String getLocalTmpDir() throws Exception;

    @BeforeEach
    void prepare() throws Exception {
        basePathForTest = new Path(basePath, StringUtils.getRandomString(RND, 16, 16, 'a', 'z'));

        final String defaultTmpDir = getLocalTmpDir();
        final java.nio.file.Path path = Paths.get(defaultTmpDir);

        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        getFileSystem().delete(basePathForTest, true);
    }

    protected static FileSystem getFileSystem() throws Exception {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(basePath.toUri());
        }
        return fileSystem;
    }

    @Test
    void testExceptionWritingAfterCloseForCommit() throws Exception {
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream =
                getFileSystem().createRecoverableWriter().open(path);
        stream.write(testData1.getBytes(StandardCharsets.UTF_8));

        stream.closeForCommit().getRecoverable();

        assertThatThrownBy(() -> stream.write(testData2.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    // IMPORTANT FOR THE FOLLOWING TWO TESTS:

    // These tests illustrate a difference in the user-perceived behavior of the different writers.
    // In HDFS this will fail when trying to recover the stream while here is will fail at "commit",
    // i.e.
    // when we try to "publish" the multipart upload and we realize that the MPU is no longer
    // active.

    @Test
    void testResumeAfterCommit() throws Exception {
        final RecoverableWriter writer = getFileSystem().createRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(testData1.getBytes(StandardCharsets.UTF_8));

        final RecoverableWriter.ResumeRecoverable recoverable = stream.persist();
        stream.write(testData2.getBytes(StandardCharsets.UTF_8));

        stream.closeForCommit().commit();

        final RecoverableFsDataOutputStream recoveredStream = writer.recover(recoverable);

        assertThatThrownBy(() -> recoveredStream.closeForCommit().commit())
                .isInstanceOf(IOException.class);
    }

    @Test
    void testResumeWithWrongOffset() throws Exception {
        // this is a rather unrealistic scenario, but it is to trigger
        // truncation of the file and try to resume with missing data.

        final RecoverableWriter writer = getFileSystem().createRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(testData1.getBytes(StandardCharsets.UTF_8));

        final RecoverableWriter.ResumeRecoverable recoverable1 = stream.persist();
        stream.write(testData2.getBytes(StandardCharsets.UTF_8));

        final RecoverableWriter.ResumeRecoverable recoverable2 = stream.persist();
        stream.write(testData3.getBytes(StandardCharsets.UTF_8));

        final RecoverableFsDataOutputStream recoveredStream = writer.recover(recoverable1);
        recoveredStream.closeForCommit().commit();

        // this should throw an exception
        final RecoverableFsDataOutputStream newRecoveredStream = writer.recover(recoverable2);

        assertThatThrownBy(() -> newRecoveredStream.closeForCommit().commit())
                .isInstanceOf(IOException.class);
    }
}
