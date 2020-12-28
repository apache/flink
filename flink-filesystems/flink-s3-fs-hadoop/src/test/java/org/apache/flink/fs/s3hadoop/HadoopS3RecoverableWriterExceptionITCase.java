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

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3.common.FlinkS3FileSystem;
import org.apache.flink.testutils.s3.S3TestCredentials;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.MAX_CONCURRENT_UPLOADS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.PART_UPLOAD_MIN_SIZE;

/**
 * Tests for exception throwing in the {@link
 * org.apache.flink.fs.s3.common.writer.S3RecoverableWriter S3RecoverableWriter}.
 */
public class HadoopS3RecoverableWriterExceptionITCase extends TestLogger {

    // ----------------------- S3 general configuration -----------------------

    private static final long PART_UPLOAD_MIN_SIZE_VALUE = 7L << 20;
    private static final int MAX_CONCURRENT_UPLOADS_VALUE = 2;

    // ----------------------- Test Specific configuration -----------------------

    private static final Random RND = new Random();

    private static Path basePath;

    private static FlinkS3FileSystem fileSystem;

    // this is set for every test @Before
    private Path basePathForTest;

    // ----------------------- Test Data to be used -----------------------

    private static final String testData1 = "THIS IS A TEST 1.";
    private static final String testData2 = "THIS IS A TEST 2.";
    private static final String testData3 = "THIS IS A TEST 3.";

    private static boolean skipped = true;

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @BeforeClass
    public static void checkCredentialsAndSetup() throws IOException {
        // check whether credentials exist
        S3TestCredentials.assumeCredentialsAvailable();

        basePath = new Path(S3TestCredentials.getTestBucketUri() + "tests-" + UUID.randomUUID());

        // initialize configuration with valid credentials
        final Configuration conf = new Configuration();
        conf.setString("s3.access.key", S3TestCredentials.getS3AccessKey());
        conf.setString("s3.secret.key", S3TestCredentials.getS3SecretKey());

        conf.setLong(PART_UPLOAD_MIN_SIZE, PART_UPLOAD_MIN_SIZE_VALUE);
        conf.setInteger(MAX_CONCURRENT_UPLOADS, MAX_CONCURRENT_UPLOADS_VALUE);

        final String defaultTmpDir = TEMP_FOLDER.getRoot().getAbsolutePath() + "s3_tmp_dir";
        conf.setString(CoreOptions.TMP_DIRS, defaultTmpDir);

        FileSystem.initialize(conf);

        skipped = false;
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        if (!skipped) {
            getFileSystem().delete(basePath, true);
        }
        FileSystem.initialize(new Configuration());
    }

    @Before
    public void prepare() throws Exception {
        basePathForTest = new Path(basePath, StringUtils.getRandomString(RND, 16, 16, 'a', 'z'));

        final String defaultTmpDir = getFileSystem().getLocalTmpDir();
        final java.nio.file.Path path = Paths.get(defaultTmpDir);

        if (!Files.exists(path)) {
            Files.createDirectory(path);
        }
    }

    @After
    public void cleanup() throws Exception {
        getFileSystem().delete(basePathForTest, true);
    }

    private static FlinkS3FileSystem getFileSystem() throws Exception {
        if (fileSystem == null) {
            fileSystem = (FlinkS3FileSystem) FileSystem.get(basePath.toUri());
        }
        return fileSystem;
    }

    @Test(expected = IOException.class)
    public void testExceptionWritingAfterCloseForCommit() throws Exception {
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream =
                getFileSystem().createRecoverableWriter().open(path);
        stream.write(testData1.getBytes(StandardCharsets.UTF_8));

        stream.closeForCommit().getRecoverable();
        stream.write(testData2.getBytes(StandardCharsets.UTF_8));
    }

    // IMPORTANT FOR THE FOLLOWING TWO TESTS:

    // These tests illustrate a difference in the user-perceived behavior of the different writers.
    // In HDFS this will fail when trying to recover the stream while here is will fail at "commit",
    // i.e.
    // when we try to "publish" the multipart upload and we realize that the MPU is no longer
    // active.

    @Test(expected = IOException.class)
    public void testResumeAfterCommit() throws Exception {
        final RecoverableWriter writer = getFileSystem().createRecoverableWriter();
        final Path path = new Path(basePathForTest, "part-0");

        final RecoverableFsDataOutputStream stream = writer.open(path);
        stream.write(testData1.getBytes(StandardCharsets.UTF_8));

        final RecoverableWriter.ResumeRecoverable recoverable = stream.persist();
        stream.write(testData2.getBytes(StandardCharsets.UTF_8));

        stream.closeForCommit().commit();

        final RecoverableFsDataOutputStream recoveredStream = writer.recover(recoverable);
        recoveredStream.closeForCommit().commit();
    }

    @Test(expected = IOException.class)
    public void testResumeWithWrongOffset() throws Exception {
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
        newRecoveredStream.closeForCommit().commit();
    }
}
