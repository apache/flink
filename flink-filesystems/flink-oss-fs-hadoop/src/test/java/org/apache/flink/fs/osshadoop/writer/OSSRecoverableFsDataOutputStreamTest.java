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

package org.apache.flink.fs.osshadoop.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.osshadoop.OSSTestUtils;
import org.apache.flink.testutils.oss.OSSTestCredentials;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link OSSRecoverableFsDataOutputStream}. */
class OSSRecoverableFsDataOutputStreamTest {

    private static Path basePath;

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    private FileSystem fs;

    private static final String TEST_OBJECT_NAME_PREFIX = "TEST-OBJECT-";

    private Path objectPath;

    private RecoverableWriter writer;

    private RecoverableFsDataOutputStream fsDataOutputStream;

    @TempDir public static File temporaryFolder;

    @BeforeEach
    void before() throws IOException {
        OSSTestCredentials.assumeCredentialsAvailable();

        final Configuration conf = new Configuration();
        conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
        FileSystem.initialize(conf);

        basePath = new Path(OSSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
        fs = basePath.getFileSystem();
        writer = fs.createRecoverableWriter();

        objectPath = new Path(basePath + "/" + TEST_OBJECT_NAME_PREFIX + UUID.randomUUID());

        fsDataOutputStream = writer.open(objectPath);
    }

    @Test
    void testRegularDataWritten() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);

        fsDataOutputStream.write(part);

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, part);
    }

    @Test
    void testNoDataWritten() throws IOException {
        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        // will not create empty object
        assertThat(fs.exists(objectPath)).isFalse();
    }

    @Test
    void testCloseForCommitOnClosedStreamShouldFail() throws IOException {
        fsDataOutputStream.closeForCommit().commit();
        assertThatThrownBy(() -> fsDataOutputStream.closeForCommit().commit())
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCloseWithoutCommit() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);

        fsDataOutputStream.write(part);

        fsDataOutputStream.close();

        // close without commit will not upload current part
        assertThat(fs.exists(objectPath)).isFalse();
    }

    @Test
    void testWriteLargeFile() throws IOException {
        List<byte[]> buffers = OSSTestUtils.generateRandomBuffer(50 * 1024 * 1024, 10 * 104 * 1024);
        for (byte[] buffer : buffers) {
            fsDataOutputStream.write(buffer);
        }

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, buffers);
    }

    @Test
    void testConcatWrites() throws IOException {
        fsDataOutputStream.write(OSSTestUtils.bytesOf("hello", 5));
        fsDataOutputStream.write(OSSTestUtils.bytesOf(" ", 1));
        fsDataOutputStream.write(OSSTestUtils.bytesOf("world", 5));

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, OSSTestUtils.bytesOf("hello world", 11));
    }

    @Test
    void testRegularRecovery() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);
        fsDataOutputStream.write(part);

        RecoverableWriter.ResumeRecoverable recoverable = fsDataOutputStream.persist();

        fsDataOutputStream = writer.recover(recoverable);

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, part);
    }

    @Test
    void testContinuousPersistWithoutWrites() throws IOException {
        fsDataOutputStream.write(OSSTestUtils.bytesOf("hello", 5));

        fsDataOutputStream.persist();
        fsDataOutputStream.persist();
        fsDataOutputStream.persist();
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf(" ", 1));
        fsDataOutputStream.write(OSSTestUtils.bytesOf("world", 5));

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, OSSTestUtils.bytesOf("hello world", 11));
    }

    @Test
    void testWriteSmallDataAndPersist() throws IOException {
        fsDataOutputStream.write(OSSTestUtils.bytesOf("h", 1));
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf("e", 1));
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf("l", 1));
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf("l", 1));
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf("o", 1));
        fsDataOutputStream.persist();

        fsDataOutputStream.write(OSSTestUtils.bytesOf(" ", 1));
        fsDataOutputStream.write(OSSTestUtils.bytesOf("world", 5));
        fsDataOutputStream.persist();

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, OSSTestUtils.bytesOf("hello world", 11));
    }

    @Test
    void testWriteBigDataAndPersist() throws IOException {
        List<byte[]> buffers = OSSTestUtils.generateRandomBuffer(50 * 1024 * 1024, 10 * 104 * 1024);
        for (byte[] buffer : buffers) {
            fsDataOutputStream.write(buffer);
            fsDataOutputStream.persist();
        }

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        OSSTestUtils.objectContentEquals(fs, objectPath, buffers);
    }

    @Test
    void testDataWrittenAfterRecovery() throws IOException {
        final byte[] part = OSSTestUtils.bytesOf("hello world", 1024 * 1024);
        fsDataOutputStream.write(part);

        RecoverableWriter.ResumeRecoverable recoverable = fsDataOutputStream.persist();

        fsDataOutputStream = writer.recover(recoverable);

        List<byte[]> buffers = OSSTestUtils.generateRandomBuffer(50 * 1024 * 1024, 10 * 104 * 1024);
        for (byte[] buffer : buffers) {
            fsDataOutputStream.write(buffer);
        }

        RecoverableFsDataOutputStream.Committer committer = fsDataOutputStream.closeForCommit();
        committer.commit();

        buffers.add(0, part);

        OSSTestUtils.objectContentEquals(fs, objectPath, buffers);
    }

    @AfterEach
    void after() throws IOException {
        try {
            if (fs != null) {
                fs.delete(basePath, true);
            }
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }
}
