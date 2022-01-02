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

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.fs.gs.writer.GSRecoverableWriter;
import org.apache.flink.util.StringUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests of various write and recovery scenarios. */
@RunWith(Parameterized.class)
public class GSFileSystemScenarioTest {

    /* The temporary bucket name to use. */
    @Parameterized.Parameter(value = 0)
    public String temporaryBucketName;

    @Parameterized.Parameters(name = "temporaryBucketName={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // no specified bucket
                    {null},
                    // specified bucket
                    {"temporary-bucket"}
                });
    }

    private Random random;

    private MockBlobStorage storage;

    private GSRecoverableWriter writer;

    private GSBlobIdentifier blobIdentifier;

    private Path path;

    @Before
    public void before() {

        random = new Random(TestUtils.RANDOM_SEED);

        // construct the flink configuration
        Configuration flinkConfig = new Configuration();
        if (!StringUtils.isNullOrWhitespaceOnly(temporaryBucketName)) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }

        storage = new MockBlobStorage();
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        writer = new GSRecoverableWriter(storage, options);

        blobIdentifier = new GSBlobIdentifier("foo", "bar");
        path =
                new Path(
                        String.format(
                                "gs://%s/%s",
                                blobIdentifier.bucketName, blobIdentifier.objectName));
    }

    /* Test writing a single array of bytes to a stream. */
    @Test
    public void simpleWriteTest() throws IOException {

        // create a stream and write some random bytes to it
        RecoverableFsDataOutputStream stream = writer.open(path);
        byte[] data = new byte[128];
        random.nextBytes(data);
        stream.write(data);

        // close for commit
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();

        // there should be a single blob now, in the specified temporary bucket or, if no temporary
        // bucket
        // specified, in the final bucket
        assertEquals(1, storage.blobs.size());
        GSBlobIdentifier temporaryBlobIdentifier =
                (GSBlobIdentifier) storage.blobs.keySet().toArray()[0];
        String expectedTemporaryBucket =
                StringUtils.isNullOrWhitespaceOnly(temporaryBucketName)
                        ? blobIdentifier.bucketName
                        : temporaryBucketName;
        assertEquals(expectedTemporaryBucket, temporaryBlobIdentifier.bucketName);

        // commit
        committer.commit();

        // there should be exactly one blob after commit, with the expected contents.
        // all temporary blobs should be removed.
        assertEquals(1, storage.blobs.size());
        MockBlobStorage.BlobValue blobValue = storage.blobs.get(blobIdentifier);
        assertArrayEquals(data, blobValue.content);
    }

    /* Test writing multiple arrays of bytes to a stream. */
    @Test
    public void compoundWriteTest() throws IOException {

        // create a stream
        RecoverableFsDataOutputStream stream = writer.open(path);

        // write 10 arrays of bytes
        final int writeCount = 10;

        // write multiple arrays of bytes to it
        try (ByteArrayOutputStream expectedData = new ByteArrayOutputStream()) {
            for (int i = 0; i < writeCount; i++) {
                byte[] data = new byte[128];
                random.nextBytes(data);
                stream.write(data);
                expectedData.write(data);
            }

            // close for commit and commit
            RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();
            committer.commit();

            // there should be exactly one blob after commit, with the expected contents.
            // all temporary blobs should be removed.
            assertEquals(1, storage.blobs.size());
            MockBlobStorage.BlobValue blobValue = storage.blobs.get(blobIdentifier);
            assertArrayEquals(expectedData.toByteArray(), blobValue.content);
        }
    }

    /* Test writing multiple arrays of bytes to a stream. */
    @Test
    public void compoundWriteTestWithRestore() throws IOException {

        // create a stream
        RecoverableFsDataOutputStream stream = writer.open(path);

        // write 10 arrays of bytes, but create a restore point after 5 and
        // confirm that we have the proper data after restoring
        final int writeCount = 10;
        final int commitCount = 5;

        // write multiple arrays of bytes to it
        RecoverableWriter.ResumeRecoverable resumeRecoverable = null;
        try (ByteArrayOutputStream expectedData = new ByteArrayOutputStream()) {
            for (int i = 0; i < writeCount; i++) {
                byte[] data = new byte[128];
                random.nextBytes(data);
                stream.write(data);

                // if this is a write we expected to be committed, add the byte
                // array to the expected data
                if (i < commitCount) {
                    expectedData.write(data);
                }

                // capture a resume recoverable at the proper point
                if (i == (commitCount - 1)) {
                    resumeRecoverable = stream.persist();
                }
            }

            // recover to the commit point
            stream = writer.recover(resumeRecoverable);

            // close for commit and commit
            RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();
            committer.commit();

            // there should be exactly one blob after commit, with the expected contents.
            // all temporary blobs should be removed.
            assertEquals(1, storage.blobs.size());
            MockBlobStorage.BlobValue blobValue = storage.blobs.get(blobIdentifier);
            assertArrayEquals(expectedData.toByteArray(), blobValue.content);
        }
    }
}
