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
import org.apache.flink.configuration.MemorySize;
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
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** Tests of various write and recovery scenarios. */
@RunWith(Parameterized.class)
public class GSFileSystemScenarioTest {

    /* The temporary bucket name to use. */
    @Parameterized.Parameter(value = 0)
    public String temporaryBucketName;

    /* The chunk size to use for writing to GCS. */
    @Parameterized.Parameter(value = 1)
    public MemorySize writeChunkSize;

    @Parameterized.Parameters(name = "temporaryBucketName={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    // no specified bucket, no chunk size
                    {null, null},
                    // specified bucket, no chunk size
                    {"temporary-bucket", null},
                    // no specified bucket, valid chunk size
                    {null, MemorySize.parse("512KB")},
                    // specified bucket, valid chunk size
                    {"temporary-bucket", MemorySize.parse("512KB")},
                    // no specified bucket, invalid chunk size
                    {null, MemorySize.parse("257KB")},
                    // specified bucket, invalid chunk size
                    {"temporary-bucket", MemorySize.parse("0KB")},
                    // no specified bucket, invalid zero chunk size
                    {null, MemorySize.parse("257KB")},
                    // specified bucket, invalid zero chunk size
                    {"temporary-bucket", MemorySize.parse("0KB")},
                });
    }

    private Random random;

    private MockBlobStorage storage;

    private Configuration flinkConfig;

    private GSBlobIdentifier blobIdentifier;

    private Path path;

    private boolean writeChunkSizeIsValid;

    @Before
    public void before() {

        random = new Random(TestUtils.RANDOM_SEED);

        // construct the flink configuration
        flinkConfig = new Configuration();
        if (!StringUtils.isNullOrWhitespaceOnly(temporaryBucketName)) {
            flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, temporaryBucketName);
        }
        if (writeChunkSize != null) {
            flinkConfig.set(GSFileSystemOptions.WRITER_CHUNK_SIZE, writeChunkSize);
        }

        if (writeChunkSize == null) {
            // unspecified chunk size is valid
            writeChunkSizeIsValid = true;
        } else {
            // chunk size that is > 0 and multiple of 256KB is valid
            long byteCount = writeChunkSize.getBytes();
            writeChunkSizeIsValid = (byteCount > 0) && (byteCount % (256 * 1024) == 0);
        }

        storage = new MockBlobStorage();

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

        // only run the test for valid chunk sizes
        assumeTrue(writeChunkSizeIsValid);

        // create the options and writer
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        RecoverableWriter writer = new GSRecoverableWriter(storage, options);

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

        // only run the test for valid chunk sizes
        assumeTrue(writeChunkSizeIsValid);

        // create the options and writer
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        RecoverableWriter writer = new GSRecoverableWriter(storage, options);

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

        // only run the test for valid chunk sizes
        assumeTrue(writeChunkSizeIsValid);

        // create the options and writer
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        RecoverableWriter writer = new GSRecoverableWriter(storage, options);

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

    @Test
    public void invalidChunkSizeTest() {

        // only run the test for invalid chunk sizes
        assumeFalse(writeChunkSizeIsValid);

        // create the options and writer
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
                });
    }
}
