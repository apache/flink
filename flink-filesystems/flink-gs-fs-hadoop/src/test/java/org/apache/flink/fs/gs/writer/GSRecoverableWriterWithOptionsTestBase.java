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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.MockBlobStorage;
import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Test recoverable writer. */
public abstract class GSRecoverableWriterWithOptionsTestBase {

    private static final Random RANDOM = new Random(601289);

    private static final int DATA_SET_SIZE = 20;

    private static final int DATA_MAX_LENGTH = 1024;

    private static final String FINAL_BUCKET_NAME = "bucket";

    private static final String FINAL_OBJECT_NAME = "foo/bar";

    private static final BlobId FINAL_BLOB_ID = BlobId.of(FINAL_BUCKET_NAME, FINAL_OBJECT_NAME);

    private static final URI FINAL_OBJECT_URI = URI.create("gs://bucket/foo/bar");

    private final GSFileSystemOptions options;

    private final String temporaryBucketName;

    private final String temporaryObjectPrefix;

    /**
     * Generate three datasets, each of dataSetSize data chunks of up to size DATA_MAX_LENGTH. We
     * use these sets of data in various ways in the tests.
     */
    private final byte[][][] dataSets;

    protected GSRecoverableWriterWithOptionsTestBase(
            int seed, int dataSetCount, GSFileSystemOptions options) {

        // ensure that we have a minimum count of datasets
        Preconditions.checkArgument(dataSetCount >= 10);

        this.options = options;
        this.temporaryBucketName =
                options.writerTemporaryBucketName.isEmpty()
                        ? FINAL_BUCKET_NAME
                        : options.writerTemporaryBucketName;
        this.temporaryObjectPrefix = options.writerTemporaryObjectPrefix;

        // populate the datasets
        this.dataSets = new byte[dataSetCount][][];
        for (int i = 0; i < dataSets.length; i++) {
            dataSets[i] = new byte[DATA_SET_SIZE][];
            for (int j = 0; j < DATA_SET_SIZE; j++) {
                int dataLength = RANDOM.nextInt(DATA_MAX_LENGTH) + 1;
                dataSets[i][j] = new byte[dataLength];
                RANDOM.nextBytes(dataSets[i][j]);
            }
        }
    }

    // these are reset for each test
    private MockBlobStorage blobStorage;
    private RecoverableWriter recoverableWriter;
    private GSRecoverableFsDataOutputStream stream;

    @Before
    public void before() throws IOException {
        blobStorage = new MockBlobStorage();
        recoverableWriter = new GSRecoverableWriter(blobStorage, options);
        stream =
                (GSRecoverableFsDataOutputStream)
                        recoverableWriter.open(new Path(FINAL_OBJECT_URI));
    }

    /**
     * This tests that writing a blob consisting of the contents of one array write and one byte
     * write succeeds.
     *
     * @throws IOException On underlying failure
     */
    @Test
    public void shouldWriteSimpleBlob() throws IOException {

        // for this test, we'll just write the first chunk of the first data set plus a single byte
        ByteArrayOutputStream expectedBlobData = new ByteArrayOutputStream();
        expectedBlobData.write(dataSets[0][0]);
        int byteValue = 56;
        expectedBlobData.write(byteValue);
        byte[] expectedBlobBytes = expectedBlobData.toByteArray();

        // write to the output stream
        stream.write(dataSets[0][0]);
        stream.write(byteValue);

        // check that the chunk size was properly initialized on the write channel. the chunk size
        // in the channel should match the options, i.e. either both should be null or both should
        // be the same non-null value
        GSChecksumWriteChannel checksumWriteChannel = stream.currentWriteChannel;
        assertNotNull(checksumWriteChannel);
        MockBlobStorage.WriteChannel mockWriteChannel =
                (MockBlobStorage.WriteChannel) checksumWriteChannel.writeChannel;
        assertEquals(options.writerChunkSize, mockWriteChannel.chunkSize);

        // get the committer
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();

        // we expect one blob at this point, which should be in the same bucket as the final blob
        // and with the expected prefix
        assertEquals(1, blobStorage.blobs.size());
        BlobId blobId = (BlobId) blobStorage.blobs.keySet().toArray()[0];
        assertEquals(temporaryBucketName, blobId.getBucket());
        assertTrue(blobId.getName().startsWith(temporaryObjectPrefix));
        assertEquals(expectedBlobBytes.length, stream.getPos());

        // commit the write, which will also clean up the temporary objects
        committer.commit();

        // we expect exactly one blob -- the final blob -- with the proper content and type
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(FINAL_BLOB_ID);
        assertNotNull(blobValue);
        assertEquals(options.writerContentType, blobValue.contentType);
        assertArrayEquals(expectedBlobBytes, blobValue.content);
    }

    @Test
    public void shouldWriteMultiPartBlob() throws IOException {

        // for this test, we'll write all the chunks of the first data set
        ByteArrayOutputStream expectedBlobData = new ByteArrayOutputStream();
        for (byte[] content : dataSets[0]) {
            expectedBlobData.write(content);
        }
        byte[] expectedBlobBytes = expectedBlobData.toByteArray();

        // create the output stream and write to it, get the committer
        for (byte[] content : dataSets[0]) {
            stream.write(content);
        }
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();

        // we expect one blob at this point, which should be in the same bucket as the final blob
        // and with
        // the expected prefix
        assertEquals(1, blobStorage.blobs.size());
        BlobId blobId = (BlobId) blobStorage.blobs.keySet().toArray()[0];
        assertEquals(temporaryBucketName, blobId.getBucket());
        assertTrue(blobId.getName().startsWith(temporaryObjectPrefix));
        assertEquals(expectedBlobBytes.length, stream.getPos());

        // commit the write, which will also clean up the temporary objects
        committer.commit();

        // we expect exactly one blob -- the final blob -- with the proper content and type
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(FINAL_BLOB_ID);
        assertNotNull(blobValue);
        assertEquals(options.writerContentType, blobValue.contentType);
        assertArrayEquals(expectedBlobBytes, blobValue.content);
    }

    @Test
    public void shouldWriteMultiPartBlobWithPersist() throws IOException {

        // for this test, we'll write all the chunks of all data sets
        ByteArrayOutputStream expectedBlobData = new ByteArrayOutputStream();
        for (byte[][] dataSet : dataSets) {
            for (byte[] content : dataSet) {
                expectedBlobData.write(content);
            }
        }
        byte[] expectedBlobBytes = expectedBlobData.toByteArray();

        // process each data set, persisting after each one
        for (byte[][] dataSet : dataSets) {
            for (byte[] content : dataSet) {
                stream.write(content);
            }
            stream.persist();
        }

        // commit
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();

        // we expect one blob at this point for each dataset, which should all be in the same bucket
        // as the final blob and with the expected prefix
        assertEquals(dataSets.length, blobStorage.blobs.size());
        for (BlobId blobId : blobStorage.blobs.keySet()) {
            assertEquals(temporaryBucketName, blobId.getBucket());
            assertTrue(blobId.getName().startsWith(temporaryObjectPrefix));
        }
        assertEquals(expectedBlobBytes.length, stream.getPos());

        // commit the write, which will also clean up the temporary objects
        committer.commit();

        // we expect exactly one blob -- the final blob -- with the proper content and type
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(FINAL_BLOB_ID);
        assertNotNull(blobValue);
        assertEquals(options.writerContentType, blobValue.contentType);
        assertArrayEquals(expectedBlobBytes, blobValue.content);
    }

    @Test
    public void shouldWriteMultiPartBlobWithPersistAndRecovery() throws IOException {

        // for this test, we'll write all the chunks of all data sets
        ByteArrayOutputStream expectedBlobData = new ByteArrayOutputStream();
        for (byte[][] dataSet : dataSets) {
            for (byte[] content : dataSet) {
                expectedBlobData.write(content);
            }
        }
        byte[] expectedBlobBytes = expectedBlobData.toByteArray();

        // figure out where to do the persist/recovery test, i.e. halfway through
        int recoverToIndex = dataSets.length / 2;
        int writePastRecoveryPointCount = 2;

        // write and persist up to the recovery point
        GSRecoverableWriterState state = null;
        for (int i = 0; i <= recoverToIndex; i++) {
            for (byte[] content : dataSets[i]) {
                stream.write(content);
            }
            state = (GSRecoverableWriterState) stream.persist();
        }

        // write the extra data sets (these are the writes that will be discarded on recovery
        int lastIndex = recoverToIndex + 1 + writePastRecoveryPointCount;
        for (int i = recoverToIndex + 1; i < lastIndex; i++) {
            for (byte[] content : dataSets[i]) {
                stream.write(content);
            }
            stream.persist();
        }

        // assume an error happens here and that we need to recover back to the recovery point
        assertNotNull(state);
        assertTrue(recoverableWriter.supportsResume());
        RecoverableFsDataOutputStream recoveredStream = recoverableWriter.recover(state);

        // write the datasets to the end, starting immediately past the recovery point
        for (int i = recoverToIndex + 1; i < dataSets.length; i++) {
            for (byte[] content : dataSets[i]) {
                recoveredStream.write(content);
            }
            recoveredStream.persist();
        }

        // validate the recovered stream's position
        assertEquals(expectedBlobBytes.length, recoveredStream.getPos());

        // commit
        RecoverableFsDataOutputStream.Committer committer = recoveredStream.closeForCommit();

        // we expect one blob at this point for each dataset, plus one extra for each
        // write past the recovery point. all of these should all be in the proper bucket with
        // the expected prefix
        assertEquals(dataSets.length + writePastRecoveryPointCount, blobStorage.blobs.size());
        for (BlobId blobId : blobStorage.blobs.keySet()) {
            assertEquals(temporaryBucketName, blobId.getBucket());
            assertTrue(blobId.getName().startsWith(temporaryObjectPrefix));
        }

        // commit the write, which will also clean up the temporary objects. call
        // commitAfterRecovery
        // here since we recovered from a prior location
        committer.commitAfterRecovery();

        // we expect exactly one blob -- the final blob -- with the proper content and type
        assertEquals(1, blobStorage.blobs.size());
        MockBlobStorage.BlobValue blobValue = blobStorage.blobs.get(FINAL_BLOB_ID);
        assertNotNull(blobValue);
        assertEquals(options.writerContentType, blobValue.contentType);
        assertArrayEquals(expectedBlobBytes, blobValue.content);
    }

    @Test
    public void shouldCleanupAfterFailure() throws IOException {

        // process each data set, persisting after each one
        GSRecoverableWriterState state = null;
        for (byte[][] dataSet : dataSets) {
            for (byte[] content : dataSet) {
                stream.write(content);
            }
            state = (GSRecoverableWriterState) stream.persist();
        }

        // close here but don't commit, instead clean up
        stream.close();

        // clean up state
        assertTrue(recoverableWriter.requiresCleanupOfRecoverableState());
        assertNotNull(state);
        assertTrue(recoverableWriter.cleanupRecoverableState(state));

        // we should have zero blobs
        assertEquals(0, blobStorage.blobs.size());
    }

    @Test(expected = IOException.class)
    public void shouldFailOnWriteAfterClose() throws IOException {
        stream.write(dataSets[0][0]);
        stream.close();
        stream.write(dataSets[0][0]);
    }
}
