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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests for successful and failing PUT operations against the BLOB server, and successful GET
 * operations.
 */
class BlobServerPutTest {

    @TempDir private java.nio.file.Path tempDir;

    private final Random rnd = new Random();

    // --- concurrency tests for utility methods which could fail during the put operation ---

    /** Checked thread that calls {@link BlobServer#getStorageLocation(JobID, BlobKey)}. */
    public static class ContentAddressableGetStorageLocation extends CheckedThread {
        private final BlobServer server;
        private final JobID jobId;
        private final BlobKey key;

        ContentAddressableGetStorageLocation(
                BlobServer server, @Nullable JobID jobId, BlobKey key) {
            this.server = server;
            this.jobId = jobId;
            this.key = key;
        }

        @Override
        public void go() throws Exception {
            server.getStorageLocation(jobId, key);
        }
    }

    /** Tests concurrent calls to {@link BlobServer#getStorageLocation(JobID, BlobKey)}. */
    @Test
    void testServerContentAddressableGetStorageLocationConcurrentNoJob() throws Exception {
        testServerContentAddressableGetStorageLocationConcurrent(null);
    }

    /** Tests concurrent calls to {@link BlobServer#getStorageLocation(JobID, BlobKey)}. */
    @Test
    void testServerContentAddressableGetStorageLocationConcurrentForJob() throws Exception {
        testServerContentAddressableGetStorageLocationConcurrent(new JobID());
    }

    private void testServerContentAddressableGetStorageLocationConcurrent(
            @Nullable final JobID jobId) throws Exception {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            BlobKey key1 = new TransientBlobKey();
            BlobKey key2 = new PermanentBlobKey();
            CheckedThread[] threads =
                    new CheckedThread[] {
                        new ContentAddressableGetStorageLocation(server, jobId, key1),
                        new ContentAddressableGetStorageLocation(server, jobId, key1),
                        new ContentAddressableGetStorageLocation(server, jobId, key1),
                        new ContentAddressableGetStorageLocation(server, jobId, key2),
                        new ContentAddressableGetStorageLocation(server, jobId, key2),
                        new ContentAddressableGetStorageLocation(server, jobId, key2)
                    };
            checkedThreadSimpleTest(threads);
        }
    }

    /**
     * Helper method to first start all threads and then wait for their completion.
     *
     * @param threads threads to use
     * @throws Exception exceptions that are thrown from the threads
     */
    private void checkedThreadSimpleTest(CheckedThread[] threads) throws Exception {

        // start all threads
        for (CheckedThread t : threads) {
            t.start();
        }

        // wait for thread completion and check exceptions
        for (CheckedThread t : threads) {
            t.sync();
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testPutBufferSuccessfulGet1() throws IOException {
        testPutBufferSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferSuccessfulGet2() throws IOException {
        testPutBufferSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferSuccessfulGet3() throws IOException {
        testPutBufferSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferSuccessfulGet4() throws IOException {
        testPutBufferSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferSuccessfulGetHa() throws IOException {
        testPutBufferSuccessfulGet(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads two byte arrays for different jobs into the server via the {@link BlobServer}. File
     * transfers should be successful.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, data, blobType);
            assertThat(key1a).isNotNull();
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, data, blobType);
            assertThat(key1a2).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, data2, blobType);
            assertThat(key1b).isNotNull();

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, data, blobType);
            assertThat(key2a).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, data2, blobType);
            assertThat(key2b).isNotNull();
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // verify the accessibility and the BLOB contents one more time (transient BLOBs should
            // not be deleted here)
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testPutStreamSuccessfulGet1() throws IOException {
        testPutStreamSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    void testPutStreamSuccessfulGet2() throws IOException {
        testPutStreamSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutStreamSuccessfulGet3() throws IOException {
        testPutStreamSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutStreamSuccessfulGet4() throws IOException {
        testPutStreamSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    void testPutStreamSuccessfulGetHa() throws IOException {
        testPutStreamSuccessfulGet(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads two file streams for different jobs into the server via the {@link BlobServer}. File
     * transfers should be successful.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutStreamSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, new ByteArrayInputStream(data), blobType);
            assertThat(key1a).isNotNull();
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, new ByteArrayInputStream(data), blobType);
            assertThat(key1a2).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, new ByteArrayInputStream(data2), blobType);
            assertThat(key1b).isNotNull();

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, new ByteArrayInputStream(data), blobType);
            assertThat(key2a).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, new ByteArrayInputStream(data2), blobType);
            assertThat(key2b).isNotNull();
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // verify the accessibility and the BLOB contents one more time (transient BLOBs should
            // not be deleted here)
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testPutChunkedStreamSuccessfulGet1() throws IOException {
        testPutChunkedStreamSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    void testPutChunkedStreamSuccessfulGet2() throws IOException {
        testPutChunkedStreamSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutChunkedStreamSuccessfulGet3() throws IOException {
        testPutChunkedStreamSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutChunkedStreamSuccessfulGet4() throws IOException {
        testPutChunkedStreamSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    void testPutChunkedStreamSuccessfulGetHa() throws IOException {
        testPutChunkedStreamSuccessfulGet(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads two chunked file streams for different jobs into the server via the {@link
     * BlobServer}. File transfers should be successful.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutChunkedStreamSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, new ChunkedInputStream(data, 19), blobType);
            assertThat(key1a).isNotNull();
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, new ChunkedInputStream(data, 19), blobType);
            assertThat(key1a2).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, new ChunkedInputStream(data2, 19), blobType);
            assertThat(key1b).isNotNull();

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, new ChunkedInputStream(data, 19), blobType);
            assertThat(key2a).isNotNull();
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, new ChunkedInputStream(data2, 19), blobType);
            assertThat(key2b).isNotNull();
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // verify the accessibility and the BLOB contents one more time (transient BLOBs should
            // not be deleted here)
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    void testPutBufferFailsNoJob() throws IOException {
        testPutBufferFails(null, TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsForJob() throws IOException {
        testPutBufferFails(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsForJobHa() throws IOException {
        testPutBufferFails(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a byte array to a server which cannot create any files via the {@link BlobServer}.
     * File transfers should fail.
     *
     * @param jobId job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferFails(@Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            // make sure the blob server cannot create any files in its storage dir
            File tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
            assertThat(tempFileDir.setExecutable(true, false)).isTrue();
            assertThat(tempFileDir.setReadable(true, false)).isTrue();
            assertThat(tempFileDir.setWritable(false, false)).isTrue();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server directly
            assertThatThrownBy(() -> put(server, jobId, data, blobType))
                    .isInstanceOf(AccessDeniedException.class);

            // set writable again to make sure we can remove the directory
            assertThat(tempFileDir.setWritable(true, false)).isTrue();
        }
    }

    @Test
    void testPutBufferFailsIncomingNoJob() throws IOException {
        testPutBufferFailsIncoming(null, TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsIncomingForJob() throws IOException {
        testPutBufferFailsIncoming(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsIncomingForJobHa() throws IOException {
        testPutBufferFailsIncoming(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a byte array to a server which cannot create incoming files via the {@link
     * BlobServer}. File transfers should fail.
     *
     * @param jobId job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferFailsIncoming(@Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        File tempFileDir = null;
        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            // make sure the blob server cannot create any files in its storage dir
            tempFileDir = server.createTemporaryFilename().getParentFile();
            assertThat(tempFileDir.setExecutable(true, false)).isTrue();
            assertThat(tempFileDir.setReadable(true, false)).isTrue();
            assertThat(tempFileDir.setWritable(false, false)).isTrue();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            try {
                // upload the file to the server directly
                assertThatThrownBy(() -> put(server, jobId, data, blobType))
                        .isInstanceOf(IOException.class)
                        .hasMessageEndingWith(" (Permission denied)");
            } finally {
                File storageDir = tempFileDir.getParentFile();
                // only the incoming directory should exist (no job directory!)
                assertThat(storageDir.list()).containsExactly("incoming");
            }
        } finally {
            // set writable again to make sure we can remove the directory
            if (tempFileDir != null) {
                //noinspection ResultOfMethodCallIgnored
                tempFileDir.setWritable(true, false);
            }
        }
    }

    @Test
    void testPutBufferFailsStoreNoJob() throws IOException {
        testPutBufferFailsStore(null, TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsStoreForJob() throws IOException {
        testPutBufferFailsStore(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testPutBufferFailsStoreForJobHa() throws IOException {
        testPutBufferFailsStore(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a byte array to a server which cannot move incoming files to the final blob store via
     * the {@link BlobServer}. File transfers should fail.
     *
     * @param jobId job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferFailsStore(@Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        File jobStoreDir = null;
        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            // make sure the blob server cannot create any files in its storage dir
            jobStoreDir =
                    server.getStorageLocation(jobId, BlobKey.createKey(blobType)).getParentFile();
            assertThat(jobStoreDir.setExecutable(true, false)).isTrue();
            assertThat(jobStoreDir.setReadable(true, false)).isTrue();
            assertThat(jobStoreDir.setWritable(false, false)).isTrue();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            try {
                // upload the file to the server directly
                assertThatThrownBy(() -> put(server, jobId, data, blobType))
                        .isInstanceOf(AccessDeniedException.class);
            } finally {
                // there should be no remaining incoming files
                File incomingFileDir = new File(jobStoreDir.getParent(), "incoming");
                assertThat(incomingFileDir.list()).isEmpty();

                // there should be no files in the job directory
                assertThat(jobStoreDir.list()).isEmpty();
            }
        } finally {
            // set writable again to make sure we can remove the directory
            if (jobStoreDir != null) {
                //noinspection ResultOfMethodCallIgnored
                jobStoreDir.setWritable(true, false);
            }
        }
    }

    @Test
    void testConcurrentPutOperationsNoJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(null, TRANSIENT_BLOB);
    }

    @Test
    void testConcurrentPutOperationsForJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testConcurrentPutOperationsForJobHa()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(new JobID(), PERMANENT_BLOB);
    }

    @Test
    void testFailedBlobStorePutsDeletesLocalBlob() throws IOException {
        final BlobKey.BlobType blobType = PERMANENT_BLOB;
        final JobID jobId = JobID.generate();
        final byte[] data = new byte[] {1, 2, 3};

        final File storageDir = TempDirUtils.newFolder(tempDir);
        final TestingBlobStore blobStore =
                new TestingBlobStoreBuilder()
                        .setPutFunction(
                                (file, jobID, blobKey) -> {
                                    throw new IOException("Could not persist the file.");
                                })
                        .createTestingBlobStore();
        try (final BlobServer blobServer =
                new BlobServer(new Configuration(), storageDir, blobStore)) {
            assertThatThrownBy(() -> put(blobServer, jobId, data, blobType))
                    .isInstanceOf(IOException.class);

            final File jobSpecificStorageDirectory =
                    new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId));

            assertThat(jobSpecificStorageDirectory).isEmptyDirectory();
        }
    }

    /**
     * [FLINK-6020] Tests that concurrent put operations will only upload the file once to the
     * {@link BlobStore} and that the files are not corrupt at any time.
     *
     * @param jobId job ID to use (or <tt>null</tt> if job-unrelated)
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testConcurrentPutOperations(
            @Nullable final JobID jobId, final BlobKey.BlobType blobType)
            throws IOException, InterruptedException, ExecutionException {
        final Configuration config = new Configuration();
        final int concurrentPutOperations = 2;
        final int dataSize = 1024;

        Collection<BlobKey> persistedBlobs = ConcurrentHashMap.newKeySet();
        TestingBlobStore blobStore =
                new TestingBlobStoreBuilder()
                        .setPutFunction(
                                (file, jobID, blobKey) -> {
                                    persistedBlobs.add(blobKey);
                                    return true;
                                })
                        .createTestingBlobStore();

        final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
        final byte[] data = new byte[dataSize];

        ArrayList<CompletableFuture<BlobKey>> allFutures = new ArrayList<>(concurrentPutOperations);

        ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

        try (BlobServer server = TestingBlobUtils.createServer(tempDir, config, blobStore)) {
            server.start();

            for (int i = 0; i < concurrentPutOperations; i++) {
                CompletableFuture<BlobKey> putFuture =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        BlockingInputStream inputStream =
                                                new BlockingInputStream(countDownLatch, data);
                                        BlobKey uploadedKey =
                                                put(server, jobId, inputStream, blobType);
                                        // check the uploaded file's contents (concurrently)
                                        verifyContents(server, jobId, uploadedKey, data);
                                        return uploadedKey;
                                    } catch (IOException e) {
                                        throw new CompletionException(
                                                new FlinkException("Could not upload blob.", e));
                                    }
                                },
                                executor);

                allFutures.add(putFuture);
            }

            FutureUtils.ConjunctFuture<Collection<BlobKey>> conjunctFuture =
                    FutureUtils.combineAll(allFutures);

            // wait until all operations have completed and check that no exception was thrown
            Collection<BlobKey> blobKeys = conjunctFuture.get();

            Iterator<BlobKey> blobKeyIterator = blobKeys.iterator();

            assertThat(blobKeyIterator).hasNext();

            BlobKey blobKey = blobKeyIterator.next();

            // make sure that all blob keys are the same
            while (blobKeyIterator.hasNext()) {
                verifyKeyDifferentHashEquals(blobKey, blobKeyIterator.next());
            }

            // check the uploaded file's contents
            verifyContents(server, jobId, blobKey, data);

            // check that we only uploaded the file once to the blob store
            if (blobType == PERMANENT_BLOB) {
                assertThat(persistedBlobs).hasSameElementsAs(blobKeys);
            } else {
                // can't really verify much in the other cases other than that the put operations
                // should
                // work and not corrupt files
                assertThat(persistedBlobs).isEmpty();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Helper to choose the right {@link BlobServer#putTransient} method.
     *
     * @param blobType whether the BLOB should become permanent or transient
     * @return blob key for the uploaded data
     */
    static BlobKey put(
            BlobService service, @Nullable JobID jobId, InputStream data, BlobKey.BlobType blobType)
            throws IOException {
        if (blobType == PERMANENT_BLOB) {
            if (service instanceof BlobServer) {
                return ((BlobServer) service).putPermanent(jobId, data);
            } else {
                throw new UnsupportedOperationException(
                        "uploading streams is only possible at the BlobServer");
            }
        } else if (jobId == null) {
            return service.getTransientBlobService().putTransient(data);
        } else {
            return service.getTransientBlobService().putTransient(jobId, data);
        }
    }

    /**
     * Helper to choose the right {@link BlobServer#putTransient} method.
     *
     * @param blobType whether the BLOB should become permanent or transient
     * @return blob key for the uploaded data
     */
    static BlobKey put(
            BlobService service, @Nullable JobID jobId, byte[] data, BlobKey.BlobType blobType)
            throws IOException {
        if (blobType == PERMANENT_BLOB) {
            if (service instanceof BlobServer) {
                return ((BlobServer) service).putPermanent(jobId, data);
            } else {
                // implement via JAR file upload instead:
                File tmpFile = Files.createTempFile("blob", ".jar").toFile();
                try {
                    FileUtils.writeByteArrayToFile(tmpFile, data);
                    InetSocketAddress serverAddress =
                            new InetSocketAddress("localhost", service.getPort());
                    // uploading HA BLOBs works on BlobServer only (and, for now, via the
                    // BlobClient)
                    Configuration clientConfig = new Configuration();
                    List<Path> jars =
                            Collections.singletonList(new Path(tmpFile.getAbsolutePath()));
                    List<PermanentBlobKey> keys =
                            BlobClient.uploadFiles(serverAddress, clientConfig, jobId, jars);
                    assertThat(keys).hasSize(1);
                    return keys.get(0);
                } finally {
                    //noinspection ResultOfMethodCallIgnored
                    tmpFile.delete();
                }
            }
        } else if (jobId == null) {
            return service.getTransientBlobService().putTransient(data);
        } else {
            return service.getTransientBlobService().putTransient(jobId, data);
        }
    }

    /**
     * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
     *
     * @param blobService BlobServer to use
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key blob key
     * @param data expected data
     */
    static void verifyContents(
            BlobService blobService, @Nullable JobID jobId, BlobKey key, byte[] data)
            throws IOException {

        File file = get(blobService, jobId, key);
        validateGetAndClose(Files.newInputStream(file.toPath()), data);
    }

    /**
     * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
     *
     * @param blobService BlobServer to use
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key blob key
     * @param data expected data
     */
    static void verifyContents(
            BlobService blobService, @Nullable JobID jobId, BlobKey key, InputStream data)
            throws IOException {

        File file = get(blobService, jobId, key);
        validateGetAndClose(Files.newInputStream(file.toPath()), data);
    }

    // --------------------------------------------------------------------------------------------

    static final class BlockingInputStream extends InputStream {

        private final CountDownLatch countDownLatch;
        private final byte[] data;
        private int index = 0;

        BlockingInputStream(CountDownLatch countDownLatch, byte[] data) {
            this.countDownLatch = Preconditions.checkNotNull(countDownLatch);
            this.data = Preconditions.checkNotNull(data);
        }

        @Override
        public int read() throws IOException {

            countDownLatch.countDown();

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Blocking operation was interrupted.", e);
            }

            if (index >= data.length) {
                return -1;
            } else {
                return data[index++];
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    static final class ChunkedInputStream extends InputStream {

        private final byte[][] data;

        private int x = 0, y = 0;

        ChunkedInputStream(byte[] data, int numChunks) {
            this.data = new byte[numChunks][];

            int bytesPerChunk = data.length / numChunks;
            int bytesTaken = 0;
            for (int i = 0; i < numChunks - 1; i++, bytesTaken += bytesPerChunk) {
                this.data[i] = new byte[bytesPerChunk];
                System.arraycopy(data, bytesTaken, this.data[i], 0, bytesPerChunk);
            }

            this.data[numChunks - 1] = new byte[data.length - bytesTaken];
            System.arraycopy(
                    data, bytesTaken, this.data[numChunks - 1], 0, this.data[numChunks - 1].length);
        }

        @Override
        public int read() {
            if (x < data.length) {
                byte[] curr = data[x];
                if (y < curr.length) {
                    byte next = curr[y];
                    y++;
                    return next;
                } else {
                    y = 0;
                    x++;
                    return read();
                }
            } else {
                return -1;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            if (x < data.length) {
                byte[] curr = data[x];
                if (y < curr.length) {
                    int toCopy = Math.min(len, curr.length - y);
                    System.arraycopy(curr, y, b, off, toCopy);
                    y += toCopy;
                    return toCopy;
                } else {
                    y = 0;
                    x++;
                    return read(b, off, len);
                }
            } else {
                return -1;
            }
        }
    }
}
