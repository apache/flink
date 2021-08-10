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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for successful and failing PUT operations against the BLOB server, and successful GET
 * operations.
 */
public class BlobServerPutTest extends TestLogger {

    private final Random rnd = new Random();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public final ExpectedException exception = ExpectedException.none();

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
    public void testServerContentAddressableGetStorageLocationConcurrentNoJob() throws Exception {
        testServerContentAddressableGetStorageLocationConcurrent(null);
    }

    /** Tests concurrent calls to {@link BlobServer#getStorageLocation(JobID, BlobKey)}. */
    @Test
    public void testServerContentAddressableGetStorageLocationConcurrentForJob() throws Exception {
        testServerContentAddressableGetStorageLocationConcurrent(new JobID());
    }

    private void testServerContentAddressableGetStorageLocationConcurrent(
            @Nullable final JobID jobId) throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

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
    public void testPutBufferSuccessfulGet1() throws IOException {
        testPutBufferSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferSuccessfulGet2() throws IOException {
        testPutBufferSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferSuccessfulGet3() throws IOException {
        testPutBufferSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferSuccessfulGet4() throws IOException {
        testPutBufferSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferSuccessfulGetHa() throws IOException {
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

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, data, blobType);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, data, blobType);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, data2, blobType);
            assertNotNull(key1b);

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, data, blobType);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, data2, blobType);
            assertNotNull(key2b);
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
    public void testPutStreamSuccessfulGet1() throws IOException {
        testPutStreamSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutStreamSuccessfulGet2() throws IOException {
        testPutStreamSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutStreamSuccessfulGet3() throws IOException {
        testPutStreamSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutStreamSuccessfulGet4() throws IOException {
        testPutStreamSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutStreamSuccessfulGetHa() throws IOException {
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

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, new ByteArrayInputStream(data), blobType);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, new ByteArrayInputStream(data), blobType);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, new ByteArrayInputStream(data2), blobType);
            assertNotNull(key1b);

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, new ByteArrayInputStream(data), blobType);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, new ByteArrayInputStream(data2), blobType);
            assertNotNull(key2b);
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
    public void testPutChunkedStreamSuccessfulGet1() throws IOException {
        testPutChunkedStreamSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutChunkedStreamSuccessfulGet2() throws IOException {
        testPutChunkedStreamSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutChunkedStreamSuccessfulGet3() throws IOException {
        testPutChunkedStreamSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutChunkedStreamSuccessfulGet4() throws IOException {
        testPutChunkedStreamSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutChunkedStreamSuccessfulGetHa() throws IOException {
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

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(server, jobId1, new ChunkedInputStream(data, 19), blobType);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(server, jobId1, new ChunkedInputStream(data, 19), blobType);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(server, jobId1, new ChunkedInputStream(data2, 19), blobType);
            assertNotNull(key1b);

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(server, jobId2, new ChunkedInputStream(data, 19), blobType);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(server, jobId2, new ChunkedInputStream(data2, 19), blobType);
            assertNotNull(key2b);
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
    public void testPutBufferFailsNoJob() throws IOException {
        testPutBufferFails(null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsForJob() throws IOException {
        testPutBufferFails(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsForJobHa() throws IOException {
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
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        File tempFileDir = null;
        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
            assertTrue(tempFileDir.setExecutable(true, false));
            assertTrue(tempFileDir.setReadable(true, false));
            assertTrue(tempFileDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server directly
            exception.expect(AccessDeniedException.class);

            put(server, jobId, data, blobType);

        } finally {
            // set writable again to make sure we can remove the directory
            if (tempFileDir != null) {
                //noinspection ResultOfMethodCallIgnored
                tempFileDir.setWritable(true, false);
            }
        }
    }

    @Test
    public void testPutBufferFailsIncomingNoJob() throws IOException {
        testPutBufferFailsIncoming(null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsIncomingForJob() throws IOException {
        testPutBufferFailsIncoming(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsIncomingForJobHa() throws IOException {
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
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        File tempFileDir = null;
        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            tempFileDir = server.createTemporaryFilename().getParentFile();
            assertTrue(tempFileDir.setExecutable(true, false));
            assertTrue(tempFileDir.setReadable(true, false));
            assertTrue(tempFileDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server directly
            exception.expect(IOException.class);
            exception.expectMessage(" (Permission denied)");

            try {
                put(server, jobId, data, blobType);
            } finally {
                File storageDir = tempFileDir.getParentFile();
                // only the incoming directory should exist (no job directory!)
                assertArrayEquals(new String[] {"incoming"}, storageDir.list());
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
    public void testPutBufferFailsStoreNoJob() throws IOException {
        testPutBufferFailsStore(null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsStoreForJob() throws IOException {
        testPutBufferFailsStore(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferFailsStoreForJobHa() throws IOException {
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
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        File jobStoreDir = null;
        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            jobStoreDir =
                    server.getStorageLocation(jobId, BlobKey.createKey(blobType)).getParentFile();
            assertTrue(jobStoreDir.setExecutable(true, false));
            assertTrue(jobStoreDir.setReadable(true, false));
            assertTrue(jobStoreDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server directly
            exception.expect(AccessDeniedException.class);

            try {
                put(server, jobId, data, blobType);
            } finally {
                // there should be no remaining incoming files
                File incomingFileDir = new File(jobStoreDir.getParent(), "incoming");
                assertArrayEquals(new String[] {}, incomingFileDir.list());

                // there should be no files in the job directory
                assertArrayEquals(new String[] {}, jobStoreDir.list());
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
    public void testConcurrentPutOperationsNoJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(null, TRANSIENT_BLOB);
    }

    @Test
    public void testConcurrentPutOperationsForJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testConcurrentPutOperationsForJobHa()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentPutOperations(new JobID(), PERMANENT_BLOB);
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
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        BlobStore blobStore = mock(BlobStore.class);
        int concurrentPutOperations = 2;
        int dataSize = 1024;

        final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
        final byte[] data = new byte[dataSize];

        ArrayList<CompletableFuture<BlobKey>> allFutures = new ArrayList<>(concurrentPutOperations);

        ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

        try (final BlobServer server = new BlobServer(config, blobStore)) {

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

            assertTrue(blobKeyIterator.hasNext());

            BlobKey blobKey = blobKeyIterator.next();

            // make sure that all blob keys are the same
            while (blobKeyIterator.hasNext()) {
                verifyKeyDifferentHashEquals(blobKey, blobKeyIterator.next());
            }

            // check the uploaded file's contents
            verifyContents(server, jobId, blobKey, data);

            // check that we only uploaded the file once to the blob store
            if (blobType == PERMANENT_BLOB) {
                verify(blobStore, times(1)).put(any(File.class), eq(jobId), eq(blobKey));
            } else {
                // can't really verify much in the other cases other than that the put operations
                // should
                // work and not corrupt files
                verify(blobStore, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
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
                    assertEquals(1, keys.size());
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
        validateGetAndClose(new FileInputStream(file), data);
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
        validateGetAndClose(new FileInputStream(file), data);
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
        public int read(byte[] b, int off, int len) throws IOException {
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
