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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
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
import java.util.function.Supplier;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyType;
import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFilesExist;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.BlockingInputStream;
import static org.apache.flink.runtime.blob.BlobServerPutTest.ChunkedInputStream;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
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
public class BlobCachePutTest extends TestLogger {

    private final Random rnd = new Random();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public final ExpectedException exception = ExpectedException.none();

    // --- concurrency tests for utility methods which could fail during the put operation ---

    /** Checked thread that calls {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}. */
    public static class TransientBlobCacheGetStorageLocation extends CheckedThread {
        private final TransientBlobCache cache;
        private final JobID jobId;
        private final BlobKey key;

        TransientBlobCacheGetStorageLocation(
                TransientBlobCache cache, @Nullable JobID jobId, BlobKey key) {
            this.cache = cache;
            this.jobId = jobId;
            this.key = key;
        }

        @Override
        public void go() throws Exception {
            cache.getStorageLocation(jobId, key);
        }
    }

    /** Checked thread that calls {@link PermanentBlobCache#getStorageLocation(JobID, BlobKey)}. */
    public static class PermanentBlobCacheGetStorageLocation extends CheckedThread {
        private final PermanentBlobCache cache;
        private final JobID jobId;
        private final BlobKey key;

        PermanentBlobCacheGetStorageLocation(PermanentBlobCache cache, JobID jobId, BlobKey key) {
            this.cache = cache;
            this.jobId = jobId;
            this.key = key;
        }

        @Override
        public void go() throws Exception {
            cache.getStorageLocation(jobId, key);
        }
    }

    /** Tests concurrent calls to {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}. */
    @Test
    public void testTransientBlobCacheGetStorageLocationConcurrentNoJob() throws Exception {
        testTransientBlobCacheGetStorageLocationConcurrent(null);
    }

    /** Tests concurrent calls to {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}. */
    @Test
    public void testTransientBlobCacheGetStorageLocationConcurrentForJob() throws Exception {
        testTransientBlobCacheGetStorageLocationConcurrent(new JobID());
    }

    private void testTransientBlobCacheGetStorageLocationConcurrent(@Nullable final JobID jobId)
            throws Exception {
        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                final TransientBlobCache cache =
                        new TransientBlobCache(
                                config, new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            BlobKey key = new TransientBlobKey();
            CheckedThread[] threads =
                    new CheckedThread[] {
                        new TransientBlobCacheGetStorageLocation(cache, jobId, key),
                        new TransientBlobCacheGetStorageLocation(cache, jobId, key),
                        new TransientBlobCacheGetStorageLocation(cache, jobId, key)
                    };
            checkedThreadSimpleTest(threads);
        }
    }

    /** Tests concurrent calls to {@link PermanentBlobCache#getStorageLocation(JobID, BlobKey)}. */
    @Test
    public void testPermanentBlobCacheGetStorageLocationConcurrentForJob() throws Exception {
        final JobID jobId = new JobID();
        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                final PermanentBlobCache cache =
                        new PermanentBlobCache(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            BlobKey key = new PermanentBlobKey();
            CheckedThread[] threads =
                    new CheckedThread[] {
                        new PermanentBlobCacheGetStorageLocation(cache, jobId, key),
                        new PermanentBlobCacheGetStorageLocation(cache, jobId, key),
                        new PermanentBlobCacheGetStorageLocation(cache, jobId, key)
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
    public void testPutBufferTransientSuccessfulGet1() throws IOException, InterruptedException {
        testPutBufferSuccessfulGet(null, null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferTransientSuccessfulGet2() throws IOException, InterruptedException {
        testPutBufferSuccessfulGet(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferTransientSuccessfulGet3() throws IOException, InterruptedException {
        testPutBufferSuccessfulGet(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferTransientSuccessfulGet4() throws IOException, InterruptedException {
        testPutBufferSuccessfulGet(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    public void testPutBufferPermanentSuccessfulGet() throws IOException, InterruptedException {
        testPutBufferSuccessfulGet(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads two byte arrays for different jobs into the server via the {@link BlobCacheService}.
     * File transfers should be successful.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testPutBufferSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException, InterruptedException {

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            BlobKey key1a = put(cache, jobId1, data, blobType);
            assertNotNull(key1a);
            verifyType(blobType, key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(cache, jobId1, data, blobType);
            assertNotNull(key1a2);
            verifyType(blobType, key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            BlobKey key1b = put(cache, jobId1, data2, blobType);
            assertNotNull(key1b);
            verifyType(blobType, key1b);

            // files should be available on the server
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            BlobKey key2a = put(cache, jobId2, data, blobType);
            assertNotNull(key2a);
            verifyType(blobType, key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            BlobKey key2b = put(cache, jobId2, data2, blobType);
            assertNotNull(key2b);
            verifyType(blobType, key2b);
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // now verify we can access the BLOBs from the cache
            verifyContents(cache, jobId1, key1a, data);
            verifyContents(cache, jobId1, key1b, data2);
            verifyContents(cache, jobId2, key2a, data);
            verifyContents(cache, jobId2, key2b, data2);

            // transient BLOBs should be deleted from the server, eventually
            if (blobType == TRANSIENT_BLOB) {
                verifyDeletedEventually(server, jobId1, key1a);
                verifyDeletedEventually(server, jobId1, key1b);
                verifyDeletedEventually(server, jobId2, key2a);
                verifyDeletedEventually(server, jobId2, key2b);

                // the files are still there on the cache though
                verifyContents(cache, jobId1, key1a, data);
                verifyContents(cache, jobId1, key1b, data2);
                verifyContents(cache, jobId2, key2a, data);
                verifyContents(cache, jobId2, key2b, data2);
            } else {
                // still on the server for permanent BLOBs after accesses from a cache
                verifyContents(server, jobId1, key1a, data);
                verifyContents(server, jobId1, key1b, data2);
                verifyContents(server, jobId2, key2a, data);
                verifyContents(server, jobId2, key2b, data2);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testPutStreamTransientSuccessfulGet1() throws IOException, InterruptedException {
        testPutStreamTransientSuccessfulGet(null, null);
    }

    @Test
    public void testPutStreamTransientSuccessfulGet2() throws IOException, InterruptedException {
        testPutStreamTransientSuccessfulGet(null, new JobID());
    }

    @Test
    public void testPutStreamTransientSuccessfulGet3() throws IOException, InterruptedException {
        testPutStreamTransientSuccessfulGet(new JobID(), new JobID());
    }

    @Test
    public void testPutStreamTransientSuccessfulGet4() throws IOException, InterruptedException {
        testPutStreamTransientSuccessfulGet(new JobID(), null);
    }

    /**
     * Uploads two file streams for different jobs into the server via the {@link BlobCacheService}.
     * File transfers should be successful.
     *
     * <p>Note that high-availability uploads of streams is currently only possible at the {@link
     * BlobServer}.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     */
    private void testPutStreamTransientSuccessfulGet(@Nullable JobID jobId1, @Nullable JobID jobId2)
            throws IOException, InterruptedException {

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            TransientBlobKey key1a =
                    (TransientBlobKey)
                            put(cache, jobId1, new ByteArrayInputStream(data), TRANSIENT_BLOB);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(cache, jobId1, new ByteArrayInputStream(data), TRANSIENT_BLOB);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            TransientBlobKey key1b =
                    (TransientBlobKey)
                            put(cache, jobId1, new ByteArrayInputStream(data2), TRANSIENT_BLOB);
            assertNotNull(key1b);

            // files should be available on the server
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            TransientBlobKey key2a =
                    (TransientBlobKey)
                            put(cache, jobId2, new ByteArrayInputStream(data), TRANSIENT_BLOB);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            TransientBlobKey key2b =
                    (TransientBlobKey)
                            put(cache, jobId2, new ByteArrayInputStream(data2), TRANSIENT_BLOB);
            assertNotNull(key2b);
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // now verify we can access the BLOBs from the cache
            verifyContents(cache, jobId1, key1a, data);
            verifyContents(cache, jobId1, key1b, data2);
            verifyContents(cache, jobId2, key2a, data);
            verifyContents(cache, jobId2, key2b, data2);

            // transient BLOBs should be deleted from the server, eventually
            verifyDeletedEventually(server, jobId1, key1a);
            verifyDeletedEventually(server, jobId1, key1b);
            verifyDeletedEventually(server, jobId2, key2a);
            verifyDeletedEventually(server, jobId2, key2b);

            // the files are still there on the cache though
            verifyContents(cache, jobId1, key1a, data);
            verifyContents(cache, jobId1, key1b, data2);
            verifyContents(cache, jobId2, key2a, data);
            verifyContents(cache, jobId2, key2b, data2);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testPutChunkedStreamTransientSuccessfulGet1()
            throws IOException, InterruptedException {
        testPutChunkedStreamTransientSuccessfulGet(null, null);
    }

    @Test
    public void testPutChunkedStreamTransientSuccessfulGet2()
            throws IOException, InterruptedException {
        testPutChunkedStreamTransientSuccessfulGet(null, new JobID());
    }

    @Test
    public void testPutChunkedStreamTransientSuccessfulGet3()
            throws IOException, InterruptedException {
        testPutChunkedStreamTransientSuccessfulGet(new JobID(), new JobID());
    }

    @Test
    public void testPutChunkedStreamTransientSuccessfulGet4()
            throws IOException, InterruptedException {
        testPutChunkedStreamTransientSuccessfulGet(new JobID(), null);
    }

    /**
     * Uploads two chunked file streams for different jobs into the server via the {@link
     * BlobCacheService}. File transfers should be successful.
     *
     * <p>Note that high-availability uploads of streams is currently only possible at the {@link
     * BlobServer}.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     */
    private void testPutChunkedStreamTransientSuccessfulGet(
            @Nullable JobID jobId1, @Nullable JobID jobId2)
            throws IOException, InterruptedException {

        final Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOfRange(data, 10, 54);

            // put data for jobId1 and verify
            TransientBlobKey key1a =
                    (TransientBlobKey)
                            put(cache, jobId1, new ChunkedInputStream(data, 19), TRANSIENT_BLOB);
            assertNotNull(key1a);
            // second upload of same data should yield a different BlobKey
            BlobKey key1a2 = put(cache, jobId1, new ChunkedInputStream(data, 19), TRANSIENT_BLOB);
            assertNotNull(key1a2);
            verifyKeyDifferentHashEquals(key1a, key1a2);

            TransientBlobKey key1b =
                    (TransientBlobKey)
                            put(cache, jobId1, new ChunkedInputStream(data2, 19), TRANSIENT_BLOB);
            assertNotNull(key1b);

            // files should be available on the server
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);

            // now put data for jobId2 and verify that both are ok
            TransientBlobKey key2a =
                    (TransientBlobKey)
                            put(cache, jobId2, new ChunkedInputStream(data, 19), TRANSIENT_BLOB);
            assertNotNull(key2a);
            verifyKeyDifferentHashEquals(key1a, key2a);

            TransientBlobKey key2b =
                    (TransientBlobKey)
                            put(cache, jobId2, new ChunkedInputStream(data2, 19), TRANSIENT_BLOB);
            assertNotNull(key2b);
            verifyKeyDifferentHashEquals(key1b, key2b);

            // verify the accessibility and the BLOB contents
            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1a2, data);
            verifyContents(server, jobId1, key1b, data2);
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // now verify we can access the BLOBs from the cache
            verifyContents(cache, jobId1, key1a, data);
            verifyContents(cache, jobId1, key1b, data2);
            verifyContents(cache, jobId2, key2a, data);
            verifyContents(cache, jobId2, key2b, data2);

            // transient BLOBs should be deleted from the server, eventually
            verifyDeletedEventually(server, jobId1, key1a);
            verifyDeletedEventually(server, jobId1, key1b);
            verifyDeletedEventually(server, jobId2, key2a);
            verifyDeletedEventually(server, jobId2, key2b);

            // the files are still there on the cache though
            verifyContents(cache, jobId1, key1a, data);
            verifyContents(cache, jobId1, key1b, data2);
            verifyContents(cache, jobId2, key2a, data);
            verifyContents(cache, jobId2, key2b, data2);
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
     * Uploads a byte array to a server which cannot create any files via the {@link
     * BlobCacheService}. File transfers should fail.
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
        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
            assertTrue(tempFileDir.setExecutable(true, false));
            assertTrue(tempFileDir.setReadable(true, false));
            assertTrue(tempFileDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server via the cache
            exception.expect(IOException.class);
            exception.expectMessage("PUT operation failed: ");

            put(cache, jobId, data, blobType);

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
     * BlobCacheService}. File transfers should fail.
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
        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            tempFileDir = server.createTemporaryFilename().getParentFile();
            assertTrue(tempFileDir.setExecutable(true, false));
            assertTrue(tempFileDir.setReadable(true, false));
            assertTrue(tempFileDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server via the cache
            exception.expect(IOException.class);
            exception.expectMessage("PUT operation failed: ");

            try {
                put(cache, jobId, data, blobType);
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
     * Uploads a byte array to a server which cannot create files via the {@link BlobCacheService}.
     * File transfers should fail.
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
        try (BlobServer server = new BlobServer(config, new VoidBlobStore());
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // make sure the blob server cannot create any files in its storage dir
            jobStoreDir =
                    server.getStorageLocation(jobId, BlobKey.createKey(blobType)).getParentFile();
            assertTrue(jobStoreDir.setExecutable(true, false));
            assertTrue(jobStoreDir.setReadable(true, false));
            assertTrue(jobStoreDir.setWritable(false, false));

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // upload the file to the server via the cache
            exception.expect(IOException.class);
            exception.expectMessage("PUT operation failed: ");

            try {
                put(cache, jobId, data, blobType);
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

        final BlobStore blobStoreServer = mock(BlobStore.class);
        final BlobStore blobStoreCache = mock(BlobStore.class);

        int concurrentPutOperations = 2;
        int dataSize = 1024;

        final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
        final byte[] data = new byte[dataSize];

        final List<Path> jars;
        if (blobType == PERMANENT_BLOB) {
            // implement via JAR file upload instead:
            File tmpFile = temporaryFolder.newFile();
            FileUtils.writeByteArrayToFile(tmpFile, data);
            jars = Collections.singletonList(new Path(tmpFile.getAbsolutePath()));
        } else {
            jars = null;
        }

        Collection<CompletableFuture<BlobKey>> allFutures =
                new ArrayList<>(concurrentPutOperations);

        ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

        try (final BlobServer server = new BlobServer(config, blobStoreServer);
                final BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                blobStoreCache,
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // for highAvailability
            final InetSocketAddress serverAddress =
                    new InetSocketAddress("localhost", server.getPort());
            // uploading HA BLOBs works on BlobServer only (and, for now, via the BlobClient)

            for (int i = 0; i < concurrentPutOperations; i++) {
                final Supplier<BlobKey> callable;
                if (blobType == PERMANENT_BLOB) {
                    // cannot use a blocking stream here (upload only possible via files)
                    callable =
                            () -> {
                                try {
                                    List<PermanentBlobKey> keys =
                                            BlobClient.uploadFiles(
                                                    serverAddress, config, jobId, jars);
                                    assertEquals(1, keys.size());
                                    BlobKey uploadedKey = keys.get(0);
                                    // check the uploaded file's contents (concurrently)
                                    verifyContents(server, jobId, uploadedKey, data);
                                    return uploadedKey;
                                } catch (IOException e) {
                                    throw new CompletionException(
                                            new FlinkException("Could not upload blob.", e));
                                }
                            };

                } else {
                    callable =
                            () -> {
                                try {
                                    BlockingInputStream inputStream =
                                            new BlockingInputStream(countDownLatch, data);
                                    BlobKey uploadedKey = put(cache, jobId, inputStream, blobType);
                                    // check the uploaded file's contents (concurrently)
                                    verifyContents(server, jobId, uploadedKey, data);
                                    return uploadedKey;
                                } catch (IOException e) {
                                    throw new CompletionException(
                                            new FlinkException("Could not upload blob.", e));
                                }
                            };
                }
                CompletableFuture<BlobKey> putFuture =
                        CompletableFuture.supplyAsync(callable, executor);

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
                // check for unique BlobKey, but should have same hash
                verifyKeyDifferentHashEquals(blobKey, blobKeyIterator.next());
            }

            // check the uploaded file's contents
            verifyContents(server, jobId, blobKey, data);

            // check that we only uploaded the file once to the blob store
            if (blobType == PERMANENT_BLOB) {
                verify(blobStoreServer, times(1)).put(any(File.class), eq(jobId), eq(blobKey));
            } else {
                // can't really verify much in the other cases other than that the put operations
                // should
                // work and not corrupt files
                verify(blobStoreServer, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
            }
            // caches must not access the blob store (they are not allowed to write there)
            verify(blobStoreCache, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Checks that the given blob will be deleted at the {@link BlobServer} eventually (waits at
     * most 30s).
     *
     * @param server BLOB server
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param keys key(s) identifying the BLOB to request
     */
    static void verifyDeletedEventually(BlobServer server, @Nullable JobID jobId, BlobKey... keys)
            throws IOException, InterruptedException {

        long deadline = System.currentTimeMillis() + 30_000L;
        do {
            Thread.sleep(10);
        } while (checkFilesExist(jobId, Arrays.asList(keys), server, false) != 0
                && System.currentTimeMillis() < deadline);

        for (BlobKey key : keys) {
            verifyDeleted(server, jobId, key);
        }
    }
}
