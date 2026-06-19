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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCachePutTest.verifyDeletedEventually;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerDeleteTest.delete;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests how DELETE requests behave. */
class BlobCacheDeleteTest {

    @TempDir private Path tempDir;

    private final Random rnd = new Random();

    @Test
    void testDeleteTransient1() throws IOException {
        testDelete(null, new JobID());
    }

    @Test
    void testDeleteTransient2() throws IOException {
        testDelete(new JobID(), null);
    }

    @Test
    void testDeleteTransient3() throws IOException {
        testDelete(null, null);
    }

    @Test
    void testDeleteTransient4() throws IOException {
        testDelete(new JobID(), new JobID());
    }

    @Test
    void testDeleteTransient5() throws IOException {
        JobID jobId = new JobID();
        testDelete(jobId, jobId);
    }

    /**
     * Uploads a (different) byte array for each of the given jobs and verifies that deleting one of
     * them (via the {@link BlobCacheService}) does not influence the other.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     */
    private void testDelete(@Nullable JobID jobId1, @Nullable JobID jobId2) throws IOException {
        Tuple2<BlobServer, BlobCacheService> serverAndCache =
                TestingBlobUtils.createServerAndCache(tempDir);

        try (BlobServer server = serverAndCache.f0;
                BlobCacheService cache = serverAndCache.f1) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOf(data, data.length);
            data2[0] ^= 1;

            // put first BLOB
            TransientBlobKey key1 = (TransientBlobKey) put(server, jobId1, data, TRANSIENT_BLOB);
            assertThat(key1).isNotNull();

            // put two more BLOBs (same key, other key) for another job ID
            TransientBlobKey key2a = (TransientBlobKey) put(server, jobId2, data, TRANSIENT_BLOB);
            assertThat(key2a).isNotNull();
            BlobKeyTest.verifyKeyDifferentHashEquals(key1, key2a);
            TransientBlobKey key2b = (TransientBlobKey) put(server, jobId2, data2, TRANSIENT_BLOB);
            assertThat(key2b).isNotNull();
            BlobKeyTest.verifyKeyDifferentHashDifferent(key1, key2b);

            // issue a DELETE request
            assertThat(delete(cache, jobId1, key1)).isTrue();

            // delete only works on local cache!
            assertThat(server.getStorageLocation(jobId1, key1)).exists();
            // delete on server so that the cache cannot re-download
            assertThat(server.deleteInternal(jobId1, key1)).isTrue();
            assertThat(server.getStorageLocation(jobId1, key1)).doesNotExist();
            verifyDeleted(cache, jobId1, key1);
            // deleting one BLOB should not affect another BLOB with a different key
            // (and keys are always different now)
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // delete first file of second job
            assertThat(delete(cache, jobId2, key2a)).isTrue();
            // delete only works on local cache
            assertThat(server.getStorageLocation(jobId2, key2a)).exists();
            // delete on server so that the cache cannot re-download
            assertThat(server.deleteInternal(jobId2, key2a)).isTrue();
            verifyDeleted(cache, jobId2, key2a);
            verifyContents(server, jobId2, key2b, data2);

            // delete second file of second job
            assertThat(delete(cache, jobId2, key2b)).isTrue();
            // delete only works on local cache
            assertThat(server.getStorageLocation(jobId2, key2b)).exists();
            // delete on server so that the cache cannot re-download
            assertThat(server.deleteInternal(jobId2, key2b)).isTrue();
            verifyDeleted(cache, jobId2, key2b);
        }
    }

    @Test
    void testDeleteTransientAlreadyDeletedNoJob() throws IOException {
        testDeleteTransientAlreadyDeleted(null);
    }

    @Test
    void testDeleteTransientAlreadyDeletedForJob() throws IOException {
        testDeleteTransientAlreadyDeleted(new JobID());
    }

    /**
     * Uploads a byte array for the given job and verifies that deleting it (via the {@link
     * BlobCacheService}) does not fail independent of whether the file exists.
     *
     * @param jobId job id
     */
    private void testDeleteTransientAlreadyDeleted(@Nullable final JobID jobId) throws IOException {
        Tuple2<BlobServer, BlobCacheService> serverAndCache =
                TestingBlobUtils.createServerAndCache(tempDir);

        try (BlobServer server = serverAndCache.f0;
                BlobCacheService cache = serverAndCache.f1) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // put BLOB
            TransientBlobKey key = (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
            assertThat(key).isNotNull();

            File blobFile = server.getStorageLocation(jobId, key);
            assertThat(blobFile.delete()).isTrue();

            // DELETE operation should not fail if file is already deleted
            assertThat(delete(cache, jobId, key)).isTrue();
            verifyDeleted(cache, jobId, key);

            // one more delete call that should not fail
            assertThat(delete(cache, jobId, key)).isTrue();
            verifyDeleted(cache, jobId, key);
        }
    }

    @Test
    void testDeleteTransientLocalFailsNoJob() throws IOException, InterruptedException {
        testDeleteTransientLocalFails(null);
    }

    @Test
    void testDeleteTransientLocalFailsForJob() throws IOException, InterruptedException {
        testDeleteTransientLocalFails(new JobID());
    }

    /**
     * Uploads a byte array for the given job and verifies that a delete operation (via the {@link
     * BlobCacheService}) does not fail even if the file is not deletable locally, e.g. via
     * restricting the permissions.
     *
     * @param jobId job id
     */
    private void testDeleteTransientLocalFails(@Nullable final JobID jobId)
            throws IOException, InterruptedException {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        Tuple2<BlobServer, BlobCacheService> serverAndCache =
                TestingBlobUtils.createServerAndCache(tempDir);

        File blobFile = null;
        File directory = null;
        try (BlobServer server = serverAndCache.f0;
                BlobCacheService cache = serverAndCache.f1) {
            server.start();

            try {
                byte[] data = new byte[2000000];
                rnd.nextBytes(data);

                // put BLOB
                TransientBlobKey key = (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
                assertThat(key).isNotNull();

                // access from cache once to have it available there
                verifyContents(cache, jobId, key, data);

                blobFile = cache.getTransientBlobService().getStorageLocation(jobId, key);
                directory = blobFile.getParentFile();

                assertThat(blobFile.setWritable(false, false)).isTrue();
                assertThat(directory.setWritable(false, false)).isTrue();

                // issue a DELETE request

                assertThat(delete(cache, jobId, key)).isFalse();

                // the file should still be there on the cache
                verifyContents(cache, jobId, key, data);
                // the server should have started the delete call after the cache accessed the
                // (transient!) BLOB
                verifyDeletedEventually(server, jobId, key);
            } finally {
                if (blobFile != null && directory != null) {
                    //noinspection ResultOfMethodCallIgnored
                    blobFile.setWritable(true, false);
                    //noinspection ResultOfMethodCallIgnored
                    directory.setWritable(true, false);
                }
            }
        }
    }

    @Test
    void testConcurrentDeleteOperationsNoJobTransient()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentDeleteOperations(null);
    }

    @Test
    void testConcurrentDeleteOperationsForJobTransient()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentDeleteOperations(new JobID());
    }

    /**
     * [FLINK-6020] Tests that concurrent delete operations don't interfere with each other.
     *
     * <p>Note: This test checks that there cannot be two threads which have checked whether a given
     * blob file exist and then one of them fails deleting it. Without the introduced lock, this
     * situation should rarely happen and make this test fail. Thus, if this test should become
     * "unstable", then the delete atomicity is most likely broken.
     *
     * @param jobId job ID to use (or <tt>null</tt> if job-unrelated)
     */
    private void testConcurrentDeleteOperations(@Nullable final JobID jobId)
            throws IOException, InterruptedException, ExecutionException {

        final int concurrentDeleteOperations = 3;
        final ExecutorService executor = Executors.newFixedThreadPool(concurrentDeleteOperations);

        final List<CompletableFuture<Void>> deleteFutures =
                new ArrayList<>(concurrentDeleteOperations);

        final byte[] data = {1, 2, 3};

        Tuple2<BlobServer, BlobCacheService> serverAndCache =
                TestingBlobUtils.createServerAndCache(tempDir);
        try (BlobServer server = serverAndCache.f0;
                BlobCacheService cache = serverAndCache.f1) {
            server.start();

            final TransientBlobKey blobKey =
                    (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);

            assertThat(server.getStorageLocation(jobId, blobKey)).exists();

            for (int i = 0; i < concurrentDeleteOperations; i++) {
                CompletableFuture<Void> deleteFuture =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        assertThat(delete(cache, jobId, blobKey)).isTrue();
                                        assertThat(
                                                        cache.getTransientBlobService()
                                                                .getStorageLocation(jobId, blobKey))
                                                .doesNotExist();
                                        // delete only works on local cache!
                                        assertThat(server.getStorageLocation(jobId, blobKey))
                                                .exists();
                                        return null;
                                    } catch (IOException e) {
                                        throw new CompletionException(
                                                new FlinkException("Could not upload blob.", e));
                                    }
                                },
                                executor);

                deleteFutures.add(deleteFuture);
            }

            CompletableFuture<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

            // make sure all delete operation have completed successfully
            // in case of no lock, one of the delete operations should eventually fail
            waitFuture.get();

            // delete only works on local cache!
            assertThat(server.getStorageLocation(jobId, blobKey)).exists();

        } finally {
            executor.shutdownNow();
        }
    }
}
