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
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.apache.flink.runtime.blob.TestingBlobHelpers.checkFileCountForJob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests how DELETE requests behave. */
class BlobServerDeleteTest {

    private final Random rnd = new Random();

    @TempDir private Path tempDir;

    @Test
    void testDeleteTransient1() throws IOException {
        testDeleteBlob(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransient2() throws IOException {
        testDeleteBlob(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransient3() throws IOException {
        testDeleteBlob(null, null, TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransient4() throws IOException {
        testDeleteBlob(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransient5() throws IOException {
        JobID jobId = new JobID();
        testDeleteBlob(jobId, jobId, TRANSIENT_BLOB);
    }

    @Test
    void testDeletePermanent() throws IOException {
        testDeleteBlob(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a (different) byte array for each of the given jobs and verifies that deleting one of
     * them (via the {@link BlobServer}) does not influence the other.
     *
     * @param jobId1 first job id
     * @param jobId2 second job id
     */
    private void testDeleteBlob(
            @Nullable JobID jobId1, @Nullable JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            byte[] data2 = Arrays.copyOf(data, data.length);
            data2[0] ^= 1;

            // put first BLOB
            BlobKey key1 = put(server, jobId1, data, blobType);
            assertThat(key1).isNotNull();

            // put two more BLOBs (same key, other key) for another job ID
            BlobKey key2a = put(server, jobId2, data, blobType);
            assertThat(key2a).isNotNull();
            verifyKeyDifferentHashEquals(key1, key2a);
            BlobKey key2b = put(server, jobId2, data2, blobType);
            assertThat(key2b).isNotNull();

            // issue a DELETE request
            assertThat(delete(server, jobId1, key1, blobType)).isTrue();

            verifyDeleted(server, jobId1, key1);
            // deleting a one BLOB should not affect another BLOB with a different key
            // (and keys are always different now)
            verifyContents(server, jobId2, key2a, data);
            verifyContents(server, jobId2, key2b, data2);

            // delete first file of second job
            assertThat(delete(server, jobId2, key2a, blobType)).isTrue();
            verifyDeleted(server, jobId2, key2a);
            verifyContents(server, jobId2, key2b, data2);

            // delete second file of second job
            assertThat(delete(server, jobId2, key2b, blobType)).isTrue();
            verifyDeleted(server, jobId2, key2b);
        }
    }

    @Test
    void testDeleteTransientAlreadyDeletedNoJob() throws IOException {
        testDeleteBlobAlreadyDeleted(null, TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransientAlreadyDeletedForJob() throws IOException {
        testDeleteBlobAlreadyDeleted(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testDeletePermanentAlreadyDeletedForJob() throws IOException {
        testDeleteBlobAlreadyDeleted(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a byte array for the given job and verifies that deleting it (via the {@link
     * BlobServer}) does not fail independent of whether the file exists.
     *
     * @param jobId job id
     */
    private void testDeleteBlobAlreadyDeleted(
            @Nullable final JobID jobId, BlobKey.BlobType blobType) throws IOException {

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // put BLOB
            BlobKey key = put(server, jobId, data, blobType);
            assertThat(key).isNotNull();

            File blobFile = server.getStorageLocation(jobId, key);
            assertThat(blobFile.delete()).isTrue();

            // DELETE operation should not fail if file is already deleted
            assertThat(delete(server, jobId, key, blobType)).isTrue();
            verifyDeleted(server, jobId, key);

            // one more delete call that should not fail
            assertThat(delete(server, jobId, key, blobType)).isTrue();
            verifyDeleted(server, jobId, key);
        }
    }

    @Test
    void testDeleteTransientFailsNoJob() throws IOException {
        testDeleteBlobFails(null, TRANSIENT_BLOB);
    }

    @Test
    void testDeleteTransientFailsForJob() throws IOException {
        testDeleteBlobFails(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    void testDeletePermanentFailsForJob() throws IOException {
        testDeleteBlobFails(new JobID(), PERMANENT_BLOB);
    }

    /**
     * Uploads a byte array for the given job and verifies that a delete operation (via the {@link
     * BlobServer}) does not fail even if the file is not deletable, e.g. via restricting the
     * permissions.
     *
     * @param jobId job id
     */
    private void testDeleteBlobFails(@Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {
        assumeThat(OperatingSystem.isWindows()).as("setWritable doesn't work on Windows").isFalse();

        File blobFile = null;
        File directory = null;
        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            try {
                byte[] data = new byte[2000000];
                rnd.nextBytes(data);

                // put BLOB
                BlobKey key = put(server, jobId, data, blobType);
                assertThat(key).isNotNull();

                blobFile = server.getStorageLocation(jobId, key);
                directory = blobFile.getParentFile();

                assertThat(blobFile.setWritable(false, false)).isTrue();
                assertThat(directory.setWritable(false, false)).isTrue();

                // issue a DELETE request
                assertThat(delete(server, jobId, key, blobType)).isFalse();

                // the file should still be there
                verifyContents(server, jobId, key, data);
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
    void testJobCleanup() throws IOException {
        testJobCleanup(TRANSIENT_BLOB);
    }

    @Test
    void testJobCleanupHa() throws IOException {
        testJobCleanup(PERMANENT_BLOB);
    }

    /**
     * Tests that {@link BlobServer} cleans up after calling {@link
     * BlobServer#globalCleanupAsync(JobID, Executor)}.
     *
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testJobCleanup(BlobKey.BlobType blobType) throws IOException {
        JobID jobId1 = new JobID();
        JobID jobId2 = new JobID();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            final byte[] data = new byte[128];
            byte[] data2 = Arrays.copyOf(data, data.length);
            data2[0] ^= 1;

            BlobKey key1a = put(server, jobId1, data, blobType);
            BlobKey key2 = put(server, jobId2, data, blobType);
            assertThat(key1a.getHash()).isEqualTo(key2.getHash());

            BlobKey key1b = put(server, jobId1, data2, blobType);

            verifyContents(server, jobId1, key1a, data);
            verifyContents(server, jobId1, key1b, data2);
            checkFileCountForJob(2, jobId1, server);

            verifyContents(server, jobId2, key2, data);
            checkFileCountForJob(1, jobId2, server);

            server.globalCleanupAsync(jobId1, executorService).join();

            verifyDeleted(server, jobId1, key1a);
            verifyDeleted(server, jobId1, key1b);
            checkFileCountForJob(0, jobId1, server);
            verifyContents(server, jobId2, key2, data);
            checkFileCountForJob(1, jobId2, server);

            server.globalCleanupAsync(jobId2, executorService).join();

            checkFileCountForJob(0, jobId1, server);
            verifyDeleted(server, jobId2, key2);
            checkFileCountForJob(0, jobId2, server);

            // calling a second time should not fail
            server.globalCleanupAsync(jobId2, executorService).join();
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();
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

        try (BlobServer server = TestingBlobUtils.createServer(tempDir)) {
            server.start();

            final TransientBlobKey blobKey =
                    (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);

            assertThat(server.getStorageLocation(jobId, blobKey)).exists();

            for (int i = 0; i < concurrentDeleteOperations; i++) {
                CompletableFuture<Void> deleteFuture =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        assertThat(delete(server, jobId, blobKey)).isTrue();
                                        assertThat(server.getStorageLocation(jobId, blobKey))
                                                .doesNotExist();
                                        return null;
                                    } catch (IOException e) {
                                        throw new CompletionException(
                                                new FlinkException(
                                                        "Could not delete the given blob key "
                                                                + blobKey
                                                                + '.'));
                                    }
                                },
                                executor);

                deleteFutures.add(deleteFuture);
            }

            CompletableFuture<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

            // make sure all delete operation have completed successfully
            // in case of no lock, one of the delete operations should eventually fail
            waitFuture.get();

            assertThat(server.getStorageLocation(jobId, blobKey)).doesNotExist();

        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Deletes a transient BLOB from the given BLOB service.
     *
     * @param service blob service
     * @param jobId job ID (or <tt>null</tt> if job-unrelated)
     * @param key blob key
     * @return delete success
     */
    static boolean delete(BlobService service, @Nullable JobID jobId, TransientBlobKey key) {
        if (jobId == null) {
            return service.getTransientBlobService().deleteFromCache(key);
        } else {
            return service.getTransientBlobService().deleteFromCache(jobId, key);
        }
    }

    private static boolean delete(
            BlobServer blobServer, @Nullable JobID jobId, BlobKey key, BlobKey.BlobType blobType) {
        Preconditions.checkNotNull(blobServer);
        Preconditions.checkNotNull(key);

        if (blobType == PERMANENT_BLOB) {
            Preconditions.checkNotNull(jobId);

            assertThat(key).isInstanceOf(PermanentBlobKey.class);
            return blobServer.deletePermanent(jobId, (PermanentBlobKey) key);
        } else {
            assertThat(key).isInstanceOf(TransientBlobKey.class);
            return delete(blobServer, jobId, (TransientBlobKey) key);
        }
    }
}
