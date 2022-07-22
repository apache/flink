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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.TriConsumerWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCachePutTest.verifyDeletedEventually;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** A few tests for the cleanup of transient BLOBs at the {@link BlobServer}. */
public class BlobServerCleanupTest extends TestLogger {

    private static final Random RANDOM = new Random();

    @TempDir private File temporaryFolder;

    private static byte[] createRandomData() {
        final byte[] randomData = new byte[2000000];
        RANDOM.nextBytes(randomData);

        return randomData;
    }

    private static BlobServer createTestInstance(String storageDirectoryPath, long cleanupInterval)
            throws IOException {
        return createTestInstance(storageDirectoryPath, cleanupInterval, new VoidBlobStore());
    }

    private static BlobServer createTestInstance(
            String storageDirectoryPath, long cleanupInterval, BlobStore blobStore)
            throws IOException {
        final Configuration config = new Configuration();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, storageDirectoryPath);
        config.setLong(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

        return new BlobServer(config, new File(storageDirectoryPath), blobStore);
    }

    @Test
    public void testTransientBlobNoJobCleanup()
            throws IOException, InterruptedException, ExecutionException {
        testTransientBlobCleanup(null);
    }

    @Test
    public void testTransientBlobForJobCleanup()
            throws IOException, InterruptedException, ExecutionException {
        testTransientBlobCleanup(new JobID());
    }

    /**
     * Tests that {@link TransientBlobCache} cleans up after a default TTL and keeps files which are
     * constantly accessed.
     */
    private void testTransientBlobCleanup(@Nullable final JobID jobId)
            throws IOException, InterruptedException, ExecutionException {

        // 1s should be a safe-enough buffer to still check for existence after a BLOB's last access
        long cleanupInterval = 1L; // in seconds
        final int numberConcurrentGetOperations = 3;

        final List<CompletableFuture<Void>> getOperations =
                new ArrayList<>(numberConcurrentGetOperations);

        byte[] data = createRandomData();
        byte[] data2 = createRandomData();

        long cleanupLowerBound;

        try (BlobServer server =
                createTestInstance(temporaryFolder.getAbsolutePath(), cleanupInterval)) {

            ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> transientBlobExpiryTimes =
                    server.getBlobExpiryTimes();

            server.start();

            // after put(), files are cached for the given TTL
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            final TransientBlobKey key1 =
                    (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
            final Long key1ExpiryAfterPut = transientBlobExpiryTimes.get(Tuple2.of(jobId, key1));
            assertThat(key1ExpiryAfterPut).isGreaterThanOrEqualTo(cleanupLowerBound);

            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            final TransientBlobKey key2 =
                    (TransientBlobKey) put(server, jobId, data2, TRANSIENT_BLOB);
            final Long key2ExpiryAfterPut = transientBlobExpiryTimes.get(Tuple2.of(jobId, key2));
            assertThat(key2ExpiryAfterPut).isGreaterThanOrEqualTo(cleanupLowerBound);

            // check that HA contents are not cleaned up
            final JobID jobIdHA = (jobId == null) ? new JobID() : jobId;
            final BlobKey keyHA = put(server, jobIdHA, data, PERMANENT_BLOB);

            // access key1, verify expiry times (delay at least 1ms to also verify key2 expiry is
            // unchanged)
            Thread.sleep(1);
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            verifyContents(server, jobId, key1, data);
            final Long key1ExpiryAfterGet = transientBlobExpiryTimes.get(Tuple2.of(jobId, key1));
            assertThat(key1ExpiryAfterGet).isGreaterThan(key1ExpiryAfterPut);
            assertThat(key1ExpiryAfterGet).isGreaterThanOrEqualTo(cleanupLowerBound);
            assertThat(key2ExpiryAfterPut)
                    .isEqualTo(transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)));

            // access key2, verify expiry times (delay at least 1ms to also verify key1 expiry is
            // unchanged)
            Thread.sleep(1);
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            verifyContents(server, jobId, key2, data2);
            assertThat(key1ExpiryAfterGet)
                    .isEqualTo(transientBlobExpiryTimes.get(Tuple2.of(jobId, key1)));
            assertThat(transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)))
                    .isGreaterThan(key2ExpiryAfterPut);
            assertThat(transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)))
                    .isGreaterThanOrEqualTo(cleanupLowerBound);

            // cleanup task is run every cleanupInterval seconds
            // => unaccessed file should remain at most 2*cleanupInterval seconds
            // (use 3*cleanupInterval to check that we can still access it)
            final long finishTime = System.currentTimeMillis() + 3 * cleanupInterval;

            final ExecutorService executor =
                    Executors.newFixedThreadPool(numberConcurrentGetOperations);
            for (int i = 0; i < numberConcurrentGetOperations; i++) {
                CompletableFuture<Void> getOperation =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        // constantly access key1 so this should not get deleted
                                        while (System.currentTimeMillis() < finishTime) {
                                            get(server, jobId, key1);
                                        }

                                        return null;
                                    } catch (IOException e) {
                                        throw new CompletionException(
                                                new FlinkException("Could not retrieve blob.", e));
                                    }
                                },
                                executor);

                getOperations.add(getOperation);
            }

            FutureUtils.ConjunctFuture<Collection<Void>> filesFuture =
                    FutureUtils.combineAll(getOperations);
            filesFuture.get();

            verifyDeletedEventually(server, jobId, key1, key2);

            // HA content should be unaffected
            verifyContents(server, jobIdHA, keyHA, data);
        }
    }

    @Test
    public void testLocalCleanup() throws Exception {
        final TestingBlobStore blobStore =
                createTestingBlobStoreBuilder()
                        .setDeleteAllFunction(
                                jobDataToDelete ->
                                        fail(
                                                "No deleteAll call is expected to be triggered but was for %s.",
                                                jobDataToDelete))
                        .createTestingBlobStore();
        testSuccessfulCleanup(
                new JobID(),
                (testInstance, jobId, executor) ->
                        testInstance.localCleanupAsync(jobId, executor).join(),
                blobStore);
    }

    @Test
    public void testGlobalCleanup() throws Exception {
        final Set<JobID> actuallyDeletedJobData = new HashSet<>();
        final JobID jobId = new JobID();
        final TestingBlobStore blobStore =
                createTestingBlobStoreBuilder()
                        .setDeleteAllFunction(
                                jobDataToDelete -> {
                                    actuallyDeletedJobData.add(jobDataToDelete);
                                    return true;
                                })
                        .createTestingBlobStore();
        testSuccessfulCleanup(
                jobId,
                (testInstance, jobIdForCleanup, executor) ->
                        testInstance.globalCleanupAsync(jobIdForCleanup, executor).join(),
                blobStore);

        assertThat(actuallyDeletedJobData).containsExactlyInAnyOrder(jobId);
    }

    @Test
    public void testGlobalCleanupUnsuccessfulInBlobStore() throws Exception {
        final TestingBlobStore blobStore =
                createTestingBlobStoreBuilder()
                        .setDeleteAllFunction(jobDataToDelete -> false)
                        .createTestingBlobStore();

        testFailedCleanup(
                new JobID(),
                (testInstance, jobId, executor) ->
                        assertThat(testInstance.globalCleanupAsync(new JobID(), executor))
                                .failsWithin(Duration.ofMillis(100))
                                .withThrowableOfType(ExecutionException.class)
                                .withCauseInstanceOf(IOException.class),
                blobStore);
    }

    @Test
    public void testGlobalCleanupFailureInBlobStore() throws Exception {
        final RuntimeException actualException = new RuntimeException("Expected RuntimeException");
        final TestingBlobStore blobStore =
                createTestingBlobStoreBuilder()
                        .setDeleteAllFunction(
                                jobDataToDelete -> {
                                    throw actualException;
                                })
                        .createTestingBlobStore();

        testFailedCleanup(
                new JobID(),
                (testInstance, jobId, executor) ->
                        assertThat(testInstance.globalCleanupAsync(new JobID(), executor))
                                .failsWithin(Duration.ofMillis(100))
                                .withThrowableOfType(ExecutionException.class)
                                .withCause(actualException),
                blobStore);
    }

    private TestingBlobStoreBuilder createTestingBlobStoreBuilder() {
        return new TestingBlobStoreBuilder()
                .setDeleteFunction(
                        (jobId, blobKey) -> {
                            throw new UnsupportedOperationException(
                                    "Deletion of individual blobs is not supported.");
                        });
    }

    private void testFailedCleanup(
            JobID jobId,
            TriConsumerWithException<BlobServer, JobID, Executor, ? extends Exception> callback,
            BlobStore blobStore)
            throws Exception {
        testCleanup(jobId, callback, blobStore, 2);
    }

    private void testSuccessfulCleanup(
            JobID jobId,
            TriConsumerWithException<BlobServer, JobID, Executor, ? extends Exception> callback,
            BlobStore blobStore)
            throws Exception {
        testCleanup(jobId, callback, blobStore, 0);
    }

    private void testCleanup(
            JobID jobId,
            TriConsumerWithException<BlobServer, JobID, Executor, ? extends Exception> callback,
            BlobStore blobStore,
            int expectedFileCountAfterCleanup)
            throws Exception {
        final JobID otherJobId = new JobID();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (BlobServer testInstance =
                createTestInstance(
                        temporaryFolder.getAbsolutePath(), Integer.MAX_VALUE, blobStore)) {
            testInstance.start();

            final BlobKey transientDataBlobKey =
                    put(testInstance, jobId, createRandomData(), TRANSIENT_BLOB);
            final BlobKey otherTransientDataBlobKey =
                    put(testInstance, otherJobId, createRandomData(), TRANSIENT_BLOB);

            final BlobKey permanentDataBlobKey =
                    put(testInstance, jobId, createRandomData(), PERMANENT_BLOB);
            final BlobKey otherPermanentDataBlobKey =
                    put(testInstance, otherJobId, createRandomData(), PERMANENT_BLOB);

            checkFilesExist(
                    jobId,
                    Arrays.asList(transientDataBlobKey, permanentDataBlobKey),
                    testInstance,
                    true);
            checkFilesExist(
                    otherJobId,
                    Arrays.asList(otherTransientDataBlobKey, otherPermanentDataBlobKey),
                    testInstance,
                    true);

            callback.accept(testInstance, jobId, executorService);

            checkFileCountForJob(expectedFileCountAfterCleanup, jobId, testInstance);
            checkFilesExist(
                    otherJobId,
                    Arrays.asList(otherTransientDataBlobKey, otherPermanentDataBlobKey),
                    testInstance,
                    true);
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();
        }
    }

    @Test
    public void testBlobServerExpiresRecoveredTransientJobBlob() throws Exception {
        runBlobServerExpiresRecoveredTransientBlob(new JobID());
    }

    @Test
    public void testBlobServerExpiresRecoveredTransientNoJobBlob() throws Exception {
        runBlobServerExpiresRecoveredTransientBlob(null);
    }

    private void runBlobServerExpiresRecoveredTransientBlob(@Nullable JobID jobId)
            throws Exception {
        final long cleanupInterval = 1L;

        final TransientBlobKey transientBlobKey =
                TestingBlobUtils.writeTransientBlob(
                        temporaryFolder.toPath(), jobId, new byte[] {1, 2, 3, 4});
        final File blob = BlobUtils.getStorageLocation(temporaryFolder, jobId, transientBlobKey);

        try (final BlobServer blobServer =
                createTestInstance(temporaryFolder.getAbsolutePath(), cleanupInterval)) {
            CommonTestUtils.waitUntilCondition(() -> !blob.exists());
        }
    }

    @Test
    public void testBlobServerRetainsJobs() throws Exception {
        final JobID jobId1 = new JobID();
        final JobID jobId2 = new JobID();

        final byte[] fileContent = {1, 2, 3, 4};
        final PermanentBlobKey blobKey1 =
                TestingBlobUtils.writePermanentBlob(temporaryFolder.toPath(), jobId1, fileContent);
        final PermanentBlobKey blobKey2 =
                TestingBlobUtils.writePermanentBlob(temporaryFolder.toPath(), jobId2, fileContent);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try (final BlobServer blobServer =
                createTestInstance(
                        temporaryFolder.getAbsolutePath(),
                        BlobServerOptions.CLEANUP_INTERVAL.defaultValue())) {
            blobServer.retainJobs(Collections.singleton(jobId1), executorService);

            assertThat(blobServer.getFile(jobId1, blobKey1)).hasBinaryContent(fileContent);
            assertThatThrownBy(() -> blobServer.getFile(jobId2, blobKey2))
                    .isInstanceOf(NoSuchFileException.class);
        } finally {
            assertThat(executorService.shutdownNow()).isEmpty();
        }
    }

    /**
     * Checks how many of the files given by blob keys are accessible.
     *
     * @param jobId ID of a job
     * @param keys blob keys to check
     * @param blobService BLOB store to use
     * @param doThrow whether exceptions should be ignored (<tt>false</tt>), or thrown
     *     (<tt>true</tt>)
     * @return number of files existing at {@link BlobServer#getStorageLocation(JobID, BlobKey)} and
     *     {@link PermanentBlobCache#getStorageLocation(JobID, BlobKey)}, respectively
     */
    public static <T> int checkFilesExist(
            JobID jobId, Collection<? extends BlobKey> keys, T blobService, boolean doThrow)
            throws IOException {

        int numFiles = 0;

        for (BlobKey key : keys) {
            final File storageDir;
            if (blobService instanceof BlobServer) {
                BlobServer server = (BlobServer) blobService;
                storageDir = server.getStorageDir();
            } else if (blobService instanceof PermanentBlobCache) {
                PermanentBlobCache cache = (PermanentBlobCache) blobService;
                storageDir = cache.getStorageDir();
            } else if (blobService instanceof TransientBlobCache) {
                TransientBlobCache cache = (TransientBlobCache) blobService;
                storageDir = cache.getStorageDir();
            } else {
                throw new UnsupportedOperationException(
                        "unsupported BLOB service class: "
                                + blobService.getClass().getCanonicalName());
            }

            final File blobFile =
                    new File(
                            BlobUtils.getStorageLocationPath(
                                    storageDir.getAbsolutePath(), jobId, key));
            if (blobFile.exists()) {
                ++numFiles;
            } else if (doThrow) {
                throw new IOException("File " + blobFile + " does not exist.");
            }
        }

        return numFiles;
    }

    /**
     * Checks how many of the files given by blob keys are accessible.
     *
     * @param expectedCount number of expected files in the blob service for the given job
     * @param jobId ID of a job
     * @param blobService BLOB store to use
     */
    public static void checkFileCountForJob(
            int expectedCount, JobID jobId, PermanentBlobService blobService) throws IOException {

        final File jobDir;
        if (blobService instanceof BlobServer) {
            BlobServer server = (BlobServer) blobService;
            jobDir = server.getStorageLocation(jobId, new PermanentBlobKey()).getParentFile();
        } else {
            PermanentBlobCache cache = (PermanentBlobCache) blobService;
            jobDir = cache.getStorageLocation(jobId, new PermanentBlobKey()).getParentFile();
        }
        File[] blobsForJob = jobDir.listFiles();
        if (blobsForJob == null) {
            if (expectedCount != 0) {
                throw new IOException("File " + jobDir + " does not exist.");
            }
        } else {
            assertThat(blobsForJob.length)
                    .as("Too many/few files in job dir: " + Arrays.asList(blobsForJob))
                    .isEqualTo(expectedCount);
        }
    }
}
