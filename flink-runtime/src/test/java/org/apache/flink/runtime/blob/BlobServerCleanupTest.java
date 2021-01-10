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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCachePutTest.verifyDeletedEventually;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** A few tests for the cleanup of transient BLOBs at the {@link BlobServer}. */
public class BlobServerCleanupTest extends TestLogger {

    private final Random rnd = new Random();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

        byte[] data = new byte[2000000];
        rnd.nextBytes(data);
        byte[] data2 = Arrays.copyOfRange(data, 10, 54);

        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
        config.setLong(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

        long cleanupLowerBound;

        try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

            ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> transientBlobExpiryTimes =
                    server.getBlobExpiryTimes();

            server.start();

            // after put(), files are cached for the given TTL
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            final TransientBlobKey key1 =
                    (TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
            final Long key1ExpiryAfterPut = transientBlobExpiryTimes.get(Tuple2.of(jobId, key1));
            assertThat(key1ExpiryAfterPut, greaterThanOrEqualTo(cleanupLowerBound));

            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            final TransientBlobKey key2 =
                    (TransientBlobKey) put(server, jobId, data2, TRANSIENT_BLOB);
            final Long key2ExpiryAfterPut = transientBlobExpiryTimes.get(Tuple2.of(jobId, key2));
            assertThat(key2ExpiryAfterPut, greaterThanOrEqualTo(cleanupLowerBound));

            // check that HA contents are not cleaned up
            final JobID jobIdHA = (jobId == null) ? new JobID() : jobId;
            final BlobKey keyHA = put(server, jobIdHA, data, PERMANENT_BLOB);

            // access key1, verify expiry times (delay at least 1ms to also verify key2 expiry is
            // unchanged)
            Thread.sleep(1);
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            verifyContents(server, jobId, key1, data);
            final Long key1ExpiryAfterGet = transientBlobExpiryTimes.get(Tuple2.of(jobId, key1));
            assertThat(key1ExpiryAfterGet, greaterThan(key1ExpiryAfterPut));
            assertThat(key1ExpiryAfterGet, greaterThanOrEqualTo(cleanupLowerBound));
            assertEquals(key2ExpiryAfterPut, transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)));

            // access key2, verify expiry times (delay at least 1ms to also verify key1 expiry is
            // unchanged)
            Thread.sleep(1);
            cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
            verifyContents(server, jobId, key2, data2);
            assertEquals(key1ExpiryAfterGet, transientBlobExpiryTimes.get(Tuple2.of(jobId, key1)));
            assertThat(
                    transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)),
                    greaterThan(key2ExpiryAfterPut));
            assertThat(
                    transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)),
                    greaterThanOrEqualTo(cleanupLowerBound));

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
            assertEquals(
                    "Too many/few files in job dir: " + Arrays.asList(blobsForJob).toString(),
                    expectedCount,
                    blobsForJob.length);
        }
    }
}
