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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashDifferent;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Helper functions for testing and verifying BLOB related logic. */
public final class TestingBlobHelpers {

    private TestingBlobHelpers() {
        throw new UnsupportedOperationException();
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

    /**
     * Checks the GET operation fails when the downloaded file (from HA store) is corrupt, i.e. its
     * content's hash does not match the {@link BlobKey}'s hash.
     *
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     */
    public static void testGetFailsFromCorruptFile(
            Configuration config, BlobStore blobStore, File blobStorage) throws IOException {

        Random rnd = new Random();
        JobID jobId = new JobID();

        try (BlobServer server = new BlobServer(config, blobStorage, blobStore)) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // put content addressable (like libraries)
            BlobKey key = put(server, jobId, data, PERMANENT_BLOB);
            assertThat(key).isNotNull();

            // delete local file to make sure that the GET requests downloads from HA
            File blobFile = server.getStorageLocation(jobId, key);
            assertThat(blobFile.delete()).isTrue();

            // change HA store file contents to make sure that GET requests fail
            byte[] data2 = Arrays.copyOf(data, data.length);
            data2[0] ^= 1;
            File tmpFile = Files.createTempFile("blob", ".jar").toFile();
            try {
                FileUtils.writeByteArrayToFile(tmpFile, data2);
                blobStore.put(tmpFile, jobId, key);
            } finally {
                //noinspection ResultOfMethodCallIgnored
                tmpFile.delete();
            }

            assertThatThrownBy(() -> get(server, jobId, key))
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(IOException.class, "data corruption"));
        }
    }

    /**
     * Checks the GET operation fails when the downloaded file (from HA store) is corrupt, i.e. its
     * content's hash does not match the {@link BlobKey}'s hash, using a permanent BLOB.
     *
     * @param jobId job ID
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     */
    public static void testGetFailsFromCorruptFile(
            JobID jobId, Configuration config, BlobStore blobStore, File blobStorage)
            throws IOException {

        testGetFailsFromCorruptFile(jobId, PERMANENT_BLOB, true, config, blobStore, blobStorage);
    }

    /**
     * Checks the GET operation fails when the downloaded file (from {@link BlobServer} or HA store)
     * is corrupt, i.e. its content's hash does not match the {@link BlobKey}'s hash.
     *
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param blobType whether the BLOB should become permanent or transient
     * @param corruptOnHAStore whether the file should be corrupt in the HA store (<tt>true</tt>,
     *     required <tt>highAvailability</tt> to be set) or on the {@link BlobServer}'s local store
     *     (<tt>false</tt>)
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     */
    static void testGetFailsFromCorruptFile(
            @Nullable JobID jobId,
            BlobKey.BlobType blobType,
            boolean corruptOnHAStore,
            Configuration config,
            BlobStore blobStore,
            File blobStorage)
            throws IOException {

        assertThat(!corruptOnHAStore || blobType == PERMANENT_BLOB)
                .as("Check HA setup for corrupt HA file")
                .isTrue();

        Random rnd = new Random();

        try (BlobServer server =
                        new BlobServer(config, new File(blobStorage, "server"), blobStore);
                BlobCacheService cache =
                        new BlobCacheService(
                                config,
                                new File(blobStorage, "cache"),
                                corruptOnHAStore ? blobStore : new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // put content addressable (like libraries)
            BlobKey key = put(server, jobId, data, blobType);
            assertThat(key).isNotNull();

            // change server/HA store file contents to make sure that GET requests fail
            byte[] data2 = Arrays.copyOf(data, data.length);
            data2[0] ^= 1;
            if (corruptOnHAStore) {
                File tmpFile = Files.createTempFile("blob", ".jar").toFile();
                try {
                    FileUtils.writeByteArrayToFile(tmpFile, data2);
                    blobStore.put(tmpFile, jobId, key);
                } finally {
                    //noinspection ResultOfMethodCallIgnored
                    tmpFile.delete();
                }

                // delete local (correct) file on server to make sure that the GET request does not
                // fall back to downloading the file from the BlobServer's local store
                File blobFile = server.getStorageLocation(jobId, key);
                assertThat(blobFile.delete()).isTrue();
            } else {
                File blobFile = server.getStorageLocation(jobId, key);
                assertThat(blobFile).exists();
                FileUtils.writeByteArrayToFile(blobFile, data2);
            }

            // issue a GET request that fails
            assertThatThrownBy(() -> get(cache, jobId, key))
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(IOException.class, "data corruption"));
        }
    }

    /**
     * Helper to test that the {@link BlobServer} recovery from its HA store works.
     *
     * <p>Uploads two BLOBs to one {@link BlobServer} and expects a second one to be able to
     * retrieve them via a shared HA store upon request of a {@link BlobCacheService}.
     *
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     * @throws IOException in case of failures
     */
    public static void testBlobServerRecovery(
            final Configuration config, final BlobStore blobStore, final File blobStorage)
            throws Exception {
        final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
        String storagePath =
                config.getString(HighAvailabilityOptions.HA_STORAGE_PATH) + "/" + clusterId;
        Random rand = new Random();

        try (BlobServer server0 =
                        new BlobServer(config, new File(blobStorage, "server0"), blobStore);
                BlobServer server1 =
                        new BlobServer(config, new File(blobStorage, "server1"), blobStore);
                // use VoidBlobStore as the HA store to force download from server[1]'s HA store
                BlobCacheService cache1 =
                        new BlobCacheService(
                                config,
                                new File(blobStorage, "cache1"),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server1.getPort()))) {

            server0.start();
            server1.start();

            // Random data
            byte[] expected = new byte[1024];
            rand.nextBytes(expected);
            byte[] expected2 = Arrays.copyOfRange(expected, 32, 288);

            BlobKey[] keys = new BlobKey[2];
            BlobKey nonHAKey;

            // Put job-related HA data
            JobID[] jobId = new JobID[] {new JobID(), new JobID()};
            keys[0] = put(server0, jobId[0], expected, PERMANENT_BLOB); // Request 1
            keys[1] = put(server0, jobId[1], expected2, PERMANENT_BLOB); // Request 2

            // put non-HA data
            nonHAKey = put(server0, jobId[0], expected2, TRANSIENT_BLOB);
            verifyKeyDifferentHashEquals(keys[1], nonHAKey);

            // check that the storage directory exists
            final Path blobServerPath = new Path(storagePath, "blob");
            FileSystem fs = blobServerPath.getFileSystem();
            assertThat(fs.exists(blobServerPath)).isTrue();

            // Verify HA requests from cache1 (connected to server1) with no immediate access to the
            // file
            verifyContents(cache1, jobId[0], keys[0], expected);
            verifyContents(cache1, jobId[1], keys[1], expected2);

            // Verify non-HA file is not accessible from server1
            verifyDeleted(cache1, jobId[0], nonHAKey);

            // Remove again
            server1.globalCleanupAsync(jobId[0], Executors.directExecutor()).join();
            server1.globalCleanupAsync(jobId[1], Executors.directExecutor()).join();

            // Verify everything is clean
            assertThat(fs.exists(new Path(storagePath))).isTrue();
            if (fs.exists(blobServerPath)) {
                final org.apache.flink.core.fs.FileStatus[] recoveryFiles =
                        fs.listStatus(blobServerPath);
                ArrayList<String> filenames = new ArrayList<>(recoveryFiles.length);
                for (org.apache.flink.core.fs.FileStatus file : recoveryFiles) {
                    filenames.add(file.toString());
                }
                fail("Unclean state backend: %s", filenames);
            }
        }
    }

    /**
     * Helper to test that the {@link BlobServer} recovery from its HA store works.
     *
     * <p>Uploads two BLOBs to one {@link BlobServer} via a {@link BlobCacheService} and expects a
     * second {@link BlobCacheService} to be able to retrieve them from a second {@link BlobServer}
     * that is configured with the same HA store.
     *
     * @param config blob server configuration (including HA settings like {@link
     *     HighAvailabilityOptions#HA_STORAGE_PATH} and {@link
     *     HighAvailabilityOptions#HA_CLUSTER_ID}) used to set up <tt>blobStore</tt>
     * @param blobStore shared HA blob store to use
     * @throws IOException in case of failures
     */
    public static void testBlobCacheRecovery(
            final Configuration config, final BlobStore blobStore, final File blobStorage)
            throws IOException {

        final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
        String storagePath =
                config.getString(HighAvailabilityOptions.HA_STORAGE_PATH) + "/" + clusterId;
        Random rand = new Random();

        try (BlobServer server0 =
                        new BlobServer(config, new File(blobStorage, "server0"), blobStore);
                BlobServer server1 =
                        new BlobServer(config, new File(blobStorage, "server1"), blobStore);
                // use VoidBlobStore as the HA store to force download from each server's HA store
                BlobCacheService cache0 =
                        new BlobCacheService(
                                config,
                                new File(blobStorage, "cache0"),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server0.getPort()));
                BlobCacheService cache1 =
                        new BlobCacheService(
                                config,
                                new File(blobStorage, "cache1"),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server1.getPort()))) {

            server0.start();
            server1.start();

            // Random data
            byte[] expected = new byte[1024];
            rand.nextBytes(expected);
            byte[] expected2 = Arrays.copyOfRange(expected, 32, 288);

            BlobKey[] keys = new BlobKey[2];
            BlobKey nonHAKey;

            // Put job-related HA data
            JobID[] jobId = new JobID[] {new JobID(), new JobID()};
            keys[0] = put(cache0, jobId[0], expected, PERMANENT_BLOB); // Request 1
            keys[1] = put(cache0, jobId[1], expected2, PERMANENT_BLOB); // Request 2

            // put non-HA data
            nonHAKey = put(cache0, jobId[0], expected2, TRANSIENT_BLOB);
            verifyKeyDifferentHashDifferent(keys[0], nonHAKey);
            verifyKeyDifferentHashEquals(keys[1], nonHAKey);

            // check that the storage directory exists
            final Path blobServerPath = new Path(storagePath, "blob");
            FileSystem fs = blobServerPath.getFileSystem();
            assertThat(fs.exists(blobServerPath)).isTrue();

            // Verify HA requests from cache1 (connected to server1) with no immediate access to the
            // file
            verifyContents(cache1, jobId[0], keys[0], expected);
            verifyContents(cache1, jobId[1], keys[1], expected2);

            // Verify non-HA file is not accessible from server1
            verifyDeleted(cache1, jobId[0], nonHAKey);
        }
    }
}
