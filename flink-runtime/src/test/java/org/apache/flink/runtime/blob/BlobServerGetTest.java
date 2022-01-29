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
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKeyTest.verifyKeyDifferentHashEquals;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobUtils.JOB_DIR_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobServer}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests by {@link
 * BlobServerPutTest}.
 */
public class BlobServerGetTest extends TestLogger {

    private final Random rnd = new Random();

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testGetTransientFailsDuringLookup1() throws IOException {
        testGetFailsDuringLookup(null, new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testGetTransientFailsDuringLookup2() throws IOException {
        testGetFailsDuringLookup(new JobID(), new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testGetTransientFailsDuringLookup3() throws IOException {
        testGetFailsDuringLookup(new JobID(), null, TRANSIENT_BLOB);
    }

    @Test
    public void testGetPermanentFailsDuringLookup() throws IOException {
        testGetFailsDuringLookup(new JobID(), new JobID(), PERMANENT_BLOB);
    }

    /**
     * Checks the correct result if a GET operation fails during the lookup of the file.
     *
     * @param jobId1 first job ID or <tt>null</tt> if job-unrelated
     * @param jobId2 second job ID different to <tt>jobId1</tt>
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testGetFailsDuringLookup(
            @Nullable final JobID jobId1, @Nullable final JobID jobId2, BlobKey.BlobType blobType)
            throws IOException {
        final Configuration config = new Configuration();

        try (BlobServer server =
                new BlobServer(config, temporaryFolder.newFolder(), new VoidBlobStore())) {

            server.start();

            byte[] data = new byte[2000000];
            rnd.nextBytes(data);

            // put content addressable (like libraries)
            BlobKey key = put(server, jobId1, data, blobType);
            assertNotNull(key);

            // delete file to make sure that GET requests fail
            File blobFile = server.getStorageLocation(jobId1, key);
            assertTrue(blobFile.delete());

            // issue a GET request that fails
            verifyDeleted(server, jobId1, key);

            // add the same data under a second jobId
            BlobKey key2 = put(server, jobId2, data, blobType);
            assertNotNull(key2);
            verifyKeyDifferentHashEquals(key, key2);

            // request for jobId2 should succeed
            get(server, jobId2, key2);
            // request for jobId1 should still fail
            verifyDeleted(server, jobId1, key);

            // same checks as for jobId1 but for jobId2 should also work:
            blobFile = server.getStorageLocation(jobId2, key2);
            assertTrue(blobFile.delete());
            verifyDeleted(server, jobId2, key2);
        }
    }

    /**
     * Retrieves a BLOB from the HA store to a {@link BlobServer} which cannot create incoming
     * files. File transfers should fail.
     */
    @Test
    public void testGetFailsIncomingForJobHa() throws IOException {
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        final JobID jobId = new JobID();

        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getPath());

        BlobStoreService blobStore = null;

        try {
            blobStore = BlobUtils.createBlobStoreFromConfig(config);

            File tempFileDir = null;
            try (BlobServer server =
                    new BlobServer(config, temporaryFolder.newFolder(), blobStore)) {

                server.start();

                // store the data on the server (and blobStore), remove from local store
                byte[] data = new byte[2000000];
                rnd.nextBytes(data);
                BlobKey blobKey = put(server, jobId, data, PERMANENT_BLOB);
                assertTrue(server.getStorageLocation(jobId, blobKey).delete());

                // make sure the blob server cannot create any files in its storage dir
                tempFileDir = server.createTemporaryFilename().getParentFile();
                assertTrue(tempFileDir.setExecutable(true, false));
                assertTrue(tempFileDir.setReadable(true, false));
                assertTrue(tempFileDir.setWritable(false, false));

                // request the file from the BlobStore
                try {
                    assertThatThrownBy(() -> get(server, jobId, blobKey))
                            .satisfies(
                                    FlinkAssertions.anyCauseMatches(
                                            IOException.class, "Permission denied"));
                } finally {
                    HashSet<String> expectedDirs = new HashSet<>();
                    expectedDirs.add("incoming");
                    expectedDirs.add(JOB_DIR_PREFIX + jobId);
                    // only the incoming and job directory should exist (no job directory!)
                    File storageDir = tempFileDir.getParentFile();
                    String[] actualDirs = storageDir.list();
                    assertNotNull(actualDirs);
                    assertEquals(expectedDirs, new HashSet<>(Arrays.asList(actualDirs)));

                    // job directory should be empty
                    File jobDir = new File(tempFileDir.getParentFile(), JOB_DIR_PREFIX + jobId);
                    assertArrayEquals(new String[] {}, jobDir.list());
                }
            } finally {
                // set writable again to make sure we can remove the directory
                if (tempFileDir != null) {
                    //noinspection ResultOfMethodCallIgnored
                    tempFileDir.setWritable(true, false);
                }
            }
        } finally {
            if (blobStore != null) {
                blobStore.closeAndCleanupAllData();
            }
        }
    }

    /**
     * Retrieves a BLOB from the HA store to a {@link BlobServer} which cannot create the final
     * storage file. File transfers should fail.
     */
    @Test
    public void testGetFailsStoreForJobHa() throws IOException {
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        final JobID jobId = new JobID();

        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getPath());

        BlobStoreService blobStore = null;

        try {
            blobStore = BlobUtils.createBlobStoreFromConfig(config);

            File jobStoreDir = null;
            try (BlobServer server =
                    new BlobServer(config, temporaryFolder.newFolder(), blobStore)) {

                server.start();

                // store the data on the server (and blobStore), remove from local store
                byte[] data = new byte[2000000];
                rnd.nextBytes(data);
                BlobKey blobKey = put(server, jobId, data, PERMANENT_BLOB);
                assertTrue(server.getStorageLocation(jobId, blobKey).delete());

                // make sure the blob cache cannot create any files in its storage dir
                jobStoreDir = server.getStorageLocation(jobId, blobKey).getParentFile();
                assertTrue(jobStoreDir.setExecutable(true, false));
                assertTrue(jobStoreDir.setReadable(true, false));
                assertTrue(jobStoreDir.setWritable(false, false));

                // request the file from the BlobStore
                try {
                    assertThatThrownBy(() -> get(server, jobId, blobKey))
                            .isInstanceOf(AccessDeniedException.class);
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
        } finally {
            if (blobStore != null) {
                blobStore.closeAndCleanupAllData();
            }
        }
    }

    /**
     * Retrieves a BLOB from the HA store to a {@link BlobServer} whose HA store does not contain
     * the file. File transfers should fail.
     */
    @Test
    public void testGetFailsHaStoreForJobHa() throws IOException {
        final JobID jobId = new JobID();

        final Configuration config = new Configuration();

        try (BlobServer server =
                new BlobServer(config, temporaryFolder.newFolder(), new VoidBlobStore())) {

            server.start();

            // store the data on the server (and blobStore), remove from local store
            byte[] data = new byte[2000000];
            rnd.nextBytes(data);
            BlobKey blobKey = put(server, jobId, data, PERMANENT_BLOB);
            assertTrue(server.getStorageLocation(jobId, blobKey).delete());

            File tempFileDir = server.createTemporaryFilename().getParentFile();

            // request the file from the BlobStore
            try {
                assertThatThrownBy(() -> get(server, jobId, blobKey))
                        .isInstanceOf(NoSuchFileException.class);
            } finally {
                HashSet<String> expectedDirs = new HashSet<>();
                expectedDirs.add("incoming");
                expectedDirs.add(JOB_DIR_PREFIX + jobId);
                // only the incoming and job directory should exist (no job directory!)
                File storageDir = tempFileDir.getParentFile();
                String[] actualDirs = storageDir.list();
                assertNotNull(actualDirs);
                assertEquals(expectedDirs, new HashSet<>(Arrays.asList(actualDirs)));

                // job directory should be empty
                File jobDir = new File(tempFileDir.getParentFile(), JOB_DIR_PREFIX + jobId);
                assertArrayEquals(new String[] {}, jobDir.list());
            }
        }
    }

    @Test
    public void testConcurrentGetOperationsNoJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentGetOperations(null, TRANSIENT_BLOB);
    }

    @Test
    public void testConcurrentGetOperationsForJob()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentGetOperations(new JobID(), TRANSIENT_BLOB);
    }

    @Test
    public void testConcurrentGetOperationsForJobHa()
            throws IOException, ExecutionException, InterruptedException {
        testConcurrentGetOperations(new JobID(), PERMANENT_BLOB);
    }

    @Test
    public void testGetChecksForCorruptionInPermanentBlobInCaseOfRestart() throws IOException {
        runGetChecksForCorruptionInCaseOfRestartTest(PERMANENT_BLOB);
    }

    @Test
    public void testGetChecksForCorruptionInTransientBlobInCaseOfRestart() throws IOException {
        runGetChecksForCorruptionInCaseOfRestartTest(TRANSIENT_BLOB);
    }

    private void runGetChecksForCorruptionInCaseOfRestartTest(BlobKey.BlobType blobType)
            throws IOException {
        final JobID jobId = JobID.generate();
        final byte[] data = new byte[] {1, 2, 3};
        final byte[] corruptedData = new byte[] {3, 2, 1};

        final File storageDir = temporaryFolder.newFolder();
        try (final BlobServer blobServer =
                new BlobServer(
                        new Configuration(), Reference.borrowed(storageDir), new VoidBlobStore())) {
            final BlobKey blobKey = put(blobServer, jobId, data, blobType);

            blobServer.close();

            final File blob = blobServer.getStorageLocation(jobId, blobKey);
            // corrupt the file
            FileUtils.writeByteArrayToFile(blob, corruptedData);

            try (final BlobServer restartedBlobServer =
                    new BlobServer(
                            new Configuration(),
                            Reference.borrowed(storageDir),
                            new VoidBlobStore())) {
                try {
                    // the file should be corrupted now
                    get(restartedBlobServer, jobId, blobKey);
                    fail("Expected to fail with an IOException because the file is corrupted.");
                } catch (IOException expected) {
                    // expected :-)
                }
            }
        }
    }

    @Test
    public void testGetReDownloadsCorruptedPermanentBlobFromBlobStoreInCaseOfRestart()
            throws IOException {
        final JobID jobId = JobID.generate();
        final byte[] data = new byte[] {1, 2, 3};
        final byte[] corruptedData = new byte[] {3, 2, 1};

        final File storageDir = temporaryFolder.newFolder();
        final OneShotLatch getCalled = new OneShotLatch();
        final BlobStore blobStore =
                new TestingBlobStoreBuilder()
                        .setGetFunction(
                                (jobID, blobKey, file) -> {
                                    getCalled.trigger();
                                    FileUtils.writeByteArrayToFile(file, data);
                                    return true;
                                })
                        .createTestingBlobStore();
        try (final BlobServer blobServer =
                new BlobServer(new Configuration(), Reference.borrowed(storageDir), blobStore)) {
            final BlobKey blobKey = put(blobServer, jobId, data, PERMANENT_BLOB);

            blobServer.close();

            final File blob = blobServer.getStorageLocation(jobId, blobKey);
            // corrupt the file
            FileUtils.writeByteArrayToFile(blob, corruptedData);

            try (final BlobServer restartedBlobServer =
                    new BlobServer(
                            new Configuration(), Reference.borrowed(storageDir), blobStore)) {
                // we should re-download the file from the BlobStore
                final File file = get(restartedBlobServer, jobId, blobKey);
                validateGetAndClose(new FileInputStream(file), data);
                assertThat(getCalled.isTriggered()).isTrue();
            }
        }
    }

    /**
     * [FLINK-6020] Tests that concurrent get operations don't concurrently access the BlobStore to
     * download a blob.
     *
     * @param jobId job ID to use (or <tt>null</tt> if job-unrelated)
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void testConcurrentGetOperations(
            @Nullable final JobID jobId, final BlobKey.BlobType blobType)
            throws IOException, InterruptedException, ExecutionException {
        final byte[] data = {1, 2, 3, 4, 99, 42};

        final BlobStore blobStore =
                new TestingBlobStoreBuilder()
                        .setGetFunction(
                                (jobID, blobKey, file) -> {
                                    FileUtils.writeByteArrayToFile(file, data);
                                    return true;
                                })
                        .createTestingBlobStore();

        final int numberConcurrentGetOperations = 3;
        final List<CompletableFuture<File>> getOperations =
                new ArrayList<>(numberConcurrentGetOperations);

        final ExecutorService executor =
                Executors.newFixedThreadPool(numberConcurrentGetOperations);

        try (final BlobServer server =
                new BlobServer(new Configuration(), temporaryFolder.newFolder(), blobStore)) {

            server.start();

            // upload data first
            final BlobKey blobKey = put(server, jobId, data, blobType);

            // now try accessing it concurrently (only HA mode will be able to retrieve it from HA
            // store!)
            if (blobType == PERMANENT_BLOB) {
                // remove local copy so that a transfer from HA store takes place
                assertTrue(server.getStorageLocation(jobId, blobKey).delete());
            }
            for (int i = 0; i < numberConcurrentGetOperations; i++) {
                CompletableFuture<File> getOperation =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        File file = get(server, jobId, blobKey);
                                        // check that we have read the right data
                                        validateGetAndClose(new FileInputStream(file), data);
                                        return file;
                                    } catch (IOException e) {
                                        throw new CompletionException(
                                                new FlinkException(
                                                        "Could not read blob for key "
                                                                + blobKey
                                                                + '.',
                                                        e));
                                    }
                                },
                                executor);

                getOperations.add(getOperation);
            }

            CompletableFuture<Collection<File>> filesFuture = FutureUtils.combineAll(getOperations);
            filesFuture.get();
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Retrieves the given blob.
     *
     * <p>Note that if a {@link BlobCacheService} is used, it may try to access the {@link
     * BlobServer} to retrieve the blob.
     *
     * @param service BLOB client to use for connecting to the BLOB service
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key key identifying the BLOB to request
     */
    static File get(BlobService service, @Nullable JobID jobId, BlobKey key) throws IOException {
        if (key instanceof PermanentBlobKey) {
            return service.getPermanentBlobService().getFile(jobId, (PermanentBlobKey) key);
        } else if (jobId == null) {
            return service.getTransientBlobService().getFile((TransientBlobKey) key);
        } else {
            return service.getTransientBlobService().getFile(jobId, (TransientBlobKey) key);
        }
    }

    /**
     * Checks that the given blob does not exist anymore by trying to access it.
     *
     * <p>Note that if a {@link BlobCacheService} is used, it may try to access the {@link
     * BlobServer} to retrieve the blob.
     *
     * @param service BLOB client to use for connecting to the BLOB service
     * @param jobId job ID or <tt>null</tt> if job-unrelated
     * @param key key identifying the BLOB to request
     */
    static void verifyDeleted(BlobService service, @Nullable JobID jobId, BlobKey key)
            throws IOException {
        try {
            get(service, jobId, key);
            fail("File " + jobId + "/" + key + " should have been deleted.");
        } catch (IOException e) {
            // expected
        }
    }
}
