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
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests how GET requests react to corrupt files when downloaded via a {@link BlobCacheService}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobCacheCorruptionTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void testGetFailsFromCorruptFile1() throws IOException {
        testGetFailsFromCorruptFile(null, TRANSIENT_BLOB, false);
    }

    @Test
    public void testGetFailsFromCorruptFile2() throws IOException {
        testGetFailsFromCorruptFile(new JobID(), TRANSIENT_BLOB, false);
    }

    @Test
    public void testGetFailsFromCorruptFile3() throws IOException {
        testGetFailsFromCorruptFile(new JobID(), PERMANENT_BLOB, false);
    }

    @Test
    public void testGetFailsFromCorruptFile4() throws IOException {
        testGetFailsFromCorruptFile(new JobID(), PERMANENT_BLOB, true);
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
     */
    private void testGetFailsFromCorruptFile(
            final JobID jobId, BlobKey.BlobType blobType, boolean corruptOnHAStore)
            throws IOException {

        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            testGetFailsFromCorruptFile(
                    jobId,
                    blobType,
                    corruptOnHAStore,
                    config,
                    blobStoreService,
                    TEMPORARY_FOLDER.newFolder());
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
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
    private static void testGetFailsFromCorruptFile(
            @Nullable JobID jobId,
            BlobKey.BlobType blobType,
            boolean corruptOnHAStore,
            Configuration config,
            BlobStore blobStore,
            File blobStorage)
            throws IOException {

        assertTrue(
                "corrupt HA file requires a HA setup",
                !corruptOnHAStore || blobType == PERMANENT_BLOB);

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
            assertNotNull(key);

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
                assertTrue(blobFile.delete());
            } else {
                File blobFile = server.getStorageLocation(jobId, key);
                assertTrue(blobFile.exists());
                FileUtils.writeByteArrayToFile(blobFile, data2);
            }

            // issue a GET request that fails
            assertThatThrownBy(() -> get(cache, jobId, key))
                    .satisfies(
                            FlinkAssertions.anyCauseMatches(IOException.class, "data corruption"));
        }
    }
}
