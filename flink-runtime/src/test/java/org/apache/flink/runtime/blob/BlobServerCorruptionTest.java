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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests how GET requests react to corrupt files when downloaded via a {@link BlobServer}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobServerCorruptionTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /**
     * Checks the GET operation fails when the downloaded file (from {@link BlobServer} or HA store)
     * is corrupt, i.e. its content's hash does not match the {@link BlobKey}'s hash.
     */
    @Test
    public void testGetFailsFromCorruptFile() throws IOException {

        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TEMPORARY_FOLDER.newFolder().getAbsolutePath());
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            testGetFailsFromCorruptFile(config, blobStoreService, TEMPORARY_FOLDER.newFolder());
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
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
            assertNotNull(key);

            // delete local file to make sure that the GET requests downloads from HA
            File blobFile = server.getStorageLocation(jobId, key);
            assertTrue(blobFile.delete());

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
}
