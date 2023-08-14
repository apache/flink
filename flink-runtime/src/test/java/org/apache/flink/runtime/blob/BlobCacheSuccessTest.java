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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;

/** This class contains unit tests for the {@link BlobCacheService}. */
class BlobCacheSuccessTest {

    @TempDir private Path tempDir;

    /**
     * BlobCache with no HA, job-unrelated BLOBs. BLOBs need to be downloaded form a working
     * BlobServer.
     */
    @Test
    void testBlobNoJobCache() throws IOException {
        Configuration config = new Configuration();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, tempDir.toString());

        uploadFileGetTest(config, null, false, false, TRANSIENT_BLOB);
    }

    /**
     * BlobCache with no HA, job-related BLOBS. BLOBs need to be downloaded form a working
     * BlobServer.
     */
    @Test
    void testBlobForJobCache() throws IOException {
        Configuration config = new Configuration();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, tempDir.toString());

        uploadFileGetTest(config, new JobID(), false, false, TRANSIENT_BLOB);
    }

    /**
     * BlobCache is configured in HA mode and the cache can download files from the file system
     * directly and does not need to download BLOBs from the BlobServer which remains active after
     * the BLOB upload. Using job-related BLOBs.
     */
    @Test
    void testBlobForJobCacheHa() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TempDirUtils.newFolder(tempDir).getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());

        uploadFileGetTest(config, new JobID(), true, true, PERMANENT_BLOB);
    }

    /**
     * BlobCache is configured in HA mode and the cache can download files from the file system
     * directly and does not need to download BLOBs from the BlobServer which is shut down after the
     * BLOB upload. Using job-related BLOBs.
     */
    @Test
    void testBlobForJobCacheHa2() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TempDirUtils.newFolder(tempDir).getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());
        uploadFileGetTest(config, new JobID(), false, true, PERMANENT_BLOB);
    }

    /**
     * BlobCache is configured in HA mode but the cache itself cannot access the file system and
     * thus needs to download BLOBs from the BlobServer. Using job-related BLOBs.
     */
    @Test
    void testBlobForJobCacheHaFallback() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TempDirUtils.newFolder(tempDir).getAbsolutePath());
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());

        uploadFileGetTest(config, new JobID(), false, false, PERMANENT_BLOB);
    }

    /**
     * Uploads two different BLOBs to the {@link BlobServer} via a {@link BlobClient} and verifies
     * we can access the files from a {@link BlobCacheService}.
     *
     * @param config configuration to use for the server and cache (the final cache's configuration
     *     will actually get some modifications)
     * @param shutdownServerAfterUpload whether the server should be shut down after uploading the
     *     BLOBs (only useful with HA mode) - this implies that the cache has access to the shared
     *     <tt>HA_STORAGE_PATH</tt>
     * @param cacheHasAccessToFs whether the cache should have access to a shared
     *     <tt>HA_STORAGE_PATH</tt> (only useful with HA mode)
     * @param blobType whether the BLOB should become permanent or transient
     */
    private void uploadFileGetTest(
            final Configuration config,
            @Nullable JobID jobId,
            boolean shutdownServerAfterUpload,
            boolean cacheHasAccessToFs,
            BlobKey.BlobType blobType)
            throws IOException {

        final Configuration cacheConfig = new Configuration(config);
        cacheConfig.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TempDirUtils.newFolder(tempDir).getAbsolutePath());
        if (!cacheHasAccessToFs) {
            // make sure the cache cannot access the HA store directly
            cacheConfig.setString(
                    HighAvailabilityOptions.HA_STORAGE_PATH,
                    TempDirUtils.newFolder(tempDir).getPath() + "/does-not-exist");
        }

        // First create two BLOBs and upload them to BLOB server
        final byte[] data = new byte[128];
        byte[] data2 = Arrays.copyOf(data, data.length);
        data2[0] ^= 1;

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(cacheConfig);
            Tuple2<BlobServer, BlobCacheService> serverAndCache =
                    TestingBlobUtils.createServerAndCache(
                            tempDir, config, cacheConfig, blobStoreService, blobStoreService);
            try (BlobServer server = serverAndCache.f0;
                    BlobCacheService cache = serverAndCache.f1) {
                server.start();

                // Upload BLOBs
                BlobKey key1 = put(server, jobId, data, blobType);
                BlobKey key2 = put(server, jobId, data2, blobType);

                if (shutdownServerAfterUpload) {
                    // Now, shut down the BLOB server, the BLOBs must still be accessible through
                    // the cache.
                    server.close();
                }

                verifyContents(cache, jobId, key1, data);
                verifyContents(cache, jobId, key2, data2);

                if (shutdownServerAfterUpload) {
                    // Now, shut down the BLOB server, the BLOBs must still be accessible through
                    // the cache.
                    server.close();

                    verifyContents(cache, jobId, key1, data);
                    verifyContents(cache, jobId, key2, data2);
                }
            }
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }
}
