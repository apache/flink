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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests how GET requests react to corrupt files when downloaded via a {@link BlobServer}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
class BlobServerCorruptionTest {

    @TempDir private Path tempDir;

    /**
     * Checks the GET operation fails when the downloaded file (from {@link BlobServer} or HA store)
     * is corrupt, i.e. its content's hash does not match the {@link BlobKey}'s hash.
     */
    @Test
    void testGetFailsFromCorruptFile() throws IOException {

        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                TempDirUtils.newFolder(tempDir).getAbsolutePath());
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            TestingBlobHelpers.testGetFailsFromCorruptFile(
                    config, blobStoreService, TempDirUtils.newFolder(tempDir));
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }
}
