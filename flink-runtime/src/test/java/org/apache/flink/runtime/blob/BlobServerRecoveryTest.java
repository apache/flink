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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for the recovery of files of a {@link BlobServer} from a HA store. */
class BlobServerRecoveryTest {

    @TempDir private java.nio.file.Path tempDir;

    /**
     * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from
     * any participating BlobServer.
     */
    @Test
    public void testBlobServerRecovery() throws Exception {
        Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            TestingBlobHelpers.testBlobServerRecovery(
                    config, blobStoreService, TempDirUtils.newFolder(tempDir));
        } finally {
            if (blobStoreService != null) {
                blobStoreService.cleanupAllData();
                blobStoreService.close();
            }
        }
    }
}
