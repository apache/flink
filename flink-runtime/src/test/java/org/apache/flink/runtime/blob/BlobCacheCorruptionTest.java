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
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;

/**
 * Tests how GET requests react to corrupt files when downloaded via a {@link BlobCacheService}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
class BlobCacheCorruptionTest {

    @TempDir private Path tempDir;

    @Test
    void testGetFailsFromCorruptFile1() throws IOException {
        testGetFailsFromCorruptFile(null, TRANSIENT_BLOB, false);
    }

    @Test
    void testGetFailsFromCorruptFile2() throws IOException {
        testGetFailsFromCorruptFile(new JobID(), TRANSIENT_BLOB, false);
    }

    @Test
    void testGetFailsFromCorruptFile3() throws IOException {
        testGetFailsFromCorruptFile(new JobID(), PERMANENT_BLOB, false);
    }

    @Test
    void testGetFailsFromCorruptFile4() throws IOException {
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
                HighAvailabilityOptions.HA_STORAGE_PATH, TempDirUtils.newFolder(tempDir).getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            TestingBlobHelpers.testGetFailsFromCorruptFile(
                    jobId,
                    blobType,
                    corruptOnHAStore,
                    config,
                    blobStoreService,
                    TempDirUtils.newFolder(tempDir));
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }
}
