/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link TransientBlobCache}. */
class TransientBlobCacheTest {

    @Test
    void transientBlobCacheCanServeFilesFromPrepopulatedStorageDirectory(
            @TempDir Path storageDirectory) throws IOException {
        final JobID jobId = new JobID();

        final TransientBlobKey blobKey =
                TestingBlobUtils.writeTransientBlob(
                        storageDirectory, jobId, new byte[] {1, 2, 3, 4});

        try (final TransientBlobCache transientBlobCache =
                new TransientBlobCache(new Configuration(), storageDirectory.toFile(), null)) {
            transientBlobCache.getFile(jobId, blobKey);
        }
    }

    @Test
    void transientBlobCacheChecksForCorruptedBlobsAtStart(@TempDir Path storageDirectory)
            throws IOException {
        final JobID jobId = new JobID();

        final TransientBlobKey blobKey =
                TestingBlobUtils.writeTransientBlob(
                        storageDirectory, jobId, new byte[] {1, 2, 3, 4});
        FileUtils.writeByteArrayToFile(
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDirectory.toString(), jobId, blobKey)),
                new byte[] {4, 3, 2, 1});

        try (final TransientBlobCache transientBlobCache =
                new TransientBlobCache(new Configuration(), storageDirectory.toFile(), null)) {
            assertThatThrownBy(() -> transientBlobCache.getFile(jobId, blobKey))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void transientBlobCacheTimesOutRecoveredBlobs(@TempDir Path storageDirectory) throws Exception {
        final JobID jobId = new JobID();
        final TransientBlobKey transientBlobKey =
                TestingBlobUtils.writeTransientBlob(
                        storageDirectory, jobId, new byte[] {1, 2, 3, 4});
        final File blobFile =
                BlobUtils.getStorageLocation(storageDirectory.toFile(), jobId, transientBlobKey);
        final Configuration configuration = new Configuration();
        final long cleanupInterval = 1L;
        configuration.set(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

        try (final TransientBlobCache transientBlobCache =
                new TransientBlobCache(configuration, storageDirectory.toFile(), null)) {
            CommonTestUtils.waitUntilCondition(() -> !blobFile.exists());
        }
    }
}
