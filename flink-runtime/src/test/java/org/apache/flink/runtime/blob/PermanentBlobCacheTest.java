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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link PermanentBlobCache}. */
class PermanentBlobCacheTest {

    @Test
    void permanentBlobCacheCanServeFilesFromPrepopulatedStorageDirectory(
            @TempDir Path storageDirectory) throws IOException {

        final JobID jobId = new JobID();

        final byte[] fileContent = "foobar".getBytes(StandardCharsets.UTF_8);
        final PermanentBlobKey blobKey =
                TestingBlobUtils.writePermanentBlob(storageDirectory, jobId, fileContent);
        try (PermanentBlobCache permanentBlobCache =
                new PermanentBlobCache(
                        new Configuration(),
                        storageDirectory.toFile(),
                        new VoidBlobStore(),
                        null)) {
            final File blob = permanentBlobCache.getFile(jobId, blobKey);

            assertThat(Files.readAllBytes(blob.toPath())).isEqualTo(fileContent);
        }
    }

    @Test
    void permanentBlobCacheChecksForCorruptedBlobsAtStart(@TempDir Path storageDirectory)
            throws IOException {
        final JobID jobId = new JobID();
        final PermanentBlobKey blobKey =
                TestingBlobUtils.writePermanentBlob(
                        storageDirectory, jobId, new byte[] {1, 2, 3, 4});

        final File blobFile =
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDirectory.toString(), jobId, blobKey));

        FileUtils.writeByteArrayToFile(blobFile, new byte[] {4, 3, 2, 1});

        try (PermanentBlobCache permanentBlobCache =
                new PermanentBlobCache(
                        new Configuration(),
                        storageDirectory.toFile(),
                        new VoidBlobStore(),
                        null)) {
            assertThatThrownBy(() -> permanentBlobCache.getFile(jobId, blobKey))
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void permanentBlobCacheTimesOutRecoveredBlobs(@TempDir Path storageDirectory) throws Exception {
        final JobID jobId = new JobID();
        final PermanentBlobKey permanentBlobKey =
                TestingBlobUtils.writePermanentBlob(
                        storageDirectory, jobId, new byte[] {1, 2, 3, 4});
        final File blobFile =
                BlobUtils.getStorageLocation(storageDirectory.toFile(), jobId, permanentBlobKey);
        final Configuration configuration = new Configuration();
        final long cleanupInterval = 1L;
        configuration.set(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

        try (final PermanentBlobCache permanentBlobCache =
                new PermanentBlobCache(
                        configuration, storageDirectory.toFile(), new VoidBlobStore(), null)) {
            CommonTestUtils.waitUntilCondition(() -> !blobFile.exists());
        }
    }
}
