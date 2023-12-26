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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Reference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobUtils}. */
class BlobUtilsTest {

    private static final Logger LOG = LoggerFactory.getLogger(BlobUtilsTest.class);

    @TempDir private Path tempDir;

    /**
     * Tests {@link BlobUtils#createBlobStorageDirectory} using {@link
     * BlobServerOptions#STORAGE_DIRECTORY} per default.
     */
    @Test
    void testDefaultBlobStorageDirectory() throws IOException {
        Configuration config = new Configuration();
        String blobStorageDir = TempDirUtils.newFolder(tempDir).getAbsolutePath();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobStorageDir);
        config.setString(CoreOptions.TMP_DIRS, TempDirUtils.newFolder(tempDir).getAbsolutePath());

        File dir = BlobUtils.createBlobStorageDirectory(config, null).deref();
        assertThat(dir.getAbsolutePath()).startsWith(blobStorageDir);
    }

    /** Tests {@link BlobUtils#createBlobStorageDirectory}'s fallback to the fall back directory. */
    @Test
    void testTaskManagerFallbackBlobStorageDirectory1() throws IOException {
        Configuration config = new Configuration();
        final File fallbackDirectory = TempDirUtils.newFile(tempDir, "foobar");

        File dir =
                BlobUtils.createBlobStorageDirectory(config, Reference.borrowed(fallbackDirectory))
                        .deref();
        assertThat(dir).isEqualTo(fallbackDirectory);
    }

    @Test
    void testBlobUtilsFailIfNoStorageDirectoryIsSpecified() {
        assertThatThrownBy(() -> BlobUtils.createBlobStorageDirectory(new Configuration(), null))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testCheckAndDeleteCorruptedBlobsDeletesCorruptedBlobs() throws IOException {
        final JobID jobId = new JobID();

        final byte[] validContent = "valid".getBytes(StandardCharsets.UTF_8);
        final BlobKey validPermanentBlobKey =
                TestingBlobUtils.writePermanentBlob(tempDir, jobId, validContent);
        final BlobKey validTransientBlobKey =
                TestingBlobUtils.writeTransientBlob(tempDir, jobId, validContent);

        final PermanentBlobKey corruptedBlobKey =
                TestingBlobUtils.writePermanentBlob(tempDir, jobId, validContent);
        FileUtils.writeFileUtf8(
                new File(
                        BlobUtils.getStorageLocationPath(
                                tempDir.toString(), jobId, corruptedBlobKey)),
                "corrupted");

        BlobUtils.checkAndDeleteCorruptedBlobs(tempDir, LOG);

        final List<BlobKey> blobKeys =
                BlobUtils.listBlobsInDirectory(tempDir).stream()
                        .map(BlobUtils.Blob::getBlobKey)
                        .collect(Collectors.toList());
        assertThat(blobKeys)
                .containsExactlyInAnyOrder(validPermanentBlobKey, validTransientBlobKey);
    }

    @Test
    void testMoveTempFileToStoreSucceeds() throws IOException {
        final FileSystemBlobStore blobStore =
                new FileSystemBlobStore(
                        new LocalFileSystem(), TempDirUtils.newFolder(tempDir).toString());
        final JobID jobId = new JobID();
        final File storageFile = tempDir.resolve(UUID.randomUUID().toString()).toFile();
        final File incomingFile = TempDirUtils.newFile(tempDir);
        final byte[] fileContent = {1, 2, 3, 4};
        final BlobKey blobKey =
                BlobKey.createKey(
                        BlobKey.BlobType.PERMANENT_BLOB,
                        BlobUtils.createMessageDigest().digest(fileContent));
        Files.write(incomingFile.toPath(), fileContent);

        BlobUtils.moveTempFileToStore(incomingFile, jobId, blobKey, storageFile, LOG, blobStore);

        assertThat(incomingFile).doesNotExist();
        assertThat(storageFile).hasBinaryContent(fileContent);

        final File blobStoreFile = tempDir.resolve(UUID.randomUUID().toString()).toFile();
        assertThat(blobStore.get(jobId, blobKey, blobStoreFile)).isTrue();
        assertThat(blobStoreFile).hasBinaryContent(fileContent);
    }

    @Test
    void testCleanupIfMoveTempFileToStoreFails() throws IOException {
        final File storageFile = tempDir.resolve(UUID.randomUUID().toString()).toFile();

        final File incomingFile = TempDirUtils.newFile(tempDir);
        Files.write(incomingFile.toPath(), new byte[] {1, 2, 3, 4});

        final FileSystemBlobStore blobStore =
                new FileSystemBlobStore(
                        new LocalFileSystem(), TempDirUtils.newFolder(tempDir).toString());

        final JobID jobId = new JobID();
        final BlobKey blobKey = BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB);
        assertThatThrownBy(
                        () ->
                                BlobUtils.internalMoveTempFileToStore(
                                        incomingFile,
                                        jobId,
                                        blobKey,
                                        storageFile,
                                        LOG,
                                        blobStore,
                                        (source, target) -> {
                                            throw new IOException("Test Failure");
                                        }))
                .isInstanceOf(IOException.class);

        assertThatThrownBy(
                        () ->
                                blobStore.get(
                                        jobId,
                                        blobKey,
                                        tempDir.resolve(UUID.randomUUID().toString()).toFile()))
                .isInstanceOf(FileNotFoundException.class);
        assertThat(incomingFile).doesNotExist();
        assertThat(storageFile).doesNotExist();
    }
}
