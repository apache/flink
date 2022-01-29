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
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Reference;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobUtils}. */
public class BlobUtilsTest extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * Tests {@link BlobUtils#createBlobStorageDirectory} using {@link
     * BlobServerOptions#STORAGE_DIRECTORY} per default.
     */
    @Test
    public void testDefaultBlobStorageDirectory() throws IOException {
        Configuration config = new Configuration();
        String blobStorageDir = temporaryFolder.newFolder().getAbsolutePath();
        config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobStorageDir);
        config.setString(CoreOptions.TMP_DIRS, temporaryFolder.newFolder().getAbsolutePath());

        File dir = BlobUtils.createBlobStorageDirectory(config, null).deref();
        assertThat(dir.getAbsolutePath()).startsWith(blobStorageDir);
    }

    /** Tests {@link BlobUtils#createBlobStorageDirectory}'s fallback to the fall back directory. */
    @Test
    public void testTaskManagerFallbackBlobStorageDirectory1() throws IOException {
        Configuration config = new Configuration();
        final File fallbackDirectory = new File(temporaryFolder.newFolder(), "foobar");

        File dir =
                BlobUtils.createBlobStorageDirectory(config, Reference.borrowed(fallbackDirectory))
                        .deref();
        assertThat(dir).isEqualTo(fallbackDirectory);
    }

    @Test(expected = IOException.class)
    public void testBlobUtilsFailIfNoStorageDirectoryIsSpecified() throws IOException {
        BlobUtils.createBlobStorageDirectory(new Configuration(), null);
    }

    @Test
    public void testCheckAndDeleteCorruptedBlobsDeletesCorruptedBlobs() throws IOException {
        final File storageDir = temporaryFolder.newFolder();
        final JobID jobId = new JobID();

        final byte[] validContent = "valid".getBytes(StandardCharsets.UTF_8);
        final BlobKey validPermanentBlobKey =
                TestingBlobUtils.writePermanentBlob(storageDir.toPath(), jobId, validContent);
        final BlobKey validTransientBlobKey =
                TestingBlobUtils.writeTransientBlob(storageDir.toPath(), jobId, validContent);

        final PermanentBlobKey corruptedBlobKey =
                TestingBlobUtils.writePermanentBlob(storageDir.toPath(), jobId, validContent);
        FileUtils.writeFileUtf8(
                new File(
                        BlobUtils.getStorageLocationPath(
                                storageDir.getAbsolutePath(), jobId, corruptedBlobKey)),
                "corrupted");

        BlobUtils.checkAndDeleteCorruptedBlobs(storageDir.toPath(), log);

        final List<BlobKey> blobKeys =
                BlobUtils.listBlobsInDirectory(storageDir.toPath()).stream()
                        .map(BlobUtils.Blob::getBlobKey)
                        .collect(Collectors.toList());
        assertThat(blobKeys)
                .containsExactlyInAnyOrder(validPermanentBlobKey, validTransientBlobKey);
    }

    @Test
    public void testMoveTempFileToStoreSucceeds() throws IOException {
        final FileSystemBlobStore blobStore =
                new FileSystemBlobStore(
                        new LocalFileSystem(), temporaryFolder.newFolder().toString());
        final JobID jobId = new JobID();
        final File storageFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());
        final File incomingFile = temporaryFolder.newFile();
        final byte[] fileContent = {1, 2, 3, 4};
        final BlobKey blobKey =
                BlobKey.createKey(
                        BlobKey.BlobType.PERMANENT_BLOB,
                        BlobUtils.createMessageDigest().digest(fileContent));
        Files.write(incomingFile.toPath(), fileContent);

        BlobUtils.moveTempFileToStore(incomingFile, jobId, blobKey, storageFile, log, blobStore);

        assertThat(incomingFile).doesNotExist();
        assertThat(storageFile).hasBinaryContent(fileContent);

        final File blobStoreFile =
                new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());
        assertThat(blobStore.get(jobId, blobKey, blobStoreFile)).isTrue();
        assertThat(blobStoreFile).hasBinaryContent(fileContent);
    }

    @Test
    public void testCleanupIfMoveTempFileToStoreFails() throws IOException {
        final File storageFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString());

        final File incomingFile = temporaryFolder.newFile();
        Files.write(incomingFile.toPath(), new byte[] {1, 2, 3, 4});

        final FileSystemBlobStore blobStore =
                new FileSystemBlobStore(
                        new LocalFileSystem(), temporaryFolder.newFolder().getAbsolutePath());

        final JobID jobId = new JobID();
        final BlobKey blobKey = BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB);
        assertThatThrownBy(
                        () ->
                                BlobUtils.internalMoveTempFileToStore(
                                        incomingFile,
                                        jobId,
                                        blobKey,
                                        storageFile,
                                        log,
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
                                        new File(
                                                temporaryFolder.getRoot(),
                                                UUID.randomUUID().toString())))
                .isInstanceOf(FileNotFoundException.class);
        assertThat(incomingFile).doesNotExist();
        assertThat(storageFile).doesNotExist();
    }
}
