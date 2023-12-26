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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.runtime.state.filesystem.TestFs;
import org.apache.flink.testutils.TestFileSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link FileSystemBlobStore}. */
class FileSystemBlobStoreTest {

    private FileSystemBlobStore testInstance;
    private Path storagePath;

    @BeforeEach
    void createTestInstance(@TempDir Path storagePath) throws IOException {
        this.testInstance = new FileSystemBlobStore(new TestFileSystem(), storagePath.toString());
        this.storagePath = storagePath;
    }

    @AfterEach
    void finalizeTestInstance() throws IOException {
        testInstance.close();
    }

    @Test
    void testSuccessfulPut() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("put");

        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);
        // Blob store operations are creating the base directory on-the-fly
        assertThat(getBlobDirectoryPath()).doesNotExist();

        final boolean successfullyWritten =
                testInstance.put(temporaryFile.toFile(), jobId, blobKey);
        assertThat(successfullyWritten).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile().hasSameTextualContentAs(temporaryFile);
    }

    @Test
    void testMissingFilePut() {
        assertThatThrownBy(
                        () ->
                                testInstance.put(
                                        new File("/not/existing/file"),
                                        new JobID(),
                                        new PermanentBlobKey()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testSuccessfulGet() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();

        final Path targetFile = Files.createTempFile("filesystemblobstoretest-get-target-", "");
        assertThat(targetFile).isEmptyFile();
        final boolean successfullyGet = testInstance.get(jobId, blobKey, targetFile.toFile());
        assertThat(successfullyGet).isTrue();

        assertThat(targetFile).hasSameTextualContentAs(temporaryFile);
    }

    @Test
    void testGetWithWrongJobId() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), new JobID(), blobKey)).isTrue();

        assertThatThrownBy(
                        () ->
                                testInstance.get(
                                        new JobID(),
                                        blobKey,
                                        Files.createTempFile(
                                                        "filesystemblobstoretest-get-with-wrong-jobid-",
                                                        "")
                                                .toFile()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGetWithWrongBlobKey() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("get");

        final JobID jobId = new JobID();
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();

        assertThatThrownBy(
                        () ->
                                testInstance.get(
                                        jobId,
                                        new PermanentBlobKey(),
                                        Files.createTempFile(
                                                        "filesystemblobstoretest-get-with-wrong-blobkey-",
                                                        "")
                                                .toFile()))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testSuccessfulDeleteOnlyBlob() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile();

        final boolean successfullyDeleted = testInstance.delete(jobId, blobKey);

        assertThat(successfullyDeleted).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    @Test
    void testSuccessfulDeleteBlob() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);
        final BlobKey otherBlobKey = new PermanentBlobKey();

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();
        // create another artifact to omit deleting the directory
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, otherBlobKey)).isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId, blobKey)).isNotEmptyFile();
        assertThat(getPath(jobId, otherBlobKey)).isNotEmptyFile();

        final boolean successfullyDeleted = testInstance.delete(jobId, blobKey);

        assertThat(successfullyDeleted).isTrue();
        assertThat(getPath(jobId, otherBlobKey)).exists();
    }

    @Test
    void testDeleteWithNotExistingJobId() {
        assertThat(testInstance.delete(new JobID(), new PermanentBlobKey())).isTrue();
    }

    @Test
    void testDeleteWithNotExistingBlobKey() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();
        final BlobKey blobKey = createPermanentBlobKeyFromFile(temporaryFile);

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, blobKey)).isTrue();
        assertThat(testInstance.delete(jobId, new PermanentBlobKey())).isTrue();
        assertThat(getPath(jobId, blobKey)).exists();
    }

    @Test
    void testDeleteAll() throws IOException {
        final Path temporaryFile = createTemporaryFileWithContent("delete");
        final JobID jobId = new JobID();

        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();
        assertThat(testInstance.put(temporaryFile.toFile(), jobId, new PermanentBlobKey()))
                .isTrue();

        assertThat(getPath(jobId)).isDirectory().exists();
        assertThat(getPath(jobId).toFile().listFiles()).hasSize(2);

        assertThat(testInstance.deleteAll(jobId)).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    @Test
    void testDeleteAllWithNotExistingJobId() {
        final JobID jobId = new JobID();
        assertThat(testInstance.deleteAll(jobId)).isTrue();
        assertThat(getPath(jobId)).doesNotExist();
    }

    private Path createTemporaryFileWithContent(String operationLabel) throws IOException {
        final String actualContent =
                String.format("Content for testing the %s operation", operationLabel);
        final Path temporaryFile =
                Files.createTempFile(
                        String.format("filesystemblobstoretest-%s-", operationLabel), "");
        try (BufferedWriter writer =
                new BufferedWriter(new FileWriter(temporaryFile.toAbsolutePath().toString()))) {
            writer.write(actualContent);
        }

        return temporaryFile;
    }

    private Path getBlobDirectoryPath() {
        return storagePath.resolve(FileSystemBlobStore.BLOB_PATH_NAME);
    }

    private Path getPath(JobID jobId) {
        return getBlobDirectoryPath().resolve(String.format("job_%s", jobId));
    }

    private Path getPath(JobID jobId, BlobKey blobKey) {
        return getPath(jobId).resolve(String.format("blob_%s", blobKey));
    }

    private BlobKey createPermanentBlobKeyFromFile(Path path) throws IOException {
        Preconditions.checkArgument(!Files.isDirectory(path));
        Preconditions.checkArgument(Files.exists(path));

        MessageDigest md = BlobUtils.createMessageDigest();
        try (InputStream is = Files.newInputStream(path.toFile().toPath())) {
            final byte[] buf = new byte[1024];
            int bytesRead = is.read(buf);
            while (bytesRead >= 0) {
                md.update(buf, 0, bytesRead);
                bytesRead = is.read(buf);
            }

            return BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB, md.digest());
        }
    }

    @Test
    void fileSystemBlobStoreCallsSyncOnPut(@TempDir Path storageDirectory) throws IOException {
        final Path blobStoreDirectory = storageDirectory.resolve("blobStore");

        final AtomicReference<TestingLocalDataOutputStream> createdOutputStream =
                new AtomicReference<>();
        final FunctionWithException<org.apache.flink.core.fs.Path, FSDataOutputStream, IOException>
                outputStreamFactory =
                        value -> {
                            final File file = new File(value.toString());
                            FileUtils.createParentDirectories(file);
                            final TestingLocalDataOutputStream outputStream =
                                    new TestingLocalDataOutputStream(file);
                            createdOutputStream.compareAndSet(null, outputStream);
                            return outputStream;
                        };
        try (FileSystemBlobStore fileSystemBlobStore =
                new FileSystemBlobStore(
                        new TestFs(outputStreamFactory), blobStoreDirectory.toString())) {
            final BlobKey blobKey = BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB);
            final File localFile = storageDirectory.resolve("localFile").toFile();
            FileUtils.createParentDirectories(localFile);
            FileUtils.writeStringToFile(localFile, "foobar", StandardCharsets.UTF_8);

            fileSystemBlobStore.put(localFile, new JobID(), blobKey);

            assertThat(createdOutputStream.get().hasSyncBeenCalled()).isTrue();
        }
    }

    private static class TestingLocalDataOutputStream extends LocalDataOutputStream {

        private boolean hasSyncBeenCalled = false;

        private TestingLocalDataOutputStream(File file) throws IOException {
            super(file);
        }

        @Override
        public void sync() throws IOException {
            hasSyncBeenCalled = true;
            super.sync();
        }

        public boolean hasSyncBeenCalled() {
            return hasSyncBeenCalled;
        }
    }
}
