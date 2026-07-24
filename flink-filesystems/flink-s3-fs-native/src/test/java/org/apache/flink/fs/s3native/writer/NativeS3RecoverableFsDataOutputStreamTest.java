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

package org.apache.flink.fs.s3native.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the local temp-file handling of {@link NativeS3RecoverableFsDataOutputStream} on the
 * part-upload failure and commit paths.
 */
class NativeS3RecoverableFsDataOutputStreamTest {

    private static final long MIN_PART_SIZE = 5L * 1024 * 1024; // 5 MB
    private static final String KEY = "test/object";
    private static final String UPLOAD_ID = "test-upload-id";

    /**
     * When {@code uploadPart()} fails for a part flushed mid-stream (from {@code write()}), the
     * local temp file must still be removed. Before the fix this path leaked the {@code
     * s3-part-<uuid>} file because {@code Files.delete} ran only after a successful upload; a later
     * {@code close()} would still reclaim it, but the per-part cleanup itself was missing.
     */
    @Test
    void uploadPartFailureFromWriteDeletesTempFile(@TempDir Path tmpDir) throws IOException {
        FailingUploadHelper helper = new FailingUploadHelper();
        NativeS3RecoverableFsDataOutputStream stream =
                new NativeS3RecoverableFsDataOutputStream(
                        helper, KEY, UPLOAD_ID, tmpDir.toString(), MIN_PART_SIZE);

        // Write >= minPartSize so write() flushes a part via uploadCurrentPart(), which fails.
        byte[] payload = new byte[(int) MIN_PART_SIZE];

        assertThatThrownBy(() -> stream.write(payload, 0, payload.length))
                .isInstanceOf(IOException.class);

        assertNoTempFilesRemain(tmpDir, "after uploadPart() failure on the write() path");

        // The mock also fails abort; we only assert no local temp file leaked.
        try {
            stream.close();
        } catch (IOException ignored) {
            // expected: abortMultiPartUpload also fails in this mock
        }
    }

    /**
     * Regression test for the permanent leak on the commit path. {@code closeForCommit()} sets
     * {@code closed = true} before finalizing the last part, so when {@code uploadPart()} throws
     * there, a subsequent {@code close()} short-circuits on its {@code if (!closed)} guard and
     * never deletes the temp file — orphaning it in the shared {@code io.tmp.dirs}. The fix deletes
     * the temp file in a {@code finally} inside {@code uploadCurrentPart()}, so nothing leaks even
     * when the commit-time upload fails. Without the fix this test leaves an {@code s3-part-*} file
     * behind.
     */
    @Test
    void closeForCommitUploadFailureDeletesTempFile(@TempDir Path tmpDir) throws IOException {
        FailingUploadHelper helper = new FailingUploadHelper();
        NativeS3RecoverableFsDataOutputStream stream =
                new NativeS3RecoverableFsDataOutputStream(
                        helper, KEY, UPLOAD_ID, tmpDir.toString(), MIN_PART_SIZE);

        // Write a small amount (< minPartSize) so no part is flushed during write(); the single
        // pending part is uploaded only at commit time, where uploadPart() then fails.
        stream.write(new byte[1024], 0, 1024);

        assertThatThrownBy(stream::closeForCommit).isInstanceOf(IOException.class);

        // The temp file is now orphaned by close()'s `if (!closed)` no-op; the fix must have
        // already removed it during the failed commit upload.
        assertNoTempFilesRemain(tmpDir, "after uploadPart() failure on the closeForCommit() path");
    }

    /** The temp file for a successfully uploaded part is deleted on the normal commit path. */
    @Test
    void closeForCommitSuccessDeletesTempFile(@TempDir Path tmpDir) throws IOException {
        NoopObjectOperations helper = new NoopObjectOperations();
        NativeS3RecoverableFsDataOutputStream stream =
                new NativeS3RecoverableFsDataOutputStream(
                        helper, KEY, UPLOAD_ID, tmpDir.toString(), MIN_PART_SIZE);

        stream.write(new byte[1024], 0, 1024);

        assertThat(stream.closeForCommit()).isNotNull();
        assertNoTempFilesRemain(tmpDir, "after a successful commit upload");
    }

    /**
     * {@code closeForCommit()} with no pending bytes takes the {@code else} branch and deletes the
     * (empty) temp file. With {@code Files.delete} this threw {@link
     * java.nio.file.NoSuchFileException} if the file was already gone; {@code Files.deleteIfExists}
     * makes it idempotent. Here the temp file is removed up front to simulate an
     * external/concurrent cleanup having already deleted it.
     */
    @Test
    void closeForCommitIsIdempotentWhenTempFileMissing(@TempDir Path tmpDir) throws IOException {
        NoopObjectOperations helper = new NoopObjectOperations();
        NativeS3RecoverableFsDataOutputStream stream =
                new NativeS3RecoverableFsDataOutputStream(
                        helper, KEY, UPLOAD_ID, tmpDir.toString(), MIN_PART_SIZE);

        // No write() -> currentPartSize == 0 -> closeForCommit() takes the else (delete) branch.
        File tempFile = findTempFile(tmpDir);
        assertThat(tempFile).isNotNull();
        Files.delete(tempFile.toPath());

        // With the fix (deleteIfExists) this succeeds; without it, it threw NoSuchFileException.
        assertThat(stream.closeForCommit()).isNotNull();
    }

    private static void assertNoTempFilesRemain(Path dir, String when) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            List<Path> remaining =
                    entries.filter(p -> p.getFileName().toString().startsWith("s3-part-"))
                            .collect(Collectors.toList());
            assertThat(remaining).as("temp file s3-part-* should be cleaned up " + when).isEmpty();
        }
    }

    private static File findTempFile(Path dir) throws IOException {
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.filter(p -> p.getFileName().toString().startsWith("s3-part-"))
                    .map(Path::toFile)
                    .findFirst()
                    .orElse(null);
        }
    }

    /** Helper whose uploadPart and abortMultiPartUpload always fail. */
    private static final class FailingUploadHelper extends NativeS3ObjectOperations {
        FailingUploadHelper() {
            super(null, "test-bucket");
        }

        @Override
        public UploadPartResult uploadPart(
                String key, String uploadId, int partNumber, File inputFile, long length)
                throws IOException {
            throw new IOException("simulated S3 503 / network error during uploadPart");
        }

        @Override
        public void abortMultiPartUpload(String key, String uploadId) throws IOException {
            // ignore - tests focus on local temp file cleanup
        }
    }

    /** Helper whose uploadPart succeeds and abort is a no-op. */
    private static final class NoopObjectOperations extends NativeS3ObjectOperations {
        NoopObjectOperations() {
            super(null, "test-bucket");
        }

        @Override
        public UploadPartResult uploadPart(
                String key, String uploadId, int partNumber, File inputFile, long length) {
            return new UploadPartResult(partNumber, "fake-etag");
        }

        @Override
        public void abortMultiPartUpload(String key, String uploadId) {
            // noop
        }
    }
}
