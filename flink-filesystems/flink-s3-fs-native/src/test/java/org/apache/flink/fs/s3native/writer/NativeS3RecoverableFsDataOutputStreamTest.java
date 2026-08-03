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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

    /** The two call sites that can hit a failing {@code uploadPart()}. */
    enum UploadFailurePath {
        WRITE,
        CLOSE_FOR_COMMIT
    }

    /**
     * When {@code uploadPart()} fails, whether flushed mid-stream from {@code write()} or at commit
     * time from {@code closeForCommit()}, the temp file is intentionally retained so the upload
     * exception propagates unmasked, and the subsequent {@code close()} reclaims it.
     */
    @ParameterizedTest
    @EnumSource(UploadFailurePath.class)
    void uploadPartFailureIsReclaimedByClose(UploadFailurePath path, @TempDir Path tmpDir)
            throws IOException {
        FailingUploadHelper helper = new FailingUploadHelper();
        NativeS3RecoverableFsDataOutputStream stream =
                new NativeS3RecoverableFsDataOutputStream(
                        helper, KEY, UPLOAD_ID, tmpDir.toString(), MIN_PART_SIZE);

        if (path == UploadFailurePath.WRITE) {
            // Write >= minPartSize so write() flushes the part immediately, which fails.
            byte[] payload = new byte[(int) MIN_PART_SIZE];
            assertThatThrownBy(() -> stream.write(payload, 0, payload.length))
                    .isInstanceOf(IOException.class);
        } else {
            // Write < minPartSize so the single pending part is uploaded only at commit time.
            stream.write(new byte[1024], 0, 1024);
            assertThatThrownBy(stream::closeForCommit).isInstanceOf(IOException.class);
        }

        assertThat(findTempFile(tmpDir))
                .as("temp file is reclaimed by close(), not eagerly")
                .isNotNull();

        stream.close();

        assertNoTempFilesRemain(tmpDir, "after close() following an uploadPart() failure");
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

    /** {@code closeForCommit()} succeeds even when the temp file was already removed. */
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
