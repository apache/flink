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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link NativeS3RecoverableFsDataOutputStream}. */
class NativeS3RecoverableFsDataOutputStreamTest {

    private static final String KEY = "out.txt";
    private static final long MIN_PART_SIZE = 10L;

    @TempDir Path tmp;

    InMemoryNativeS3Operations s3;
    String uploadId;
    NativeS3RecoverableFsDataOutputStream stream;

    @BeforeEach
    void setUp() throws IOException {
        s3 = new InMemoryNativeS3Operations();
        uploadId = s3.startMultiPartUpload(KEY);
        stream = newStream(s3, uploadId);
        stream.write(bytes('A', 5), 0, 5); // < MIN_PART_SIZE, so it is uploaded during commit
    }

    @Test
    void closeForCommitAbortsMultipartUploadWhenPartUploadFails() throws Exception {
        s3.failUploadPart = true;

        assertThatThrownBy(stream::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected uploadPart failure");

        assertThat(s3.abortAttempts)
                .as("closeForCommit must abort the upload on failure")
                .isEqualTo(1);
        assertThat(s3.openMultipartUploads)
                .as("the multipart upload must not leak after a failed commit")
                .doesNotContainKey(uploadId);
        assertThat(countLocalFilesIn(tmp)).as("the local temp file must be cleaned up").isZero();
    }

    @Test
    void closeForCommitSurfacesAbortFailureWhenBothUploadAndAbortFail() throws Exception {
        s3.failUploadPart = true;
        s3.failAbortMultiPartUpload = true;

        assertThatThrownBy(stream::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected uploadPart failure")
                .satisfies(
                        t ->
                                assertThat(t.getSuppressed())
                                        .as("the abort failure must be surfaced, not swallowed")
                                        .anySatisfy(
                                                s ->
                                                        assertThat(s)
                                                                .hasMessageContaining(
                                                                        "injected abort failure")));

        assertThat(s3.abortAttempts).isEqualTo(1);
    }

    @Test
    void closeSurfacesAbortFailureInsteadOfSwallowingIt() throws Exception {
        s3.failAbortMultiPartUpload = true;

        assertThatThrownBy(stream::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected abort failure");

        assertThat(s3.abortAttempts).isEqualTo(1);
        assertThat(countLocalFilesIn(tmp))
                .as("local resources are still released even when the abort fails")
                .isZero();
    }

    /** An abnormal {@code close()} aborts the upload and releases local state. */
    @Test
    void closeAbortsMultipartUploadOnAbnormalClose() throws Exception {
        stream.close();

        assertThat(s3.abortAttempts).isEqualTo(1);
        assertThat(s3.openMultipartUploads).doesNotContainKey(uploadId);
        assertThat(countLocalFilesIn(tmp)).isZero();
    }

    @Test
    void closeSurfacesTempFileDeletionFailure() throws Exception {
        NativeS3RecoverableFsDataOutputStream failingStream = newFailingDeleteStream();
        failingStream.write(bytes('A', 5), 0, 5);

        assertThatThrownBy(failingStream::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected temp-file delete failure");

        assertThat(s3.abortAttempts)
                .as("abort is still attempted despite delete failure")
                .isEqualTo(1);
    }

    @Test
    void closeForCommitDoesNotAbortOnSuccess() throws Exception {
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();

        assertThat(s3.abortAttempts).as("a successful commit must not abort the upload").isZero();
        assertThat(s3.openMultipartUploads)
                .as("the upload stays open until the committer commits it")
                .containsKey(uploadId);

        committer.commit();

        assertThat(s3.committedObjects.get(KEY)).containsExactly(bytes('A', 5));
        assertThat(s3.openMultipartUploads).doesNotContainKey(uploadId);
    }

    @Test
    void closeAfterSuccessfulCloseForCommitIsNoOp() throws Exception {
        RecoverableFsDataOutputStream.Committer committer = stream.closeForCommit();
        stream.close();

        assertThat(s3.abortAttempts)
                .as("close() after a successful commit must not abort the pending upload")
                .isZero();
        assertThat(s3.openMultipartUploads).containsKey(uploadId);

        committer.commit();
        assertThat(s3.committedObjects.get(KEY)).containsExactly(bytes('A', 5));
    }

    @Test
    void closeForCommitDeletesTempFileOnSuccess() throws Exception {
        assertThat(countLocalFilesIn(tmp))
                .as("the pending part is buffered in a temp file")
                .isOne();

        stream.closeForCommit();

        assertThat(s3.openMultipartUploads.get(uploadId))
                .as("the commit must upload the pending part")
                .containsOnlyKeys(1);
        assertThat(countLocalFilesIn(tmp)).as("a successful commit deletes the temp file").isZero();
    }

    @Test
    void closeForCommitDeletesAlreadyRemovedTempFile() throws Exception {
        Path dir = tmp.resolve("empty-commit");
        String uid = s3.startMultiPartUpload(KEY);
        // No write(), so there is no pending part and closeForCommit() only deletes the temp file.
        NativeS3RecoverableFsDataOutputStream emptyStream =
                new NativeS3RecoverableFsDataOutputStream(
                        s3, KEY, uid, dir.toString(), MIN_PART_SIZE);
        assertThat(countLocalFilesIn(dir)).as("the stream creates its temp file on open").isOne();

        Files.delete(onlyFileIn(dir).toPath());

        assertThat(emptyStream.closeForCommit()).as("the commit must still succeed").isNotNull();

        assertThat(s3.abortAttempts).as("a healthy commit must not abort the upload").isZero();
        assertThat(countLocalFilesIn(dir)).isZero();
    }

    @Test
    void partUploadFailureLeavesTempFileForClose() throws Exception {
        s3.failUploadPart = true;
        assertThat(countLocalFilesIn(tmp))
                .as("the pending part is buffered in a temp file")
                .isOne();

        // setUp() wrote 5 bytes; 5 more reach MIN_PART_SIZE and flush the part from write().
        assertThatThrownBy(() -> stream.write(bytes('B', 5), 0, 5))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected uploadPart failure");

        assertThat(countLocalFilesIn(tmp))
                .as("a failed part upload keeps the temp file so the exception is not masked")
                .isOne();

        stream.close();

        assertThat(countLocalFilesIn(tmp)).as("close() reclaims the temp file").isZero();
    }

    @Test
    void partUploadDeletesAlreadyRemovedTempFile() throws Exception {
        s3.deletePartFileAfterUpload = true;
        File flushedFile = onlyFileIn(tmp);

        stream.write(bytes('B', 5), 0, 5);

        assertThat(s3.uploadPartAttempts).as("exactly one part was uploaded").isOne();
        assertThat(flushedFile).as("the uploaded part file is gone").doesNotExist();
        assertThat(onlyFileIn(tmp))
                .as("write() rotated to a fresh temp file")
                .isNotEqualTo(flushedFile);

        stream.closeForCommit().commit();

        assertThat(s3.committedObjects.get(KEY))
                .as("the uploaded part is still committed")
                .hasSize((int) MIN_PART_SIZE);
        assertThat(countLocalFilesIn(tmp)).as("the commit deletes the rotated temp file").isZero();
    }

    @Test
    void closeDeletesTempFileRemovedDuringCleanup() throws Exception {
        Path dir = tmp.resolve("close-race");
        String uid = s3.startMultiPartUpload(KEY);
        // close() may run concurrently with the writer thread during cancellation, so the temp
        // file may already be gone when close() deletes it.
        NativeS3RecoverableFsDataOutputStream racingStream =
                new NativeS3RecoverableFsDataOutputStream(
                        s3, KEY, uid, dir.toString(), MIN_PART_SIZE) {
                    @Override
                    protected void deleteTempFile(File file) throws IOException {
                        Files.delete(file.toPath());
                        super.deleteTempFile(file);
                    }
                };
        racingStream.write(bytes('A', 5), 0, 5);
        assertThat(countLocalFilesIn(dir))
                .as("the pending part is buffered in a temp file")
                .isOne();

        racingStream.close();

        assertThat(s3.abortAttempts).isEqualTo(1);
        assertThat(countLocalFilesIn(dir)).isZero();
    }

    private NativeS3RecoverableFsDataOutputStream newStream(
            InMemoryNativeS3Operations ops, String uid) throws IOException {
        return new NativeS3RecoverableFsDataOutputStream(
                ops, KEY, uid, tmp.toString(), MIN_PART_SIZE);
    }

    private static File onlyFileIn(Path dir) {
        File[] files = dir.toFile().listFiles();
        assertThat(files).hasSize(1);
        return files[0];
    }

    private NativeS3RecoverableFsDataOutputStream newFailingDeleteStream() throws IOException {
        String uid = s3.startMultiPartUpload(KEY);
        return new NativeS3RecoverableFsDataOutputStream(
                s3, KEY, uid, tmp.toString(), MIN_PART_SIZE) {
            @Override
            protected void deleteTempFile(File file) throws IOException {
                throw new IOException("injected temp-file delete failure");
            }
        };
    }

    private static long countLocalFilesIn(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        try (java.util.stream.Stream<Path> s = Files.list(dir)) {
            return s.count();
        }
    }

    private static byte[] bytes(char c, int n) {
        byte[] b = new byte[n];
        Arrays.fill(b, (byte) c);
        return b;
    }
}
