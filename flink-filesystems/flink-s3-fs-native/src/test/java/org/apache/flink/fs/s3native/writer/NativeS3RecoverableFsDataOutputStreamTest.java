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

    private NativeS3RecoverableFsDataOutputStream newStream(
            InMemoryNativeS3Operations ops, String uid) throws IOException {
        return new NativeS3RecoverableFsDataOutputStream(
                ops, KEY, uid, tmp.toString(), MIN_PART_SIZE);
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
