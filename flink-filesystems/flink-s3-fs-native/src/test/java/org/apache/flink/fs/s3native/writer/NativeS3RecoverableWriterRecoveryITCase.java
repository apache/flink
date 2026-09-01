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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3native.NativeS3FileSystemFactory;
import org.apache.flink.fs.s3native.SeaweedFsNativeS3TestContainer;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link NativeS3RecoverableWriter#recover} running against SeaweedFS.
 *
 * <p>SeaweedFS enforces the S3 5 MiB minimum part size on multipart-complete, so every scenario
 * writes one full {@value #PART}-byte first part (the only non-final part) followed by a small tail
 * that becomes the final part.
 *
 * <p>Terminology used below:
 *
 * <pre>
 *   target object (the file the caller is writing, e.g. "out-&lt;uuid&gt;.txt")
 *     +-- part 1: PART bytes, uploaded as a completed multipart upload part
 *     +-- tail: any bytes written after part 1, not yet part of a completed multipart part
 *
 *   side object ("&lt;key&gt;/.incomplete/&lt;uploadId&gt;/&lt;uuid&gt;", see #incompletePrefix())
 *     - written by persist() only when there IS a tail, so that the tail bytes survive a
 *       writer restart
 *     - read back by recover(), which downloads it locally and appends it to the in-progress
 *       multipart upload before returning a resumed output stream
 *     - has no side object at all when persist() is called exactly on a part boundary
 *       (see recoverWithoutIncompleteTailStillWorks)
 * </pre>
 */
class NativeS3RecoverableWriterRecoveryITCase {

    private static final int PART = (int) NativeS3FileSystemFactory.S3_MULTIPART_MIN_PART_SIZE;
    private static final long MIN_PART_SIZE = PART;

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsNativeS3TestContainer::new));

    @TempDir java.nio.file.Path tmp;

    private String bucket;
    private String key;
    private SeaweedFsNativeS3Operations s3;

    @BeforeEach
    void setUp() {
        bucket = getContainer().getDefaultBucketName();
        key = "out-" + UUID.randomUUID() + ".txt";
        s3 = new SeaweedFsNativeS3Operations(getContainer().getClient(), bucket);
    }

    private static SeaweedFsNativeS3TestContainer getContainer() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    private NativeS3RecoverableWriter writer() {
        return NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
    }

    private Path targetPath() {
        return new Path("s3://" + bucket + "/" + key);
    }

    private String incompletePrefix(String uploadId) {
        return ".incomplete/" + uploadId + "/";
    }

    @Test
    void recoverWithoutIncompleteTailStillWorks() throws Exception {
        final NativeS3RecoverableWriter writer1 = writer();

        // Write exactly one full part => currentPartSize=0, no side object on persist.
        final RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        final NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        assertThat(r.incompleteObjectName()).as("no tail => no side object").isNull();
        assertThat(s3.listKeys(incompletePrefix(r.uploadId()))).isEmpty();

        final NativeS3RecoverableWriter writer2 = writer();
        final RecoverableFsDataOutputStream resumed = writer2.recover(r);
        resumed.write(bytes('C', 10), 0, 10);
        resumed.closeForCommit().commit();

        assertContentEquals(s3.readObject(key), concat(bytes('A', PART), bytes('C', 10)));
    }

    @Test
    void recoverWithNestedKeyStillWorks() throws Exception {
        // Exercise a target key containing "/" path separators, not just a flat key.
        key = "nested/path-" + UUID.randomUUID() + "/out.txt";
        final NativeS3RecoverableWriter writer1 = writer();

        final RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        out.write(bytes('E', 5), 0, 5);
        final NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        assertThat(r.incompleteObjectName()).as("tail written => side object expected").isNotNull();
        assertThat(s3.listKeys(incompletePrefix(r.uploadId())))
                .containsExactly(r.incompleteObjectName());

        final NativeS3RecoverableWriter writer2 = writer();
        final RecoverableFsDataOutputStream resumed = writer2.recover(r);
        resumed.write(bytes('C', 10), 0, 10);
        resumed.closeForCommit().commit();

        assertContentEquals(
                s3.readObject(key), concat(bytes('A', PART), bytes('E', 5), bytes('C', 10)));
    }

    @Test
    void recoverFailsCleanlyWhenSideObjectMissing() throws Exception {
        final NativeS3Recoverable r = persistWithTail();
        final String sideObjectKey = r.incompleteObjectName();
        assertThat(sideObjectKey).isNotNull();

        s3.removeObject(sideObjectKey);

        assertRecoverFailsCleanly(r, "Failed to get object");
    }

    @Test
    void recoverFailsCleanlyOnLengthMismatch() throws Exception {
        final NativeS3Recoverable r = persistWithTail();
        final String sideObjectKey = r.incompleteObjectName();

        // Simulate the side object having been overwritten/corrupted out-of-band between
        // persist() and recover() (e.g. a retried writer racing on the same side-object key, or
        // an eventual-consistency edge case on a non-AWS S3 implementation): the side object's
        // actual length no longer agrees with the length recorded in the recoverable's metadata.
        s3.writeObject(sideObjectKey, bytes('X', 99));

        assertRecoverFailsCleanly(r, "unexpected length");
    }

    /** Writes one full part plus a small tail, forcing a side object to be created on persist. */
    private NativeS3Recoverable persistWithTail() throws IOException {
        final NativeS3RecoverableWriter writer1 = writer();
        final RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        out.write(bytes('E', 5), 0, 5);
        return (NativeS3Recoverable) out.persist();
    }

    /**
     * Asserts that recovering {@code r} fails with an {@link IOException} containing {@code
     * expectedMessageFragment}, and that no partially-downloaded local file is left behind.
     */
    private void assertRecoverFailsCleanly(NativeS3Recoverable r, String expectedMessageFragment)
            throws IOException {
        final long localFilesBefore = countLocalFilesIn(tmp);
        final NativeS3RecoverableWriter writer2 = writer();

        assertThatThrownBy(() -> writer2.recover(r))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(expectedMessageFragment);

        assertThat(countLocalFilesIn(tmp))
                .as("partial download must be cleaned up on failure")
                .isEqualTo(localFilesBefore);
    }

    private static void assertContentEquals(byte[] actual, byte[] expected) {
        assertThat(actual).hasSameSizeAs(expected);
        assertThat(Arrays.equals(actual, expected))
                .as("committed object content must match every persisted byte")
                .isTrue();
    }

    private static long countLocalFilesIn(java.nio.file.Path dir) throws IOException {
        if (!java.nio.file.Files.isDirectory(dir)) {
            return 0;
        }
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(dir)) {
            return s.count();
        }
    }

    private static byte[] bytes(char c, int n) {
        byte[] b = new byte[n];
        Arrays.fill(b, (byte) c);
        return b;
    }

    private static byte[] concat(byte[]... chunks) {
        byte[] out = ArrayUtils.EMPTY_BYTE_ARRAY;
        for (byte[] c : chunks) {
            out = ArrayUtils.addAll(out, c);
        }
        return out;
    }
}
