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
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;
import org.apache.flink.fs.s3native.SeaweedFsTestContainer;

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
 */
class NativeS3RecoverableWriterRecoveryITCase {

    private static final int PART = 5 * 1024 * 1024;
    private static final long MIN_PART_SIZE = PART;

    @RegisterExtension
    private static final AllCallbackWrapper<TestContainerExtension<SeaweedFsTestContainer>>
            SEAWEEDFS_EXTENSION =
                    new AllCallbackWrapper<>(
                            new TestContainerExtension<>(SeaweedFsTestContainer::new));

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

    private static SeaweedFsTestContainer getContainer() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    private NativeS3RecoverableWriter writer() {
        return NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
    }

    private Path targetPath() {
        return new Path("s3://" + bucket + "/" + key);
    }

    private String incompletePrefix() {
        int lastSlash = key.lastIndexOf('/');
        String parent = lastSlash < 0 ? "" : key.substring(0, lastSlash + 1);
        String name = lastSlash < 0 ? key : key.substring(lastSlash + 1);
        return parent + "_" + name + ".incomplete.";
    }

    @Test
    void recoverWithoutIncompleteTailStillWorks() throws Exception {
        NativeS3RecoverableWriter writer1 = writer();

        // Write exactly one full part => currentPartSize=0, no side object on persist.
        RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        RecoverableWriter.ResumeRecoverable r = out.persist();
        assertThat(((NativeS3Recoverable) r).incompleteObjectName())
                .as("no tail => no side object")
                .isNull();
        assertThat(s3.listKeys(incompletePrefix())).isEmpty();

        NativeS3RecoverableWriter writer2 = writer();
        RecoverableFsDataOutputStream resumed = writer2.recover(r);
        resumed.write(bytes('C', 10), 0, 10);
        resumed.closeForCommit().commit();

        assertContentEquals(s3.readObject(key), concat(bytes('A', PART), bytes('C', 10)));
    }

    @Test
    void recoverFailsCleanlyWhenSideObjectMissing() throws Exception {
        NativeS3RecoverableWriter writer1 = writer();
        RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        out.write(bytes('E', 5), 0, 5);
        NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        String sideObjectKey = r.incompleteObjectName();
        assertThat(sideObjectKey).isNotNull();

        s3.removeObject(sideObjectKey);
        long localFilesBefore = countLocalFilesIn(tmp);
        NativeS3RecoverableWriter writer2 = writer();

        assertThatThrownBy(() -> writer2.recover(r))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to get object");

        assertThat(countLocalFilesIn(tmp))
                .as("partial download must be cleaned up on failure")
                .isEqualTo(localFilesBefore);
    }

    @Test
    void recoverFailsCleanlyOnLengthMismatch() throws Exception {
        NativeS3RecoverableWriter writer1 = writer();
        RecoverableFsDataOutputStream out = writer1.open(targetPath());
        out.write(bytes('A', PART), 0, PART);
        out.write(bytes('E', 5), 0, 5);
        NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        String sideObjectKey = r.incompleteObjectName();

        // Corrupt the side object so its actual length disagrees with the metadata.
        s3.writeObject(sideObjectKey, bytes('X', 99));

        long localFilesBefore = countLocalFilesIn(tmp);
        NativeS3RecoverableWriter writer2 = writer();

        assertThatThrownBy(() -> writer2.recover(r))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected length");

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
        int total = 0;
        for (byte[] c : chunks) {
            total += c.length;
        }
        byte[] out = new byte[total];
        int off = 0;
        for (byte[] c : chunks) {
            System.arraycopy(c, 0, out, off, c.length);
            off += c.length;
        }
        return out;
    }
}
