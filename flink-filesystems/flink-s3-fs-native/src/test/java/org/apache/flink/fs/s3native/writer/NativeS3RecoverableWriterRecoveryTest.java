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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3RecoverableWriter#recover}. */
class NativeS3RecoverableWriterRecoveryTest {

    private static final String BUCKET = InMemoryNativeS3Operations.DEFAULT_BUCKET;
    private static final String KEY = "out.txt";
    private static final long MIN_PART_SIZE = 10L;

    @TempDir java.nio.file.Path tmp;

    @Test
    void persistThenRecoverPreservesTailBytes() throws Exception {
        InMemoryNativeS3Operations s3 = new InMemoryNativeS3Operations();
        NativeS3RecoverableWriter writer1 =
                NativeS3RecoverableWriter.writer(
                        s3, tmp.toString(), MIN_PART_SIZE, /* maxConcurrent */ 1);

        // --- Phase 1: write 15 bytes; minPartSize=10 ⇒ 10B uploaded as part #1, 5B tail in memory.
        RecoverableFsDataOutputStream out = writer1.open(new Path("s3://" + BUCKET + "/" + KEY));
        byte[] firstChunk = bytes('A', 10); // becomes part #1
        byte[] tail = bytes('E', 5); // becomes the persisted side object
        out.write(firstChunk, 0, firstChunk.length);
        out.write(tail, 0, tail.length);

        // --- Phase 2: checkpoint barrier: persist() and round-trip metadata through the
        //              serializer (this is what Flink does when storing checkpoint state).
        RecoverableWriter.ResumeRecoverable r = out.persist();
        byte[] checkpointed =
                NativeS3RecoverableSerializer.INSTANCE.serialize((NativeS3Recoverable) r);

        // Sanity: the 5-byte tail is sitting in S3 as a side object right now.
        assertThat(s3.storedObjects).hasSize(1);
        String sideObjectKey = s3.storedObjects.keySet().iterator().next();
        assertThat(sideObjectKey).startsWith(KEY + "/.incomplete/");
        assertThat(s3.storedObjects.get(sideObjectKey)).containsExactly(tail);

        // --- Phase 3: simulate task crash + restore from checkpoint. We deliberately do NOT
        //              close `out` because in a real crash the JVM dies; close() would also abort
        //              the MPU and invalidate the recoverable.
        NativeS3Recoverable restored =
                NativeS3RecoverableSerializer.INSTANCE.deserialize(
                        NativeS3RecoverableSerializer.INSTANCE.getVersion(), checkpointed);
        assertThat(restored.incompleteObjectName())
                .as("metadata survives the checkpoint round-trip")
                .isEqualTo(sideObjectKey);
        assertThat(restored.incompleteObjectLength()).isEqualTo(tail.length);

        NativeS3RecoverableWriter writer2 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream resumed = writer2.recover(restored);

        // --- Phase 4: continue writing 10 more bytes, then commit.
        byte[] afterResume = bytes('F', 10);
        resumed.write(afterResume, 0, afterResume.length);
        RecoverableFsDataOutputStream.Committer committer = resumed.closeForCommit();
        committer.commit();

        // --- Phase 5: read what landed in S3.
        byte[] finalObject = s3.committedObjects.get(KEY);
        assertThat(finalObject).isNotNull();

        // EXPECTED (correct exactly-once): 25 bytes — "AAAAAAAAAA" + "EEEEE" + "FFFFFFFFFF"
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.write(firstChunk);
        expected.write(tail);
        expected.write(afterResume);

        // ON UN-FIXED CODE this assertion FAILS:
        //   expected length 25 ("AAAAAAAAAAEEEEEFFFFFFFFFF")
        //   actual   length 20 ("AAAAAAAAAAFFFFFFFFFF")
        // The 5 'E' bytes — the tail durably persisted to S3 at checkpoint time — would be gone.
        assertThat(finalObject)
                .as("recover() must replay the persisted tail before continuing")
                .containsExactly(expected.toByteArray());

        // The side object is intentionally NOT deleted by recover() so that re-recovery from the
        // same checkpoint stays correct. Cleanup is owned by cleanupRecoverableState(), which
        // Flink invokes once the checkpoint is retired.
        assertThat(s3.storedObjects)
                .as("side object outlives recover() to support re-recovery from same checkpoint")
                .containsKey(sideObjectKey);
        assertThat(writer2.cleanupRecoverableState(restored)).isTrue();
        assertThat(s3.storedObjects)
                .as("cleanupRecoverableState must delete the side object")
                .doesNotContainKey(sideObjectKey);
    }

    /**
     * Pre-existing happy path: a recoverable with NO incomplete tail (parts only) must still work.
     */
    @Test
    void recoverWithoutIncompleteTailStillWorks() throws Exception {
        InMemoryNativeS3Operations s3 = new InMemoryNativeS3Operations();
        NativeS3RecoverableWriter writer1 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);

        // Write exactly 20 bytes => two full parts, currentPartSize=0, no side object on persist.
        RecoverableFsDataOutputStream out = writer1.open(new Path("s3://" + BUCKET + "/" + KEY));
        out.write(bytes('A', 10), 0, 10);
        out.write(bytes('B', 10), 0, 10);
        RecoverableWriter.ResumeRecoverable r = out.persist();
        assertThat(((NativeS3Recoverable) r).incompleteObjectName())
                .as("no tail => no side object")
                .isNull();
        assertThat(s3.storedObjects).isEmpty();

        // Resume and append another 10 bytes, commit.
        NativeS3RecoverableWriter writer2 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream resumed = writer2.recover(r);
        resumed.write(bytes('C', 10), 0, 10);
        resumed.closeForCommit().commit();

        assertThat(s3.committedObjects.get(KEY))
                .containsExactly(concat(bytes('A', 10), bytes('B', 10), bytes('C', 10)));
    }

    /** If the side object is gone from S3, recover() must fail cleanly and clean up local state. */
    @Test
    void recoverFailsCleanlyWhenSideObjectMissing() throws Exception {
        InMemoryNativeS3Operations s3 = new InMemoryNativeS3Operations();
        NativeS3RecoverableWriter writer1 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream out = writer1.open(new Path("s3://" + BUCKET + "/" + KEY));
        out.write(bytes('A', 10), 0, 10);
        out.write(bytes('E', 5), 0, 5);
        NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        String sideObjectKey = r.incompleteObjectName();
        assertThat(sideObjectKey).isNotNull();

        s3.storedObjects.remove(sideObjectKey);
        long localFilesBefore = countLocalFilesIn(tmp);
        NativeS3RecoverableWriter writer2 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);

        assertThatThrownBy(() -> writer2.recover(r))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not found");

        assertThat(countLocalFilesIn(tmp))
                .as("partial download must be cleaned up on failure")
                .isEqualTo(localFilesBefore);
    }

    /**
     * If the side object is the wrong length, recover() must fail and clean up the partial file.
     */
    @Test
    void recoverFailsCleanlyOnLengthMismatch() throws Exception {
        InMemoryNativeS3Operations s3 = new InMemoryNativeS3Operations();
        NativeS3RecoverableWriter writer1 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream out = writer1.open(new Path("s3://" + BUCKET + "/" + KEY));
        out.write(bytes('A', 10), 0, 10);
        out.write(bytes('E', 5), 0, 5);
        NativeS3Recoverable r = (NativeS3Recoverable) out.persist();
        String sideObjectKey = r.incompleteObjectName();

        // Corrupt the side object so its actual length disagrees with the metadata.
        s3.storedObjects.put(sideObjectKey, bytes('X', 99));

        long localFilesBefore = countLocalFilesIn(tmp);
        NativeS3RecoverableWriter writer2 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);

        assertThatThrownBy(() -> writer2.recover(r))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unexpected length");

        assertThat(countLocalFilesIn(tmp))
                .as("partial download must be cleaned up on failure")
                .isEqualTo(localFilesBefore);
    }

    /**
     * persist → recover → persist → recover chain. Models multiple checkpoint cycles, each one
     * recovering from the previous. Every persisted byte must end up in the committed object.
     */
    @Test
    void recoverThenPersistThenRecoverPreservesAllBytes() throws Exception {
        InMemoryNativeS3Operations s3 = new InMemoryNativeS3Operations();

        // Cycle 1: write 12 bytes (10 -> part, 2 -> tail), persist.
        NativeS3RecoverableWriter w1 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream s1 = w1.open(new Path("s3://" + BUCKET + "/" + KEY));
        s1.write(bytes('A', 10), 0, 10);
        s1.write(bytes('B', 2), 0, 2);
        NativeS3Recoverable r1 = (NativeS3Recoverable) s1.persist();
        String firstSideObject = r1.incompleteObjectName();
        assertThat(firstSideObject).isNotNull();

        // Cycle 2: recover from r1, write 3 bytes => total tail now 2+3=5 (still < minPartSize),
        // persist again => new side object containing the combined 5-byte tail.
        NativeS3RecoverableWriter w2 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream s2 = w2.recover(r1);
        s2.write(bytes('C', 3), 0, 3);
        NativeS3Recoverable r2 = (NativeS3Recoverable) s2.persist();
        String secondSideObject = r2.incompleteObjectName();
        assertThat(secondSideObject).isNotEqualTo(firstSideObject);
        assertThat(s3.storedObjects.get(secondSideObject))
                .as("second side object must contain old tail + new bytes")
                .containsExactly(concat(bytes('B', 2), bytes('C', 3)));

        // Cycle 3: recover from r2, write 7 more bytes => tail becomes 5+7=12 >= minPartSize =>
        // upload.
        // Then commit.
        NativeS3RecoverableWriter w3 =
                NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
        RecoverableFsDataOutputStream s3stream = w3.recover(r2);
        s3stream.write(bytes('D', 7), 0, 7);
        s3stream.closeForCommit().commit();

        // Final object must be A*10 + B*2 + C*3 + D*7 = 22 bytes.
        assertThat(s3.committedObjects.get(KEY))
                .containsExactly(
                        concat(bytes('A', 10), bytes('B', 2), bytes('C', 3), bytes('D', 7)));

        // Both old side objects survive until cleanupRecoverableState is called per checkpoint.
        assertThat(s3.storedObjects).containsKeys(firstSideObject, secondSideObject);
        assertThat(w3.cleanupRecoverableState(r1)).isTrue();
        assertThat(w3.cleanupRecoverableState(r2)).isTrue();
        assertThat(s3.storedObjects).doesNotContainKeys(firstSideObject, secondSideObject);
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
