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

package org.apache.flink.table.runtime.sequencedmultisetstate.linked;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordUtils;
import org.apache.flink.table.types.logical.IntType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.flink.table.runtime.sequencedmultisetstate.linked.RowDataKeySerializerTest.EQUALISER;
import static org.apache.flink.table.runtime.sequencedmultisetstate.linked.RowDataKeySerializerTest.HASH_FUNCTION;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataKeySerializerSnapshot}, specifically partial-read scenarios. */
class RowDataKeySerializerSnapshotTest {

    /**
     * Verifies that the snapshot can be restored when the underlying stream returns fewer bytes
     * than requested from {@code read(byte[], int, int)} (e.g., when data spans multiple memory
     * segments). This is a regression test for a bug where {@code DataInputView.read()} was used
     * instead of {@code readFully()}.
     */
    @Test
    void testSnapshotRestoreWithPartialReads() throws Exception {
        RowDataKeySerializer serializer = createSerializer();
        RowDataKeySerializerSnapshot snapshot = new RowDataKeySerializerSnapshot(serializer);

        // Write snapshot to bytes.
        byte[] snapshotBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
            snapshot.writeSnapshot(out);
            snapshotBytes = baos.toByteArray();
        }

        // Read snapshot back through a stream that returns at most 1 byte per read() call.
        RowDataKeySerializerSnapshot restored = new RowDataKeySerializerSnapshot();
        try (InputStream bais =
                new OneByteAtATimeInputStream(new ByteArrayInputStream(snapshotBytes))) {
            DataInputView in = new DataInputViewStreamWrapper(bais);
            restored.readSnapshot(0, in, Thread.currentThread().getContextClassLoader());
        }

        // Verify the restored serializer works.
        RowDataKeySerializer restoredSerializer =
                (RowDataKeySerializer) restored.restoreSerializer();
        RowDataKey original =
                RowDataKey.toKeyNotProjected(
                        StreamRecordUtils.row(42),
                        serializer.equalizerInstance,
                        serializer.hashFunctionInstance);
        assertThat(restoredSerializer.copy(original)).isEqualTo(original);
    }

    private static RowDataKeySerializer createSerializer() {
        return new RowDataKeySerializer(
                new RowDataSerializer(new IntType()),
                EQUALISER.newInstance(RowDataKeySerializerSnapshotTest.class.getClassLoader()),
                HASH_FUNCTION.newInstance(RowDataKeySerializerSnapshotTest.class.getClassLoader()),
                EQUALISER,
                HASH_FUNCTION);
    }

    /**
     * An {@link InputStream} wrapper that returns at most 1 byte per {@code read(byte[], int, int)}
     * call, simulating the behavior of streams that don't fulfill the full read request (e.g.,
     * multi-segment memory views or network streams).
     */
    private static class OneByteAtATimeInputStream extends FilterInputStream {

        OneByteAtATimeInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return super.read(b, off, Math.min(1, len));
        }
    }
}
