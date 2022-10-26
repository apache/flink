/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SkipListKeyComparator}. */
class SkipListKeyComparatorTest {
    private static final SkipListKeySerializer<Long, Integer> skipListKeySerializerForPrimitive =
            new SkipListKeySerializer<>(LongSerializer.INSTANCE, IntSerializer.INSTANCE);
    private static final SkipListKeySerializer<byte[], byte[]> skipListKeySerializerForByteArray =
            new SkipListKeySerializer<>(ByteArraySerializer.INSTANCE, ByteArraySerializer.INSTANCE);
    private static final SkipListKeySerializer<byte[], byte[]>
            skipListKeySerializerForNamespaceCompare =
                    new SkipListKeySerializer<>(
                            ByteArraySerializer.INSTANCE, ByteArraySerializer.INSTANCE);

    @Test
    void testPrimitiveEqualKeyAndEqualNamespace() {
        // verify equal namespace and key
        assertThat(compareSkipListKeyOfPrimitive(0L, 0, 0L, 0)).isEqualTo(0);
    }

    @Test
    void testPrimitiveDiffKeyAndEqualNamespace() {
        // verify equal namespace and unequal key
        assertThat(compareSkipListKeyOfPrimitive(0L, 5, 1L, 5)).isLessThan(0);
        assertThat(compareSkipListKeyOfPrimitive(192L, 90, 87L, 90)).isGreaterThan(0);
    }

    @Test
    void testPrimitiveEqualKeyAndDiffNamespace() {
        // verify unequal namespace and equal key
        assertThat(compareSkipListKeyOfPrimitive(8374L, 2, 8374L, 3)).isLessThan(0);
        assertThat(compareSkipListKeyOfPrimitive(839L, 3, 839L, 2)).isGreaterThan(0);
    }

    @Test
    void testPrimitiveDiffKeyAndDiffNamespace() {
        // verify unequal namespace and unequal key
        assertThat(compareSkipListKeyOfPrimitive(1L, 2, 3L, 4)).isLessThan(0);
        assertThat(compareSkipListKeyOfPrimitive(1L, 4, 3L, 2)).isGreaterThan(0);
        assertThat(compareSkipListKeyOfPrimitive(3L, 2, 1L, 4)).isLessThan(0);
        assertThat(compareSkipListKeyOfPrimitive(3L, 4, 1L, 2)).isGreaterThan(0);
    }

    @Test
    void testByteArrayEqualKeyAndEqualNamespace() {
        // verify equal namespace and key
        assertThat(compareSkipListKeyOfByteArray("34", "25", "34", "25")).isEqualTo(0);
    }

    @Test
    void testByteArrayEqualKeyAndLargerNamespace() {
        // verify larger namespace
        assertThat(compareSkipListKeyOfByteArray("34", "27", "34", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "27", "34", "25,34")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "27,28", "34", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "27,28", "34", "25,34")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "27,28", "34", "27,3")).isGreaterThan(0);
    }

    @Test
    void testByteArrayEqualKeyAndSmallerNamespace() {
        // verify smaller namespace
        assertThat(compareSkipListKeyOfByteArray("34", "25", "34", "27")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "25", "34", "27,34")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "25,28", "34", "27")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "25,28", "34", "27,34")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "25,28", "34", "25,34")).isLessThan(0);
    }

    @Test
    void testByteArrayLargerKeyAndEqualNamespace() {
        // verify larger key
        assertThat(compareSkipListKeyOfByteArray("34", "25", "30", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34", "25", "30,38", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34,22", "25", "30", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34,22", "25", "30,38", "25")).isGreaterThan(0);
        assertThat(compareSkipListKeyOfByteArray("34,82", "25", "34,38", "25")).isGreaterThan(0);
    }

    @Test
    void testByteArraySmallerKeyAndEqualNamespace() {
        // verify smaller key
        assertThat(compareSkipListKeyOfByteArray("30", "25", "34", "25")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("30,38", "25", "34", "25")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("30", "25", "34,22", "25")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("30,38", "25", "34,22", "25")).isLessThan(0);
        assertThat(compareSkipListKeyOfByteArray("30,38", "25", "30,72", "25")).isLessThan(0);
    }

    @Test
    void testEqualNamespace() {
        // test equal namespace
        assertThat(compareNamespace("23", "23")).isEqualTo(0);
    }

    @Test
    void testSmallerNamespace() {
        // test smaller namespace
        assertThat(compareNamespace("23", "24")).isLessThan(0);
        assertThat(compareNamespace("23", "24,35")).isLessThan(0);
        assertThat(compareNamespace("23,25", "24")).isLessThan(0);
        assertThat(compareNamespace("23,20", "24,45")).isLessThan(0);
        assertThat(compareNamespace("23,20", "23,45")).isLessThan(0);
    }

    @Test
    void testLargerNamespace() {
        // test larger namespace
        assertThat(compareNamespace("26", "14")).isGreaterThan(0);
        assertThat(compareNamespace("26", "14,73")).isGreaterThan(0);
        assertThat(compareNamespace("26,25", "14")).isGreaterThan(0);
        assertThat(compareNamespace("26,20", "14,45")).isGreaterThan(0);
        assertThat(compareNamespace("26,90", "26,45")).isGreaterThan(0);
    }

    private int compareSkipListKeyOfByteArray(
            String key1, String namespace1, String key2, String namespace2) {
        return compareSkipListKey(
                skipListKeySerializerForByteArray,
                convertStringToByteArray(key1),
                convertStringToByteArray(namespace1),
                convertStringToByteArray(key2),
                convertStringToByteArray(namespace2));
    }

    private int compareSkipListKeyOfPrimitive(
            long key1, int namespace1, long key2, int namespace2) {
        return compareSkipListKey(
                skipListKeySerializerForPrimitive, key1, namespace1, key2, namespace2);
    }

    private <K, N> int compareSkipListKey(
            @Nonnull SkipListKeySerializer<K, N> keySerializer,
            K key1,
            N namespace1,
            K key2,
            N namespace2) {
        MemorySegment b1 = MemorySegmentFactory.wrap(keySerializer.serialize(key1, namespace1));
        MemorySegment b2 = MemorySegmentFactory.wrap(keySerializer.serialize(key2, namespace2));
        return SkipListKeyComparator.compareTo(b1, 0, b2, 0);
    }

    private int compareNamespace(String namespace, String targetNamespace) {
        final byte[] key = convertStringToByteArray("34");
        byte[] n =
                skipListKeySerializerForNamespaceCompare.serializeNamespace(
                        convertStringToByteArray(namespace));
        byte[] k =
                skipListKeySerializerForNamespaceCompare.serialize(
                        key, convertStringToByteArray(targetNamespace));
        return SkipListKeyComparator.compareNamespaceAndNode(
                MemorySegmentFactory.wrap(n), 0, n.length, MemorySegmentFactory.wrap(k), 0);
    }

    private byte[] convertStringToByteArray(@Nonnull String str) {
        String[] subStr = str.split(",");
        byte[] value = new byte[subStr.length];
        for (int i = 0; i < subStr.length; i++) {
            int v = Integer.valueOf(subStr[i]);
            value[i] = (byte) v;
        }
        return value;
    }

    /** A serializer for byte array which does not support deserialization. */
    private static class ByteArraySerializer extends TypeSerializerSingleton<byte[]> {

        private static final byte[] EMPTY = new byte[0];

        static final ByteArraySerializer INSTANCE = new ByteArraySerializer();

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public byte[] createInstance() {
            return EMPTY;
        }

        @Override
        public byte[] copy(byte[] from) {
            byte[] copy = new byte[from.length];
            System.arraycopy(from, 0, copy, 0, from.length);
            return copy;
        }

        @Override
        public byte[] copy(byte[] from, byte[] reuse) {
            return copy(from);
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(byte[] record, DataOutputView target) throws IOException {
            if (record == null) {
                throw new IllegalArgumentException("The record must not be null.");
            }

            // do not write length of array, so deserialize is not supported
            target.write(record);
        }

        @Override
        public byte[] deserialize(DataInputView source) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] deserialize(byte[] reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializerSnapshot<byte[]> snapshotConfiguration() {
            throw new UnsupportedOperationException();
        }
    }
}
