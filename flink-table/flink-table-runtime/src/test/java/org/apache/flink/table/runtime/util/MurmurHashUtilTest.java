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

package org.apache.flink.table.runtime.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.data.binary.BinaryRowDataUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MurmurHashUtil}. */
class MurmurHashUtilTest {

    @Test
    void testHashBytesMatchesHashUnsafeBytesForAllTailLengths() {
        // Lengths 0..17 cover every tail length (0..3 trailing bytes) with 0..4 full words.
        for (int length = 0; length <= 17; length++) {
            byte[] bytes = bytes(length);
            assertThat(MurmurHashUtil.hashBytes(MemorySegmentFactory.wrap(bytes), 0, length))
                    .as("length %d", length)
                    .isEqualTo(
                            MurmurHashUtil.hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, length));
        }
    }

    @Test
    void testHashBytesByWordsMatchesHashUnsafeBytesByWords() {
        for (int length = 0; length <= 16; length += 4) {
            byte[] bytes = bytes(length);
            assertThat(MurmurHashUtil.hashBytesByWords(MemorySegmentFactory.wrap(bytes), 0, length))
                    .as("length %d", length)
                    .isEqualTo(
                            MurmurHashUtil.hashUnsafeBytesByWords(
                                    bytes, BYTE_ARRAY_BASE_OFFSET, length))
                    // Without a tail, hashing by words is the same as hashing by bytes.
                    .isEqualTo(
                            MurmurHashUtil.hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, length));
        }
    }

    @Test
    void testHashBytesWithOffsetInsideSegment() {
        byte[] data = bytes(13);
        int offset = 7;
        byte[] padded = new byte[offset + data.length + 5];
        System.arraycopy(data, 0, padded, offset, data.length);
        MemorySegment segment = MemorySegmentFactory.wrap(padded);

        int expected = MurmurHashUtil.hashBytes(MemorySegmentFactory.wrap(data), 0, data.length);
        assertThat(MurmurHashUtil.hashBytes(segment, offset, data.length)).isEqualTo(expected);
        assertThat(
                        MurmurHashUtil.hashUnsafeBytes(
                                padded, BYTE_ARRAY_BASE_OFFSET + offset, data.length))
                .isEqualTo(expected);
        assertThat(MurmurHashUtil.hashBytesByWords(segment, offset, 12))
                .isEqualTo(MurmurHashUtil.hashBytesByWords(MemorySegmentFactory.wrap(data), 0, 12));
    }

    @Test
    void testFmixKnownValues() {
        // Reference outputs of the MurmurHash3 fmix32 / fmix64 finalizers.
        assertThat(MurmurHashUtil.fmix(0)).isZero();
        assertThat(MurmurHashUtil.fmix(1)).isEqualTo(0x514e28b7);
        assertThat(MurmurHashUtil.fmix(0L)).isZero();
        assertThat(MurmurHashUtil.fmix(1L)).isEqualTo(0xb456bcfc34c2cb2cL);
    }

    /**
     * These values are pinned on purpose. The hash codes of binary rows and strings, and the
     * generated hash code of BINARY/VARBINARY fields, are computed with this algorithm and can end
     * up in key-group assignment and hash partitioning. Changing any of these values breaks
     * compatibility with existing state, so update them only for an intentional, documented change.
     * The values assume a little-endian platform.
     */
    @Test
    void testPinnedHashValues() {
        assertThat(hashBytes(new byte[0])).isEqualTo(142593372);
        assertThat(hashBytes(utf8("a"))).isEqualTo(1485273170);
        assertThat(hashBytes(utf8("abcd"))).isEqualTo(-396302900);
        assertThat(hashBytes(utf8("Hello, Flink!"))).isEqualTo(50873294);
        // Tail bytes are sign-extended before mixing.
        assertThat(hashBytes(new byte[] {(byte) 0x80, (byte) 0xff, 0x7f})).isEqualTo(1911184379);
        assertThat(
                        MurmurHashUtil.hashBytesByWords(
                                MemorySegmentFactory.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}),
                                0,
                                8))
                .isEqualTo(1184650953);
    }

    @Test
    void testMatchesBinaryStringDataHashCode() {
        // BinaryStringData hashes with the copy of this algorithm in flink-table-common.
        for (String s : new String[] {"", "a", "abcd", "Hello, Flink!"}) {
            assertThat(hashBytes(utf8(s)))
                    .as("'%s'", s)
                    .isEqualTo(BinaryStringData.fromString(s).hashCode());
        }
    }

    private static int hashBytes(byte[] bytes) {
        return MurmurHashUtil.hashBytes(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 37 - 100);
        }
        return bytes;
    }
}
