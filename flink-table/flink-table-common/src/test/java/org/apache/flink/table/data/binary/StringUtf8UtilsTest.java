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

package org.apache.flink.table.data.binary;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StringUtf8Utils#firstInvalidUtf8ByteIndex(byte[], int, int)}. */
class StringUtf8UtilsTest {

    @Test
    void testFirstInvalidUtf8ByteIndex() {
        // Valid input -> -1, including ASCII fast-path and multi-byte sequences.
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                "Hello".getBytes(StandardCharsets.UTF_8), 0, 5))
                .isEqualTo(-1);
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                "é€😀".getBytes(StandardCharsets.UTF_8),
                                0,
                                "é€😀".getBytes(StandardCharsets.UTF_8).length))
                .isEqualTo(-1);

        // Code-point boundaries at each width - hits every DFA-equivalent transition.
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xC2, (byte) 0x80}, 0, 2))
                .isEqualTo(-1); // U+0080
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF},
                                0,
                                4))
                .isEqualTo(-1); // U+10FFFF (largest valid)

        // Failure index lands at the first bad byte after an ASCII run.
        final byte[] mixedBad = {'A', 'B', 'C', (byte) 0x80};
        assertThat(StringUtf8Utils.firstInvalidUtf8ByteIndex(mixedBad, 0, mixedBad.length))
                .isEqualTo(3);

        // Above U+10FFFF, surrogate, overlong, and forbidden-lead - all reported.
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80},
                                0,
                                4))
                .isEqualTo(0); // U+110000
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}, 0, 3))
                .isEqualTo(0); // U+D800 surrogate
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xC0, (byte) 0xAF}, 0, 2))
                .isEqualTo(0); // overlong '/'
        // F5 is a forbidden lead per RFC 3629 because any 4-byte sequence starting with F5+
        // decodes above U+10FFFF; pad with continuation bytes so the shortest-form check fires
        // (a bare F5 would be reported as truncation at index 1 instead).
        assertThat(
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(
                                new byte[] {(byte) 0xF5, (byte) 0x80, (byte) 0x80, (byte) 0x80},
                                0,
                                4))
                .isEqualTo(0);

        // Truncated sequence: index points one past the input (the missing continuation).
        assertThat(StringUtf8Utils.firstInvalidUtf8ByteIndex(new byte[] {(byte) 0xE2}, 0, 1))
                .isEqualTo(1);

        // Offset/length variant: only the inner range is validated.
        final byte[] padded = {(byte) 0x80, 'O', 'K', (byte) 0x80};
        assertThat(StringUtf8Utils.firstInvalidUtf8ByteIndex(padded, 1, 2)).isEqualTo(-1);
        assertThat(StringUtf8Utils.firstInvalidUtf8ByteIndex(padded, 0, 4)).isEqualTo(0);

        // Strict on bad arguments - no int return value can unambiguously mean "null" or
        // "out-of-range", so report loudly.
        assertThatThrownBy(() -> StringUtf8Utils.firstInvalidUtf8ByteIndex(null, 0, 0))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> StringUtf8Utils.firstInvalidUtf8ByteIndex(new byte[3], -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtf8Utils.firstInvalidUtf8ByteIndex(new byte[3], 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StringUtf8Utils.firstInvalidUtf8ByteIndex(new byte[3], 1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
