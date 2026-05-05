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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the UTF-8 validator and the connector-facing {@link BinaryStringData#fromUtf8Bytes}
 * factory that delegates to it.
 */
class StringUtf8UtilsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("utf8Inputs")
    void testFirstInvalidUtf8ByteIndex(
            String name, byte[] bytes, int offset, int numBytes, int expected) {
        assertThat(StringUtf8Utils.firstInvalidUtf8ByteIndex(bytes, offset, numBytes))
                .isEqualTo(expected);
    }

    static Stream<Arguments> utf8Inputs() {
        return Stream.of(
                // Valid - one boundary per width hits each DFA-equivalent transition.
                Arguments.of("U+0080 smallest 2-byte", bytes(0xC2, 0x80), 0, 2, -1),
                Arguments.of("U+0800 smallest 3-byte", bytes(0xE0, 0xA0, 0x80), 0, 3, -1),
                Arguments.of("U+10FFFF largest valid", bytes(0xF4, 0x8F, 0xBF, 0xBF), 0, 4, -1),
                // Invalid - one per malformed class.
                Arguments.of("stray continuation", bytes(0x80), 0, 1, 0),
                Arguments.of("ASCII run then bad byte", bytes('A', 'B', 'C', 0x80), 0, 4, 3),
                Arguments.of("U+110000 above max", bytes(0xF4, 0x90, 0x80, 0x80), 0, 4, 0),
                // F5 is a forbidden lead per RFC 3629; padded with continuations so the
                // shortest-form check fires (a bare F5 reports as truncation at index 1).
                Arguments.of("F5 forbidden lead (padded)", bytes(0xF5, 0x80, 0x80, 0x80), 0, 4, 0),
                Arguments.of("U+D800 surrogate", bytes(0xED, 0xA0, 0x80), 0, 3, 0),
                Arguments.of("overlong '/'", bytes(0xC0, 0xAF), 0, 2, 0),
                // Truncated -> index points one past the input.
                Arguments.of("truncated 3-byte", bytes(0xE2), 0, 1, 1),
                // Offset/length variant validates only the inner range.
                Arguments.of("inner range valid", bytes(0x80, 'O', 'K', 0x80), 1, 2, -1),
                Arguments.of("outer bytes invalid", bytes(0x80, 'O', 'K', 0x80), 0, 4, 0));
    }

    @Test
    void testFromUtf8Bytes() {
        final byte[] hello = "Hello".getBytes(StandardCharsets.UTF_8);

        // Valid input wraps the bytes.
        assertThat(BinaryStringData.fromUtf8Bytes(hello))
                .isEqualTo(BinaryStringData.fromBytes(hello));

        // null in -> null out, mirroring fromString.
        assertThat(BinaryStringData.fromUtf8Bytes((byte[]) null)).isNull();
        assertThat(StringData.fromUtf8Bytes((byte[]) null)).isNull();

        // Invalid UTF-8 throws TableRuntimeException with the relative byte index.
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(bytes('A', 'B', 0x80)))
                .isInstanceOf(TableRuntimeException.class)
                .hasMessageContaining("Invalid UTF-8 byte at index 2 of 3");
    }

    private static byte[] bytes(int... values) {
        final byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
