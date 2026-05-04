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

import org.apache.flink.table.data.StringData;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BinaryStringData#fromUtf8Bytes(byte[])} and {@link StringData#fromUtf8Bytes}.
 */
class BinaryStringDataFromUtf8BytesTest {

    @Test
    void testFromUtf8Bytes() {
        // Valid input: ASCII, multi-byte, empty, and the offset/length variant all yield the
        // same instance you would get from the unchecked fromBytes / fromString factory.
        final byte[] hello = "Hello, world!".getBytes(StandardCharsets.UTF_8);
        assertThat(BinaryStringData.fromUtf8Bytes(hello))
                .isEqualTo(BinaryStringData.fromBytes(hello));

        final byte[] multibyte = "é€😀".getBytes(StandardCharsets.UTF_8);
        assertThat(BinaryStringData.fromUtf8Bytes(multibyte))
                .isEqualTo(BinaryStringData.fromBytes(multibyte));

        assertThat(BinaryStringData.fromUtf8Bytes(new byte[0]))
                .isEqualTo(BinaryStringData.EMPTY_UTF8);

        // Offset/length variant validates only the inner range.
        final byte[] padded = {(byte) 0x80, 'O', 'K', (byte) 0x80};
        assertThat(BinaryStringData.fromUtf8Bytes(padded, 1, 2))
                .isEqualTo(BinaryStringData.fromString("OK"));

        // null in -> null out so connector authors can forward upstream nullability without
        // an extra guard at every call site.
        assertThat(BinaryStringData.fromUtf8Bytes((byte[]) null)).isNull();
        assertThat(BinaryStringData.fromUtf8Bytes(null, 0, 0)).isNull();

        // StringData facade delegates with the same null behavior.
        assertThat(StringData.fromUtf8Bytes(hello))
                .isEqualTo(StringData.fromString("Hello, world!"));
        assertThat(StringData.fromUtf8Bytes((byte[]) null)).isNull();
        assertThat(StringData.fromUtf8Bytes(null, 0, 0)).isNull();
    }

    @Test
    void testFromUtf8BytesRejectsInvalid() {
        // Each invalid class throws IllegalArgumentException.
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(new byte[] {(byte) 0x80}))
                .isInstanceOf(IllegalArgumentException.class);
        // Truncated: 0xE2 0x82 expects three bytes, only two are present.
        assertThatThrownBy(
                        () -> BinaryStringData.fromUtf8Bytes(new byte[] {(byte) 0xE2, (byte) 0x82}))
                .isInstanceOf(IllegalArgumentException.class);
        // Overlong 2-byte form of '/' (0xC0 0xAF).
        assertThatThrownBy(
                        () -> BinaryStringData.fromUtf8Bytes(new byte[] {(byte) 0xC0, (byte) 0xAF}))
                .isInstanceOf(IllegalArgumentException.class);
        // Surrogate code point U+D800 (0xED 0xA0 0x80).
        assertThatThrownBy(
                        () ->
                                BinaryStringData.fromUtf8Bytes(
                                        new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}))
                .isInstanceOf(IllegalArgumentException.class);

        // Offset/length variant rejects the full range when the outer bytes are invalid.
        final byte[] padded = {(byte) 0x80, 'O', 'K', (byte) 0x80};
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(padded, 0, 4))
                .isInstanceOf(IllegalArgumentException.class);

        // StringData facade rejects identically.
        assertThatThrownBy(() -> StringData.fromUtf8Bytes(new byte[] {(byte) 0x80}))
                .isInstanceOf(IllegalArgumentException.class);

        // Error message contract: includes the relative byte index (within numBytes), not the
        // absolute array index. This makes log lines actionable for connectors handling slices.
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(new byte[] {(byte) 0x80}))
                .hasMessageContaining("Invalid UTF-8 byte at index 0");
        final byte[] failsAtIndexTwo = {'A', 'B', (byte) 0x80};
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(failsAtIndexTwo))
                .hasMessageContaining("Invalid UTF-8 byte at index 2 of 3");
        assertThatThrownBy(() -> BinaryStringData.fromUtf8Bytes(failsAtIndexTwo, 1, 2))
                .hasMessageContaining("Invalid UTF-8 byte at index 1 of 2");
    }
}
