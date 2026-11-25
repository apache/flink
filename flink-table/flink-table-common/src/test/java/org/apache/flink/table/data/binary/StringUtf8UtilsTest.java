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
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link StringUtf8Utils} and the connector-facing {@link BinaryStringData#fromUtf8Bytes}
 * factory.
 */
class StringUtf8UtilsTest {

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

    @Test
    void testToValidUtf8Bytes() {
        final byte[] valid = "Grüße, 世界 🚀".getBytes(StandardCharsets.UTF_8);

        // Valid bytes are returned as they are.
        assertThat(StringUtf8Utils.toValidUtf8Bytes(StringData.fromBytes(valid))).isSameAs(valid);

        // Every malformed sequence becomes U+FFFD, as when the string is decoded.
        assertThat(StringUtf8Utils.toValidUtf8Bytes(StringData.fromBytes(bytes('a', 0xFF, 'b'))))
                .isEqualTo("a\uFFFDb".getBytes(StandardCharsets.UTF_8));
        assertThat(StringUtf8Utils.toValidUtf8Bytes(StringData.fromBytes(bytes('a', 0xC3))))
                .isEqualTo("a\uFFFD".getBytes(StandardCharsets.UTF_8));

        // A Java string is encoded without building its binary form. An unpaired surrogate has no
        // UTF-8 form, so encoding stores '?' in its place.
        final BinaryStringData javaString = BinaryStringData.fromString("a\uD800b");
        assertThat(StringUtf8Utils.toValidUtf8Bytes(javaString)).isEqualTo(bytes('a', '?', 'b'));
        assertThat(javaString.getBinarySection()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"Hello, world", "Café au lait", "é", "a😀b"})
    void testDecodeUtf8RoundTrip(String input) {
        final byte[] b = input.getBytes(StandardCharsets.UTF_8);
        assertThat(StringUtf8Utils.decodeUTF8(b, 0, b.length)).isEqualTo(input);
        final byte[] padded = new byte[b.length + 1];
        System.arraycopy(b, 0, padded, 1, b.length);
        assertThat(StringUtf8Utils.decodeUTF8(padded, 1, b.length)).isEqualTo(input);
    }

    private static byte[] bytes(int... values) {
        final byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
