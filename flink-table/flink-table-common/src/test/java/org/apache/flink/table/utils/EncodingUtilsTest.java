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

package org.apache.flink.table.utils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.flink.table.utils.EncodingUtils}. */
class EncodingUtilsTest {

    @Test
    void testObjectStringEncoding() {
        final MyPojo pojo = new MyPojo(33, "Hello");
        final String base64 = EncodingUtils.encodeObjectToString(pojo);
        assertThat(EncodingUtils.decodeStringToObject(base64, Serializable.class)).isEqualTo(pojo);
    }

    @Test
    void testStringBase64Encoding() {
        final String string = "Hello, this is apache flink.";
        final String base64 = EncodingUtils.encodeStringToBase64(string);
        assertThat(base64).isEqualTo("SGVsbG8sIHRoaXMgaXMgYXBhY2hlIGZsaW5rLg==");
        assertThat(EncodingUtils.decodeBase64ToString(base64)).isEqualTo(string);
    }

    @Test
    void testMd5Hex() {
        final String string = "Hello, world! How are you? 高精确";
        assertThat(EncodingUtils.hex(EncodingUtils.md5(string)))
                .isEqualTo("983abac84e994b4ba73be177e5cc298b");
    }

    @Test
    void testJavaEscaping() {
        assertThat(EncodingUtils.escapeJava("\\hello\"world'space/"))
                .isEqualTo("\\\\hello\\\"world'space/");
    }

    @Test
    void testRepetition() {
        assertThat(EncodingUtils.repeat("we", 3)).isEqualTo("wewewe");
    }

    @Test
    void testIsValidUtf8() {
        // ASCII fast-path
        assertThat(EncodingUtils.isValidUtf8(new byte[0])).isTrue();
        assertThat(EncodingUtils.isValidUtf8(new byte[] {0x00})).isTrue();
        assertThat(EncodingUtils.isValidUtf8("Hello, world!".getBytes(StandardCharsets.UTF_8)))
                .isTrue();

        // Multi-byte sequences: 2-byte (é), 3-byte (€), 4-byte (😀)
        assertThat(EncodingUtils.isValidUtf8("é€😀".getBytes(StandardCharsets.UTF_8))).isTrue();

        // Code-point boundaries at each width (smallest + largest)
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xC2, (byte) 0x80}))
                .isTrue(); // U+0080
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xDF, (byte) 0xBF}))
                .isTrue(); // U+07FF
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xE0, (byte) 0xA0, (byte) 0x80}))
                .isTrue(); // U+0800
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBF}))
                .isTrue(); // U+FFFF
        assertThat(
                        EncodingUtils.isValidUtf8(
                                new byte[] {(byte) 0xF0, (byte) 0x90, (byte) 0x80, (byte) 0x80}))
                .isTrue(); // U+10000
        assertThat(
                        EncodingUtils.isValidUtf8(
                                new byte[] {(byte) 0xF4, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}))
                .isTrue(); // U+10FFFF (largest valid)

        // Above U+10FFFF and forbidden lead bytes (RFC 3629)
        assertThat(
                        EncodingUtils.isValidUtf8(
                                new byte[] {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}))
                .isFalse(); // U+110000

        // Stray continuation byte without a lead
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0x80})).isFalse();

        // Truncated multi-byte sequences
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xE2, (byte) 0x82})).isFalse();
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xF0})).isFalse();

        // Overlong forms of '/' at every width
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xC0, (byte) 0xAF})).isFalse();
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xE0, (byte) 0x80, (byte) 0xAF}))
                .isFalse();
        assertThat(
                        EncodingUtils.isValidUtf8(
                                new byte[] {(byte) 0xF0, (byte) 0x80, (byte) 0x80, (byte) 0xAF}))
                .isFalse();

        // UTF-16 surrogates (high + low, smallest + largest)
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}))
                .isFalse(); // U+D800
        assertThat(EncodingUtils.isValidUtf8(new byte[] {(byte) 0xED, (byte) 0xBF, (byte) 0xBF}))
                .isFalse(); // U+DFFF

        // Offset/length variant: only the inner range is validated
        final byte[] padded = {(byte) 0x80, 'O', 'K', (byte) 0x80};
        assertThat(EncodingUtils.isValidUtf8(padded, 1, 2)).isTrue();
        assertThat(EncodingUtils.isValidUtf8(padded, 0, 4)).isFalse();
        assertThat(EncodingUtils.isValidUtf8(padded, padded.length, 0)).isTrue();

        // null input -> false (Guava Strings.isNullOrEmpty convention)
        assertThat(EncodingUtils.isValidUtf8((byte[]) null)).isFalse();
        assertThat(EncodingUtils.isValidUtf8(null, 0, 0)).isFalse();

        // Bad bounds -> IllegalArgumentException
        assertThatThrownBy(() -> EncodingUtils.isValidUtf8(new byte[3], 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EncodingUtils.isValidUtf8(new byte[3], 1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testUnhex() {
        assertThat(EncodingUtils.unhex("".getBytes())).isEqualTo(new byte[0]);
        assertThat(EncodingUtils.unhex("1".getBytes())).isEqualTo(new byte[] {0});
        assertThat(EncodingUtils.unhex("146".getBytes())).isEqualTo(new byte[] {0, 0x46});
        assertThat(EncodingUtils.unhex("z".getBytes())).isEqualTo(null);
        assertThat(EncodingUtils.unhex("1-".getBytes())).isEqualTo(null);
        assertThat(EncodingUtils.unhex("466C696E6B".getBytes()))
                .isEqualTo(new byte[] {0x46, 0x6c, 0x69, 0x6E, 0x6B});
        assertThat(EncodingUtils.unhex("4D7953514C".getBytes()))
                .isEqualTo(new byte[] {0x4D, 0x79, 0x53, 0x51, 0x4C});
        assertThat(EncodingUtils.unhex("\uD83D\uDE00".getBytes())).isEqualTo(null);
        assertThat(EncodingUtils.unhex(EncodingUtils.hex("\uD83D\uDE00").getBytes()))
                .isEqualTo("\uD83D\uDE00".getBytes());
    }

    // --------------------------------------------------------------------------------------------

    private static class MyPojo implements Serializable {

        private int number;
        private String string;

        public MyPojo(int number, String string) {
            this.number = number;
            this.string = string;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MyPojo myPojo = (MyPojo) o;
            return number == myPojo.number && Objects.equals(string, myPojo.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, string);
        }
    }
}
