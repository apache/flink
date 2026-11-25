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

package org.apache.flink.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SwarUtils}. */
class SwarUtilsTest {

    // Longer than two words, with a tail, so every byte is checked by the word and the tail loop.
    private static final int LENGTH = 19;

    @Test
    void testIsAsciiOnAsciiRange() {
        final byte[] bytes = filled((byte) 'x');
        assertThat(SwarUtils.isAscii(bytes, 0, LENGTH)).isTrue();
        assertThat(SwarUtils.isAscii(bytes, 0, 0)).isTrue();
        assertThat(SwarUtils.asciiPrefix(bytes, 0, LENGTH)).isEqualTo(LENGTH);
    }

    @ParameterizedTest
    @MethodSource("indices")
    void testAsciiPrefix(int index) {
        final byte[] bytes = filled((byte) 'x');
        bytes[index] = (byte) 0x80;
        assertThat(SwarUtils.asciiPrefix(bytes, 0, LENGTH)).isEqualTo(index);
        // an unaligned start counts from the start, up to the byte unless it's before the start
        assertThat(SwarUtils.asciiPrefix(bytes, 1, LENGTH - 1))
                .isEqualTo(index == 0 ? LENGTH - 1 : index - 1);
        assertThat(SwarUtils.asciiPrefix(bytes, index + 1, LENGTH - index - 1))
                .isEqualTo(LENGTH - index - 1);
    }

    // bytes next to the ones searched for, and bytes of 0x80 and above
    @ParameterizedTest
    @ValueSource(bytes = {'x', ' ', '!', '#', '[', ']', 0x7F, (byte) 0x80, (byte) 0xFF})
    void testContainsAnyOrLessThanWithoutMatch(byte b) {
        assertThat(contains(filled(b), 0, LENGTH)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("escapesAtEveryIndex")
    void testContainsAnyOrLessThan(byte b, int index) {
        final byte[] bytes = filled((byte) 0xC3);
        bytes[index] = b;
        assertThat(contains(bytes, 0, LENGTH)).isTrue();
        // an unaligned start still finds the byte, unless it's before the start
        assertThat(contains(bytes, 1, LENGTH - 1)).isEqualTo(index > 0);
        assertThat(contains(bytes, index + 1, LENGTH - index - 1)).isFalse();
    }

    // bytes from the limit up, and bytes of 0x80 and above
    @ParameterizedTest
    @ValueSource(bytes = {0x20, 'x', 0x7F, (byte) 0x80, (byte) 0xFF})
    void testContainsLessThanWithoutMatch(byte b) {
        assertThat(SwarUtils.containsLessThan(filled(b), 0, LENGTH, 0x20)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("controlCharsAtEveryIndex")
    void testContainsLessThan(byte b, int index) {
        final byte[] bytes = filled((byte) 0xC3);
        bytes[index] = b;
        assertThat(SwarUtils.containsLessThan(bytes, 0, LENGTH, 0x20)).isTrue();
        // an unaligned start still finds the byte, unless it's before the start
        assertThat(SwarUtils.containsLessThan(bytes, 1, LENGTH - 1, 0x20)).isEqualTo(index > 0);
        assertThat(SwarUtils.containsLessThan(bytes, index + 1, LENGTH - index - 1, 0x20))
                .isFalse();
    }

    static IntStream indices() {
        return IntStream.range(0, LENGTH);
    }

    static Stream<Arguments> escapesAtEveryIndex() {
        return atEveryIndex((byte) '"', (byte) '\\', (byte) 0x00, (byte) 0x1F);
    }

    static Stream<Arguments> controlCharsAtEveryIndex() {
        return atEveryIndex((byte) 0x00, (byte) '\n', (byte) 0x1F);
    }

    private static Stream<Arguments> atEveryIndex(Byte... bytes) {
        return Stream.of(bytes).flatMap(b -> indices().mapToObj(i -> Arguments.of(b, i)));
    }

    private static boolean contains(byte[] bytes, int offset, int len) {
        return SwarUtils.containsAnyOrLessThan(bytes, offset, len, (byte) '"', (byte) '\\', 0x20);
    }

    private static byte[] filled(byte b) {
        final byte[] bytes = new byte[LENGTH];
        Arrays.fill(bytes, b);
        return bytes;
    }
}
