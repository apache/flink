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

import java.time.DayOfWeek;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StringUtils}. */
class StringUtilsTest {

    @Test
    void testControlCharacters() {
        String testString = "\b \t \n \f \r default";
        String controlString = StringUtils.showControlCharacters(testString);
        assertThat(controlString).isEqualTo("\\b \\t \\n \\f \\r default");
    }

    @Test
    void testArrayAwareToString() {
        assertThat(StringUtils.arrayAwareToString(null)).isEqualTo("null");

        assertThat(StringUtils.arrayAwareToString(DayOfWeek.MONDAY)).isEqualTo("MONDAY");

        assertThat(StringUtils.arrayAwareToString(new int[] {1, 2, 3})).isEqualTo("[1, 2, 3]");

        assertThat(StringUtils.arrayAwareToString(new byte[][] {{4, 5, 6}, null, {}}))
                .isEqualTo("[[4, 5, 6], null, []]");

        assertThat(
                        StringUtils.arrayAwareToString(
                                new Object[] {new Integer[] {4, 5, 6}, null, DayOfWeek.MONDAY}))
                .isEqualTo("[[4, 5, 6], null, MONDAY]");
    }

    @Test
    void testStringToHexArray() {
        String hex = "019f314a";
        byte[] hexArray = StringUtils.hexStringToByte(hex);
        byte[] expectedArray = new byte[] {1, -97, 49, 74};
        assertThat(hexArray).isEqualTo(expectedArray);
    }

    @Test
    void testHexArrayToString() {
        byte[] byteArray = new byte[] {1, -97, 49, 74};
        String hex = StringUtils.byteToHexString(byteArray);
        assertThat(hex).isEqualTo("019f314a");
    }

    @Test
    void testGenerateAlphanumeric() {
        String str = StringUtils.generateRandomAlphanumericString(new Random(), 256);

        assertThat(str).matches("[a-zA-Z0-9]{256}");
    }
}
