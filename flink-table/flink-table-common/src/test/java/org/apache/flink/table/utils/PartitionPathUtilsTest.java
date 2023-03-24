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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.table.utils.PartitionPathUtils}. */
class PartitionPathUtilsTest {

    @Test
    void testEscapeChar() {
        for (char c = 0; c <= 128; c++) {
            String expected = "%" + String.format("%1$02X", (int) c);
            String actual = PartitionPathUtils.escapeChar(c, new StringBuilder()).toString();
            assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    void testEscapePathNameWithHeadControl() {
        String origin = "[00";
        String expected = "%5B00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithTailControl() {
        String origin = "00]";
        String expected = "00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithMidControl() {
        String origin = "00:00";
        String expected = "00%3A00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathName() {
        String origin = "[00:00]";
        String expected = "%5B00%3A00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithoutControl() {
        String origin = "0000";
        String expected = "0000";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }

    @Test
    void testEscapePathNameWithCurlyBraces() {
        String origin = "{partName}";
        String expected = "%7BpartName%7D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertThat(actual).isEqualTo(expected);
        assertThat(PartitionPathUtils.unescapePathName(actual)).isEqualTo(origin);
    }
}
