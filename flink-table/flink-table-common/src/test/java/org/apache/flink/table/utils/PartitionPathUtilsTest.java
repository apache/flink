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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link org.apache.flink.table.utils.PartitionPathUtils}. */
public class PartitionPathUtilsTest {

    @Test
    public void testEscapeChar() {
        for (char c = 0; c <= 128; c++) {
            String expected = "%" + String.format("%1$02X", (int) c);
            String actual = PartitionPathUtils.escapeChar(c, new StringBuilder()).toString();
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testEscapePathNameWithHeadControl() {
        String origin = "[00";
        String expected = "%5B00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertEquals(expected, actual);
        assertEquals(origin, PartitionPathUtils.unescapePathName(actual));
    }

    @Test
    public void testEscapePathNameWithTailControl() {
        String origin = "00]";
        String expected = "00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertEquals(expected, actual);
        assertEquals(origin, PartitionPathUtils.unescapePathName(actual));
    }

    @Test
    public void testEscapePathNameWithMidControl() {
        String origin = "00:00";
        String expected = "00%3A00";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertEquals(expected, actual);
        assertEquals(origin, PartitionPathUtils.unescapePathName(actual));
    }

    @Test
    public void testEscapePathName() {
        String origin = "[00:00]";
        String expected = "%5B00%3A00%5D";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertEquals(expected, actual);
        assertEquals(origin, PartitionPathUtils.unescapePathName(actual));
    }

    @Test
    public void testEscapePathNameWithoutControl() {
        String origin = "0000";
        String expected = "0000";
        String actual = PartitionPathUtils.escapePathName(origin);
        assertEquals(expected, actual);
        assertEquals(origin, PartitionPathUtils.unescapePathName(actual));
    }
}
