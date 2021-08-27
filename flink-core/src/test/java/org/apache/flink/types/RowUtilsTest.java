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

package org.apache.flink.types;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RowUtils}. */
public class RowUtilsTest {

    @Test
    public void testCompareRowsUnordered() {
        final List<Row> originalList =
                Arrays.asList(
                        Row.of("a", 12, false),
                        Row.of("b", 12, false),
                        Row.of("b", 12, false),
                        Row.of("b", 12, true));

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true));
            assertTrue(RowUtils.compareRows(originalList, list, false));
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true), // diff order here
                            Row.of("b", 12, false));
            assertFalse(RowUtils.compareRows(originalList, list, false));
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true), // diff order here
                            Row.of("b", 12, false));
            assertTrue(RowUtils.compareRows(originalList, list, true));
        }

        {
            final List<Row> list =
                    Arrays.asList(
                            Row.of("a", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, false),
                            Row.of("b", 12, true),
                            Row.of("b", 12, true)); // diff here
            assertFalse(RowUtils.compareRows(originalList, list, true));
        }
    }
}
