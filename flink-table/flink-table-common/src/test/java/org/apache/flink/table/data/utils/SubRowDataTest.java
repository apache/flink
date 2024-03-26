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

package org.apache.flink.table.data.utils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for {@link SubRowData}. */
public class SubRowDataTest {
    @Test
    public void testProjection() {
        final RowData sourceRow = GenericRowData.of(1, 2, 3, 4, 5);
        final RowData subRow = new SubRowData(sourceRow, 1, 3);
        assertSubRow(subRow, 2, 3);
    }

    @Test
    public void testRowKind() {
        final RowData sourceRow = GenericRowData.ofKind(RowKind.DELETE, 1, 2, 3);
        final RowData subRow = new SubRowData(sourceRow, 0, 3);
        assertSubRow(subRow, 1, 2, 3);
        assertEquals(RowKind.DELETE, subRow.getRowKind());
    }

    @Test
    public void testEdgeCases() {
        final RowData sourceRow = GenericRowData.of(1, 2, 3);

        // Complete row
        assertSubRow(new SubRowData(sourceRow, 0, 3), 1, 2, 3);

        // Left edge
        assertSubRow(new SubRowData(sourceRow, 0, 1), 1);

        // Right edge
        assertSubRow(new SubRowData(sourceRow, 2, 3), 3);

        // Empty
        assertEquals(0, new SubRowData(sourceRow, 0, 0).getArity());
        assertEquals(0, new SubRowData(sourceRow, 2, 2).getArity());
    }

    @Test
    public void testInvalidCases() {
        final RowData sourceRow = GenericRowData.of(1, 2, 3);

        // startIndex < 0
        assertThrows(
                () -> new SubRowData(sourceRow, -1, 1),
                IllegalArgumentException.class,
                "startIndex must be within bounds.");

        // startIndex >= arity
        assertThrows(
                () -> new SubRowData(sourceRow, 3, 2),
                IllegalArgumentException.class,
                "startIndex must be within bounds.");

        // endIndex < 0
        assertThrows(
                () -> new SubRowData(sourceRow, 1, -1),
                IllegalArgumentException.class,
                "endIndex must be within bounds.");

        // endIndex > arity
        assertThrows(
                () -> new SubRowData(sourceRow, 2, 4),
                IllegalArgumentException.class,
                "endIndex must be within bounds.");

        // endIndex < startIndex
        assertThrows(
                () -> new SubRowData(sourceRow, 2, 1),
                IllegalArgumentException.class,
                "endIndex must be within bounds.");

        // Out of bounds read
        assertThrows(
                () -> new SubRowData(sourceRow, 0, 1).getInt(1),
                IllegalArgumentException.class,
                "Position '1' is out of bounds.");
    }

    private void assertSubRow(RowData subRow, int... expectedValues) {
        assertEquals(expectedValues.length, subRow.getArity());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], subRow.getInt(i));
        }
    }

    private <E extends Exception> void assertThrows(
            Runnable runnable, Class<E> clazz, String expectedMessage) {
        try {
            runnable.run();

            fail(
                    String.format(
                            "Expected '%s: %s', but no exception occurred.",
                            clazz.getSimpleName(), expectedMessage));
        } catch (Exception e) {
            if (clazz.isAssignableFrom(e.getClass())) {
                assertEquals(expectedMessage, e.getMessage());
            } else {
                throw e;
            }
        }
    }
}
