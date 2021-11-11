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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ProjectedRowData}. */
public class ProjectedRowDataTest {

    @Test
    public void testProjectedRows() {
        final RowData initialRow = GenericRowData.of(0L, 1L, 2L, 3L, 4L);
        final ProjectedRowData projectedRowData =
                ProjectedRowData.from(
                        new int[][] {new int[] {2}, new int[] {0}, new int[] {1}, new int[] {4}});
        projectedRowData.replaceRow(initialRow);

        assertEquals(RowKind.INSERT, initialRow.getRowKind());
        assertEquals(4, projectedRowData.getArity());
        assertEquals(2L, projectedRowData.getLong(0));
        assertEquals(0L, projectedRowData.getLong(1));
        assertEquals(1L, projectedRowData.getLong(2));
        assertEquals(4L, projectedRowData.getLong(3));

        projectedRowData.replaceRow(GenericRowData.of(5L, 6L, 7L, 8L, 9L, 10L));
        assertEquals(4, projectedRowData.getArity());
        assertEquals(7L, projectedRowData.getLong(0));
        assertEquals(5L, projectedRowData.getLong(1));
        assertEquals(6L, projectedRowData.getLong(2));
        assertEquals(9L, projectedRowData.getLong(3));
    }

    @Test
    public void testProjectedRowsDoesntSupportNestedProjections() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ProjectedRowData.from(
                                new int[][] {
                                    new int[] {2}, new int[] {0, 1}, new int[] {1}, new int[] {4}
                                }));
    }
}
