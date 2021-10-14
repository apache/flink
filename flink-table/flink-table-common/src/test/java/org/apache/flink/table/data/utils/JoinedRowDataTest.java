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
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link JoinedRowData}. */
public class JoinedRowDataTest {

    @Test
    public void testJoinedRows() {
        final RowData row1 = GenericRowData.of(1L, 2L);
        final RowData row2 = GenericRowData.of(3L, StringData.fromString("4"));
        final RowData joinedRow = new JoinedRowData(row1, row2);

        assertEquals(RowKind.INSERT, joinedRow.getRowKind());
        assertEquals(4, joinedRow.getArity());
        assertEquals(1L, joinedRow.getLong(0));
        assertEquals(2L, joinedRow.getLong(1));
        assertEquals(3L, joinedRow.getLong(2));
        assertEquals("4", joinedRow.getString(3).toString());
    }

    @Test
    public void testJoinedRowKind() {
        final RowData joinedRow =
                new JoinedRowData(RowKind.DELETE, GenericRowData.of(), GenericRowData.of());
        assertEquals(RowKind.DELETE, joinedRow.getRowKind());
    }

    @Test
    public void testReplace() {
        final RowData row1 = GenericRowData.of(1L);
        final RowData row2 = GenericRowData.of(2L);
        final JoinedRowData joinedRow = new JoinedRowData(row1, row2);
        assertEquals(2, joinedRow.getArity());

        joinedRow.replace(GenericRowData.of(3L), GenericRowData.of(4L, 5L));
        assertEquals(3, joinedRow.getArity());
        assertEquals(3L, joinedRow.getLong(0));
        assertEquals(4L, joinedRow.getLong(1));
        assertEquals(5L, joinedRow.getLong(2));
    }
}
