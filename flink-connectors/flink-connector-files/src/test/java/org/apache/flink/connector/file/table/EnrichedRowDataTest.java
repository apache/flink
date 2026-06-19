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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link JoinedRowData}. */
class EnrichedRowDataTest {

    @Test
    void testEnrichedRow() {
        final List<String> completeRowFields =
                Arrays.asList(
                        "fixedRow1",
                        "mutableRow1",
                        "mutableRow3",
                        "mutableRow2",
                        "fixedRow2",
                        "mutableRow4");
        final List<String> mutableRowFields =
                Arrays.asList("mutableRow1", "mutableRow2", "mutableRow3", "mutableRow4");
        final List<String> fixedRowFields = Arrays.asList("fixedRow1", "fixedRow2");

        final RowData fixedRowData = GenericRowData.of(1L, 2L);
        final EnrichedRowData enrichedRowData =
                EnrichedRowData.from(
                        fixedRowData, completeRowFields, mutableRowFields, fixedRowFields);
        final RowData mutableRowData = GenericRowData.of(3L, 4L, 5L, 6L);
        enrichedRowData.replaceMutableRow(mutableRowData);

        assertEquals(RowKind.INSERT, enrichedRowData.getRowKind());
        assertEquals(6, enrichedRowData.getArity());
        assertEquals(1L, enrichedRowData.getLong(0));
        assertEquals(3L, enrichedRowData.getLong(1));
        assertEquals(5L, enrichedRowData.getLong(2));
        assertEquals(4L, enrichedRowData.getLong(3));
        assertEquals(2L, enrichedRowData.getLong(4));
        assertEquals(6L, enrichedRowData.getLong(5));

        final RowData newMutableRowData = GenericRowData.of(7L, 8L, 9L, 10L);
        enrichedRowData.replaceMutableRow(newMutableRowData);

        assertEquals(1L, enrichedRowData.getLong(0));
        assertEquals(7L, enrichedRowData.getLong(1));
        assertEquals(9L, enrichedRowData.getLong(2));
        assertEquals(8L, enrichedRowData.getLong(3));
        assertEquals(2L, enrichedRowData.getLong(4));
        assertEquals(10L, enrichedRowData.getLong(5));
    }
}
