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

package org.apache.flink.table.client.gateway.result;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.util.CliClientTestUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedCollectStreamResult}. */
class MaterializedCollectStreamResultTest extends BaseMaterializedResultTest {

    @Test
    void testSnapshot() {
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.INT()});

        @SuppressWarnings({"unchecked", "rawtypes"})
        final DataStructureConverter<RowData, Row> rowConverter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(schema.toPhysicalRowDataType());

        try (TestMaterializedCollectStreamResult result =
                new TestMaterializedCollectStreamResult(
                        CliClientTestUtils.createTestClient(schema),
                        Integer.MAX_VALUE,
                        createInternalBinaryRowDataConverter(schema.toPhysicalRowDataType()))) {
            result.isRetrieving = true;

            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "B", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "C", 2));

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(4));

            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(2),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(3),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("C", 2)),
                    result.retrievePage(4),
                    rowConverter);

            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "A", 1));

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(3));

            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(2),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("C", 2)),
                    result.retrievePage(3),
                    rowConverter);

            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "C", 2));
            result.processRecord(Row.ofKind(RowKind.UPDATE_BEFORE, "A", 1));
            result.processRecord(Row.ofKind(RowKind.UPDATE_AFTER, "D", 1));

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(2));

            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("D", 1)),
                    result.retrievePage(2),
                    rowConverter);
        }
    }

    @Test
    void testLimitedSnapshot() {
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.INT()});

        @SuppressWarnings({"unchecked", "rawtypes"})
        final DataStructureConverter<RowData, Row> rowConverter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(schema.toPhysicalRowDataType());

        // limit the materialized table to 2 rows
        // with 3 rows overcommitment
        try (TestMaterializedCollectStreamResult result =
                new TestMaterializedCollectStreamResult(
                        CliClientTestUtils.createTestClient(schema),
                        2,
                        3,
                        createInternalBinaryRowDataConverter(schema.toPhysicalRowDataType()))) {

            result.isRetrieving = true;

            result.processRecord(Row.ofKind(RowKind.INSERT, "D", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "B", 1));
            result.processRecord(Row.ofKind(RowKind.INSERT, "A", 1));

            assertRowEquals(
                    Arrays.asList(
                            null,
                            null,
                            Row.ofKind(RowKind.INSERT, "B", 1),
                            Row.ofKind(RowKind.INSERT, "A", 1)), // two over-committed rows
                    result.getMaterializedTable(),
                    rowConverter);

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(2));

            assertRowEquals(
                    Collections.singletonList(Row.ofKind(RowKind.INSERT, "B", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.ofKind(RowKind.INSERT, "A", 1)),
                    result.retrievePage(2),
                    rowConverter);

            result.processRecord(Row.ofKind(RowKind.INSERT, "C", 1));

            assertRowEquals(
                    Arrays.asList(
                            Row.ofKind(RowKind.INSERT, "A", 1),
                            Row.ofKind(RowKind.INSERT, "C", 1)), // limit clean up has taken place
                    result.getMaterializedTable(),
                    rowConverter);

            result.processRecord(Row.ofKind(RowKind.DELETE, "A", 1));

            assertRowEquals(
                    Collections.singletonList(
                            Row.ofKind(RowKind.INSERT, "C", 1)), // regular clean up has taken place
                    result.getMaterializedTable(),
                    rowConverter);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class TestMaterializedCollectStreamResult extends MaterializedCollectStreamResult
            implements AutoCloseable {

        private final Function<Row, BinaryRowData> converter;

        public boolean isRetrieving;

        public TestMaterializedCollectStreamResult(
                StatementResult tableResult,
                int maxRowCount,
                int overcommitThreshold,
                Function<Row, BinaryRowData> converter) {
            super(tableResult, maxRowCount, overcommitThreshold);
            this.converter = converter;
        }

        public TestMaterializedCollectStreamResult(
                StatementResult tableResult,
                int maxRowCount,
                Function<Row, BinaryRowData> converter) {
            super(tableResult, maxRowCount);
            this.converter = converter;
        }

        @Override
        protected boolean isRetrieving() {
            return isRetrieving;
        }

        public void processRecord(Row row) {
            processRecord(converter.apply(row));
        }
    }
}
