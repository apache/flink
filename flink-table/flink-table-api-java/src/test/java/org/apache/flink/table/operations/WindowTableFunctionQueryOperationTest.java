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

package org.apache.flink.table.operations;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.WindowTableFunctionQueryOperation.WindowKind;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for describing {@link WindowTableFunctionQueryOperation}. */
class WindowTableFunctionQueryOperationTest {

    private static QueryOperation source() {
        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT()),
                        Column.physical("amount", DataTypes.INT()),
                        Column.physical(
                                "ts",
                                new AtomicDataType(
                                        new TimestampType(false, TimestampKind.ROWTIME, 3))));
        return new SourceQueryOperation(
                ContextResolvedTable.temporary(
                        ObjectIdentifier.of("cat1", "db1", "src"),
                        new ResolvedCatalogTable(
                                CatalogTable.newBuilder()
                                        .schema(Schema.newBuilder().build())
                                        .build(),
                                schema)));
    }

    @Test
    void testConstructorAppendsWindowColumns() {
        assertThat(
                        new WindowTableFunctionQueryOperation(
                                        WindowKind.TUMBLE,
                                        "ts",
                                        List.of(Duration.ofMinutes(10)),
                                        source())
                                .getResolvedSchema()
                                .getColumnNames())
                .containsExactly("id", "amount", "ts", "window_start", "window_end", "window_time");
    }

    @Test
    void testWindowColumnsMatchPlannerNullability() {
        final ResolvedSchema schema =
                new WindowTableFunctionQueryOperation(
                                WindowKind.TUMBLE, "ts", List.of(Duration.ofMinutes(10)), source())
                        .getResolvedSchema();

        assertThat(schema.getColumn("window_start").orElseThrow(AssertionError::new).getDataType())
                .isEqualTo(DataTypes.TIMESTAMP(3).notNull());
        assertThat(schema.getColumn("window_end").orElseThrow(AssertionError::new).getDataType())
                .isEqualTo(DataTypes.TIMESTAMP(3).notNull());
        assertThat(schema.getColumn("window_time").orElseThrow(AssertionError::new).getDataType())
                .isEqualTo(DataTypes.TIMESTAMP(3).notNull());
    }

    @Test
    void testWrongIntervalCountRejected() {
        assertThatThrownBy(
                        () ->
                                new WindowTableFunctionQueryOperation(
                                        WindowKind.HOP,
                                        "ts",
                                        List.of(Duration.ofMinutes(10)),
                                        source()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("HOP")
                .hasMessageContaining("requires 2 interval(s), but got 1");
    }

    @Test
    void testIntervalsArePreserved() {
        assertThat(
                        new WindowTableFunctionQueryOperation(
                                        WindowKind.HOP,
                                        "ts",
                                        List.of(Duration.ofMinutes(5), Duration.ofMinutes(10)),
                                        source())
                                .getIntervals())
                .containsExactly(Duration.ofMinutes(5), Duration.ofMinutes(10));
    }

    @Test
    void testUnknownTimeColumnRejected() {
        assertThatThrownBy(
                        () ->
                                new WindowTableFunctionQueryOperation(
                                        WindowKind.TUMBLE,
                                        "nope",
                                        List.of(Duration.ofMinutes(10)),
                                        source()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("nope");
    }

    @Test
    void testNonTimestampTimeColumnRejected() {
        assertThatThrownBy(
                        () ->
                                new WindowTableFunctionQueryOperation(
                                        WindowKind.TUMBLE,
                                        "amount",
                                        List.of(Duration.ofMinutes(10)),
                                        source()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("TIMESTAMP or TIMESTAMP_LTZ");
    }

    @Test
    void testNonTimeAttributeTimeColumnRejected() {
        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("id", DataTypes.INT()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3)));
        final QueryOperation plainTimestampSource =
                new SourceQueryOperation(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of("cat1", "db1", "src"),
                                new ResolvedCatalogTable(
                                        CatalogTable.newBuilder()
                                                .schema(Schema.newBuilder().build())
                                                .build(),
                                        schema)));

        assertThatThrownBy(
                        () ->
                                new WindowTableFunctionQueryOperation(
                                        WindowKind.TUMBLE,
                                        "ts",
                                        List.of(Duration.ofMinutes(10)),
                                        plainTimestampSource))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("time attribute");
    }

    @Test
    void testAsSerializableStringEmitsWindowTvf() {
        String sql =
                new WindowTableFunctionQueryOperation(
                                WindowKind.TUMBLE, "ts", List.of(Duration.ofMinutes(10)), source())
                        .asSerializableString();
        assertThat(sql).contains("TUMBLE((").contains("DESCRIPTOR(`ts`)").contains("INTERVAL");
    }
}
