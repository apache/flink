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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for different combinations around {@link BuiltInFunctionDefinitions#ROW}. */
class RowFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ROW, "with field access")
                        .onFieldsWithData(12, "Hello world")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .testTableApiResult(
                                row($("f0"), $("f1")),
                                Row.of(12, "Hello world"),
                                DataTypes.ROW(
                                                DataTypes.FIELD("f0", DataTypes.INT()),
                                                DataTypes.FIELD("f1", DataTypes.STRING()))
                                        .notNull())
                        .testSqlResult(
                                "ROW(f0, f1)",
                                Row.of(12, "Hello world"),
                                DataTypes.ROW(
                                                DataTypes.FIELD("EXPR$0", DataTypes.INT()),
                                                DataTypes.FIELD("EXPR$1", DataTypes.STRING()))
                                        .notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ROW, "within function call")
                        .onFieldsWithData(12, "Hello world")
                        .andDataTypes(DataTypes.INT(), DataTypes.STRING())
                        .withFunction(TakesRow.class)
                        .testResult(
                                call("TakesRow", row($("f0"), $("f1")), 1),
                                "TakesRow(ROW(f0, f1), 1)",
                                Row.of(13, "Hello world"),
                                DataTypes.ROW(
                                        DataTypes.FIELD("i", DataTypes.INT()),
                                        DataTypes.FIELD("s", DataTypes.STRING()))),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ROW, "within cast")
                        .onFieldsWithData(1)
                        .testResult(
                                row($("f0").plus(12), "Hello world")
                                        .cast(
                                                DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "i", DataTypes.INT()),
                                                                DataTypes.FIELD(
                                                                        "s", DataTypes.STRING()))
                                                        .notNull()),
                                "CAST((f0 + 12, 'Hello world') AS ROW<i INT, s STRING>)",
                                Row.of(13, "Hello world"),
                                DataTypes.ROW(
                                                DataTypes.FIELD("i", DataTypes.INT()),
                                                DataTypes.FIELD("s", DataTypes.STRING()))
                                        .notNull()));
    }

    @Test
    void testRowFromNonLiteralInput() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        TableResult result =
                env.executeSql(
                        "SELECT "
                                + "CAST(ROW(CAST(a AS SMALLINT), CAST(b AS TINYINT)) AS ROW<a SMALLINT, b TINYINT>) "
                                + "AS `row` "
                                + "FROM (VALUES (1, 2)) AS T(a, b)");
        assertThat(result.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical(
                                        "row",
                                        DataTypes.ROW(
                                                        DataTypes.FIELD("a", DataTypes.SMALLINT()),
                                                        DataTypes.FIELD("b", DataTypes.TINYINT()))
                                                .notNull())));

        try (CloseableIterator<Row> it = result.collect()) {
            final Row row = (Row) it.next().getField(0);
            assertThat(row).isNotNull();
            assertThat(row.getField(0)).isEqualTo((short) 1);
            assertThat(row.getField(1)).isEqualTo((byte) 2);
            assertThat(it).isExhausted();
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Identity function for a row. */
    public static class TakesRow extends ScalarFunction {
        public @DataTypeHint("ROW<i INT, s STRING>") Row eval(
                @DataTypeHint("ROW<i INT, s STRING>") Row row, Integer i) {
            row.setField("i", (int) row.getField("i") + i);
            return row;
        }
    }
}
