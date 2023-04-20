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
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.LocalDateTime;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for functions that access nested fields/elements of composite/collection types. */
@Execution(ExecutionMode.CONCURRENT)
@ExtendWith(MiniClusterExtension.class)
class ConstructedAccessFunctionsITCase {

    @Test
    public void testSqlAccessingNullableRow() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);

        assertThatThrownBy(
                        () ->
                                env.executeSql(
                                        "SELECT CustomScalarFunction(1, CustomScalarFunction().nested)"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Invalid function call:\n"
                                        + "CustomScalarFunction(INT NOT NULL, INT)"));
    }

    @Test
    public void testSqlAccessingNotNullRow() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);

        TableResult result =
                env.executeSql("SELECT CustomScalarFunction(1, CustomScalarFunction(1).nested)");
        try (CloseableIterator<Row> it = result.collect()) {
            assertThat(it.next()).isEqualTo(Row.of(2L));
            assertThat(it).isExhausted();
        }
    }

    @Test
    public void testSqlAccessingNullableRowWithAlias() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);

        TableResult result =
                env.executeSql(
                        "SELECT t.b, t.a FROM "
                                + "(SELECT * FROM (VALUES(1))), "
                                + "LATERAL TABLE(RowTableFunction()) AS t(a, b)");
        assertThat(result.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("b", DataTypes.ARRAY(DataTypes.STRING()).notNull()),
                                Column.physical("a", DataTypes.STRING())));
        try (CloseableIterator<Row> it = result.collect()) {
            assertThat(it.next()).isEqualTo(Row.of(new String[] {"A", "B"}, "A"));
            assertThat(it).isExhausted();
        }
    }

    @Test
    public void testTableApiAccessingNullableRow() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        assertThatThrownBy(
                        () ->
                                env.fromValues(1)
                                        .select(
                                                call(
                                                        CustomScalarFunction.class,
                                                        1,
                                                        call(CustomScalarFunction.class)
                                                                .get("nested")))
                                        .execute())
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Invalid function call:\n"
                                        + "CustomScalarFunction(INT NOT NULL, INT)"));
    }

    @Test
    public void testTableApiAccessingNotNullRow() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        TableResult result =
                env.fromValues(1)
                        .select(
                                call(
                                        CustomScalarFunction.class,
                                        1,
                                        call(CustomScalarFunction.class, 1).get("nested")))
                        .execute();
        try (CloseableIterator<Row> it = result.collect()) {
            assertThat(it.next()).isEqualTo(Row.of(2L));
            assertThat(it).isExhausted();
        }
    }

    @Test
    public void testTableApiFlattenRowType() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        TableResult result =
                env.fromValues(
                                ROW(FIELD(
                                                "f0",
                                                ROW(
                                                                FIELD(
                                                                        "nested0",
                                                                        BIGINT().notNull()),
                                                                FIELD("nested1", STRING()))
                                                        .nullable()))
                                        .notNull(),
                                Row.of(Row.of(1, "ABC")))
                        .select($("f0").flatten())
                        .execute();

        assertThat(result.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f0$nested0", BIGINT().nullable()),
                                Column.physical("f0$nested1", STRING().nullable())));

        try (CloseableIterator<Row> it = result.collect()) {
            assertThat(it.next()).isEqualTo(Row.of(1L, "ABC"));
            assertThat(it).isExhausted();
        }
    }

    @Test
    public void testTableApiFlattenStructuredType() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Row row =
                Row.of(1, LocalDateTime.parse("2012-12-12T12:12:12.001"), "a", Row.of(10, "aa"));

        final Table data = env.fromValues(row);

        final TableResult result =
                data.select(call(PojoConstructorScalarFunction.class, $("*")).flatten()).execute();

        assertThat(result.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("_c0", INT().bridgedTo(int.class)),
                                Column.physical("_c1", TIMESTAMP(3)),
                                Column.physical("_c2", STRING()),
                                Column.physical(
                                        "_c3", ROW(FIELD("ri", INT()), FIELD("rs", STRING())))));

        try (CloseableIterator<Row> it = result.collect()) {
            assertThat(it.next()).isEqualTo(row);
            assertThat(it).isExhausted();
        }
    }

    /**
     * A helper function for testing accessing nested fields of a {@link LogicalTypeRoot#ROW} type.
     *
     * <p>It has three different methods that
     *
     * <ul>
     *   <li>create a nullable ROW with not null nested field
     *   <li>create a not null ROW with not null nested field
     *   <li>expect not null arguments
     * </ul>
     */
    public static class CustomScalarFunction extends ScalarFunction {
        public long eval(int i, long l) {
            return i + l;
        }

        public @DataTypeHint("ROW<nested INT NOT NULL>") Row eval() {
            return null;
        }

        public @DataTypeHint("ROW<nested INT NOT NULL> NOT NULL") Row eval(int nested) {
            return Row.of(nested);
        }
    }

    /** Table function that returns a nullable row. */
    @FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING> NOT NULL>"))
    public static class RowTableFunction extends TableFunction<Row> {
        public void eval() {
            collect(null);
            collect(Row.of("A", new String[] {"A", "B"}));
        }
    }

    /** Scalar function that returns a POJO. */
    public static class PojoConstructorScalarFunction extends ScalarFunction {
        public ComplexPojo eval(
                int i,
                @DataTypeHint("TIMESTAMP(3)") LocalDateTime t,
                String s,
                @DataTypeHint("ROW<ri INT, rs STRING>") Row r) {
            return new ComplexPojo(i, t, s, r);
        }
    }

    /** Complex POJO for testing flattening. */
    public static class ComplexPojo {
        public final int i;

        public final @DataTypeHint("TIMESTAMP(3)") LocalDateTime t;

        public final String s;

        public final @DataTypeHint("ROW<ri INT, rs STRING>") Row r;

        public ComplexPojo(int i, LocalDateTime t, String s, Row r) {
            this.i = i;
            this.t = t;
            this.s = s;
            this.r = r;
        }
    }
}
