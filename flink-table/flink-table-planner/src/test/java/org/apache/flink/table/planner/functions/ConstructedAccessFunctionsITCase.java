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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

/** Tests for functions that access nested fields/elements of composite/collection types. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ConstructedAccessFunctionsITCase.FieldAccessFromTable.class,
    ConstructedAccessFunctionsITCase.FieldAccessAfterCall.class
})
public class ConstructedAccessFunctionsITCase {

    /**
     * Regular tests. See also {@link FieldAccessAfterCall} for tests that access a nested field of
     * an expression or for {@link BuiltInFunctionDefinitions#FLATTEN} which produces multiple
     * columns from a single one.
     */
    public static class FieldAccessFromTable extends BuiltInFunctionTestBase {
        @Parameterized.Parameters(name = "{index}: {0}")
        public static List<TestSpec> testData() {
            return Arrays.asList(

                    // Actually in case of SQL it does not use the GET method, but
                    // a custom logic for accessing nested fields of a Table.
                    TestSpec.forFunction(BuiltInFunctionDefinitions.GET)
                            .onFieldsWithData(null, Row.of(1))
                            .andDataTypes(
                                    ROW(FIELD("nested", BIGINT().notNull())).nullable(),
                                    ROW(FIELD("nested", BIGINT().notNull())).notNull())
                            .testResult(
                                    $("f0").get("nested"), "f0.nested", null, BIGINT().nullable())
                            .testResult($("f1").get("nested"), "f1.nested", 1L, BIGINT().notNull()),

                    // In Calcite it maps to FlinkSqlOperatorTable.ITEM
                    TestSpec.forFunction(BuiltInFunctionDefinitions.AT)
                            .onFieldsWithData(
                                    null,
                                    new int[] {1},
                                    null,
                                    singletonMap("nested", 1),
                                    null,
                                    Row.of(1))
                            .andDataTypes(
                                    ARRAY(BIGINT().notNull()).nullable(),
                                    ARRAY(BIGINT().notNull()).notNull(),
                                    MAP(STRING(), BIGINT().notNull()).nullable(),
                                    MAP(STRING(), BIGINT().notNull()).notNull(),
                                    ROW(FIELD("nested", BIGINT().notNull())).nullable(),
                                    ROW(FIELD("nested", BIGINT().notNull())).notNull())
                            // accessing elements of MAP or ARRAY is a runtime operations,
                            // we do not know about the size or contents during the inference
                            // therefore the results are always nullable
                            .testSqlResult("f0[1]", null, BIGINT().nullable())
                            .testSqlResult("f1[1]", 1L, BIGINT().nullable())
                            .testSqlResult("f2['nested']", null, BIGINT().nullable())
                            .testSqlResult("f3['nested']", 1L, BIGINT().nullable())

                            // we know all the fields of a type up front, therefore we can
                            // derive more accurate types during the inference
                            .testSqlResult("f4['nested']", null, BIGINT().nullable())
                            .testSqlResult("f5['nested']", 1L, BIGINT().notNull()));
        }
    }

    /** A class for customized tests. */
    public static class FieldAccessAfterCall {

        @ClassRule
        public static MiniClusterWithClientResource miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(1)
                                .build());

        @Rule public ExpectedException thrown = ExpectedException.none();

        @Test
        public void testSqlAccessingNullableRow() {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            env.createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);

            thrown.expect(ValidationException.class);
            thrown.expectMessage(
                    "Invalid function call:\n" + "CustomScalarFunction(INT NOT NULL, INT)");
            env.executeSql("SELECT CustomScalarFunction(1, CustomScalarFunction().nested)");
        }

        @Test
        public void testSqlAccessingNotNullRow() throws Exception {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            env.createTemporarySystemFunction("CustomScalarFunction", CustomScalarFunction.class);

            TableResult result =
                    env.executeSql(
                            "SELECT CustomScalarFunction(1, CustomScalarFunction(1).nested)");
            try (CloseableIterator<Row> it = result.collect()) {
                assertThat(it.next(), equalTo(Row.of(2L)));
                assertFalse(it.hasNext());
            }
        }

        @Test
        public void testSqlAccessingNullableRowWithAlias() throws Exception {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            env.createTemporarySystemFunction("RowTableFunction", RowTableFunction.class);

            TableResult result =
                    env.executeSql(
                            "SELECT t.b, t.a FROM "
                                    + "(SELECT * FROM (VALUES(1))), "
                                    + "LATERAL TABLE(RowTableFunction()) AS t(a, b)");
            assertThat(
                    result.getResolvedSchema(),
                    equalTo(
                            ResolvedSchema.of(
                                    Column.physical(
                                            "b", DataTypes.ARRAY(DataTypes.STRING()).notNull()),
                                    Column.physical("a", DataTypes.STRING()))));
            try (CloseableIterator<Row> it = result.collect()) {
                assertThat(it.next(), equalTo(Row.of(new String[] {"A", "B"}, "A")));
                assertFalse(it.hasNext());
            }
        }

        @Test
        public void testTableApiAccessingNullableRow() {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());

            thrown.expect(ValidationException.class);
            thrown.expectMessage(
                    "Invalid function call:\n" + "CustomScalarFunction(INT NOT NULL, INT)");
            env.fromValues(1)
                    .select(
                            call(
                                    CustomScalarFunction.class,
                                    1,
                                    call(CustomScalarFunction.class).get("nested")))
                    .execute();
        }

        @Test
        public void testTableApiAccessingNotNullRow() throws Exception {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());

            TableResult result =
                    env.fromValues(1)
                            .select(
                                    call(
                                            CustomScalarFunction.class,
                                            1,
                                            call(CustomScalarFunction.class, 1).get("nested")))
                            .execute();
            try (CloseableIterator<Row> it = result.collect()) {
                assertThat(it.next(), equalTo(Row.of(2L)));
                assertFalse(it.hasNext());
            }
        }

        @Test
        public void testTableApiFlattenRowType() throws Exception {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());

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

            assertThat(
                    result.getResolvedSchema(),
                    equalTo(
                            ResolvedSchema.of(
                                    Column.physical("f0$nested0", BIGINT().nullable()),
                                    Column.physical("f0$nested1", STRING().nullable()))));

            try (CloseableIterator<Row> it = result.collect()) {
                assertThat(it.next(), equalTo(Row.of(1L, "ABC")));
                assertFalse(it.hasNext());
            }
        }

        @Test
        public void testTableApiFlattenStructuredType() throws Exception {
            final TableEnvironment env =
                    TableEnvironment.create(EnvironmentSettings.inStreamingMode());

            final Row row =
                    Row.of(
                            1,
                            LocalDateTime.parse("2012-12-12T12:12:12.001"),
                            "a",
                            Row.of(10, "aa"));

            final Table data = env.fromValues(row);

            final TableResult result =
                    data.select(call(PojoConstructorScalarFunction.class, $("*")).flatten())
                            .execute();

            assertThat(
                    result.getResolvedSchema(),
                    equalTo(
                            ResolvedSchema.of(
                                    Column.physical("_c0", INT().bridgedTo(int.class)),
                                    Column.physical("_c1", TIMESTAMP(3)),
                                    Column.physical("_c2", STRING()),
                                    Column.physical(
                                            "_c3",
                                            ROW(FIELD("ri", INT()), FIELD("rs", STRING()))))));

            try (CloseableIterator<Row> it = result.collect()) {
                assertThat(it.next(), equalTo(row));
                assertFalse(it.hasNext());
            }
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
