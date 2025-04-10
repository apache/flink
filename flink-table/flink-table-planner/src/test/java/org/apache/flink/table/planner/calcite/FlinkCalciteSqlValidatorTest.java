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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

/** Test for {@link FlinkCalciteSqlValidator}. */
class FlinkCalciteSqlValidatorTest {

    /** Test scalar UDF that performs a sum (needs to be public to be registered). * */
    public static class SumUDF extends ScalarFunction {
        public String eval(Integer a, Integer b) {
            return String.valueOf(a + b);
        }
    }

    /** Test scalar UDF returns the same string as given (needs to be public to be registered). * */
    public static class StringIdentityUDF extends ScalarFunction {
        public String eval(String s) {
            return s;
        }
    }

    private final PlannerMocks plannerMocks =
            PlannerMocks.create()
                    .registerFunction("udfSum", SumUDF.class)
                    .registerFunction("udfId", StringIdentityUDF.class)
                    .registerTemporaryTable(
                            "t1", Schema.newBuilder().column("a", DataTypes.INT()).build())
                    .registerTemporaryTable(
                            "t2",
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .build())
                    .registerTemporaryTable(
                            "t2_copy",
                            Schema.newBuilder()
                                    .column("a", DataTypes.INT())
                                    .column("b", DataTypes.INT())
                                    .build())
                    .registerTemporaryTable(
                            "t_nested",
                            Schema.newBuilder()
                                    .column(
                                            "f",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("a", DataTypes.INT()),
                                                    DataTypes.FIELD("b", DataTypes.INT())))
                                    .build());

    @Test
    void testUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "INSERT INTO t2 (a, b) VALUES (1)",
                "INSERT INTO t2 (a, b) VALUES (1, 2), (3)",
                "INSERT INTO t2 (a, b) VALUES (1), (2, 3)",
                "INSERT INTO t2 (a, b) VALUES (1, 2), (3, 4, 5)",
                "INSERT INTO t2 (a, b) SELECT 1",
                "INSERT INTO t2 (a, b) SELECT COALESCE(123, 456), LEAST(1, 2), GREATEST(3, 4, 5)",
                "INSERT INTO t2 (a, b) SELECT * FROM t1",
                "INSERT INTO t2 (a, b) SELECT *, *, * FROM t1",
                "INSERT INTO t2 (a, b) SELECT *, 42 FROM t2_copy",
                "INSERT INTO t2 (a, b) SELECT 42, * FROM t2_copy",
                "INSERT INTO t2 (a, b) SELECT * FROM t_nested",
                "INSERT INTO t2 (a, b) TABLE t_nested",
                "INSERT INTO t2 (a, b) SELECT * FROM (TABLE t_nested)",
                "INSERT INTO t2 (a, b) WITH cte AS (SELECT 1, 2, 3) SELECT * FROM cte",
                "INSERT INTO t2 (a, b) WITH cte AS (SELECT * FROM t1, t2_copy) SELECT * FROM cte",
                "INSERT INTO t2 (a, b) WITH cte1 AS (SELECT 1, 2), cte2 AS (SELECT 2, 1) SELECT * FROM cte1, cte2"
            })
    void testInvalidNumberOfColumnsWhileInsertInto(String sql) {
        assertThatThrownBy(() -> plannerMocks.getParser().parse(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(" Number of columns must match number of query columns");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "INSERT INTO t2 (a, b) VALUES (1, 2), (3, 4)",
                "INSERT INTO t2 (a) VALUES (1), (3)",
                "INSERT INTO t2 (a, b) SELECT 1, 2",
                "INSERT INTO t2 (a, b) SELECT LEAST(1, 2, 3), 2 * 2",
                "INSERT INTO t2 (a, b) SELECT * FROM t2_copy",
                "INSERT INTO t2 (a, b) SELECT *, * FROM t1",
                "INSERT INTO t2 (a, b) SELECT *, 42 FROM t1",
                "INSERT INTO t2 (a, b) SELECT 42, * FROM t1",
                "INSERT INTO t2 (a, b) SELECT f.* FROM t_nested",
                "INSERT INTO t2 (a, b) TABLE t2_copy",
                "INSERT INTO t2 (a, b) WITH cte AS (SELECT 1, 2) SELECT * FROM cte",
                "INSERT INTO t2 (a, b) WITH cte AS (SELECT * FROM t2_copy) SELECT * FROM cte",
                "INSERT INTO t2 (a, b) WITH cte AS (SELECT t1.a, t2_copy.b FROM t1, t2_copy) SELECT * FROM cte",
                "INSERT INTO t2 (a, b) WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2",
                "INSERT INTO t2 (a, b) "
                        + "WITH cte1 AS (SELECT 1, 2), "
                        + "cte2 AS (SELECT 2, 3) "
                        + "SELECT * FROM cte1 UNION SELECT * FROM cte2",
                "INSERT INTO t2 (a, b) "
                        + "WITH cte1 AS (SELECT 1, 2), "
                        + "cte2 AS (SELECT 2, 3), "
                        + "cte3 AS (SELECT 3, 4), "
                        + "cte4 AS (SELECT 4, 5) "
                        + "SELECT * FROM cte1 "
                        + "UNION SELECT * FROM cte2 "
                        + "INTERSECT SELECT * FROM cte3 "
                        + "UNION ALL SELECT * FROM cte4"
            })
    void validInsertIntoTest(final String sql) {
        assertDoesNotThrow(
                () -> {
                    plannerMocks.getParser().parse(sql);
                });
    }

    @Test
    void testExplainUpsertInto() {
        assertThatThrownBy(() -> plannerMocks.getParser().parse("EXPLAIN UPSERT INTO t1 VALUES(1)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.");
    }

    private static Stream<Arguments> provideUDFStarInputs() {
        final String prefix = "`default_catalog`.`default_database`.";
        return Stream.of(
                Arguments.of("SELECT udfSum(*) FROM t2", prefix + "`udfSum`(`t2`.`a`, `t2`.`b`)"),
                Arguments.of(
                        "SELECT udfSum(*) FROM t2 as foo2",
                        prefix + "`udfSum`(`foo2`.`a`, `foo2`.`b`)"),
                Arguments.of(
                        "SELECT udfSum(bar2.*) FROM t2 as bar2",
                        prefix + "`udfSum`(`bar2`.`a`, `bar2`.`b`)"),
                Arguments.of(
                        "SELECT udfId(udfSum(*)) FROM t2",
                        prefix + "`udfId`(" + prefix + "`udfSum`(`t2`.`a`, `t2`.`b`))"));
    }

    @ParameterizedTest
    @MethodSource("provideUDFStarInputs")
    void testStarInUDFCallInSelect(String sql, String expected) {
        List<Operation> parsed = plannerMocks.getParser().parse(sql);
        assertEquals(1, parsed.size());
        Operation op = parsed.get(0);
        assertInstanceOf(PlannerQueryOperation.class, op);
        PlannerQueryOperation queryOp = (PlannerQueryOperation) op;
        int selectStartIdx = queryOp.asSerializableString().indexOf("SELECT ") + "SELECT ".length();
        int selectEndIdx = queryOp.asSerializableString().indexOf("FROM");
        String selectClause =
                queryOp.asSerializableString().substring(selectStartIdx, selectEndIdx);
        assertThat(selectClause).contains(expected);
    }

    @Test
    void testStarInUDFCallInWhere() {
        assertThatThrownBy(
                        () ->
                                plannerMocks
                                        .getParser()
                                        .parse("SELECT * FROM t2 WHERE udfSum(*) > 0"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "SQL validation failed. At line 1, column 31: Unknown identifier '*'");
    }
}
