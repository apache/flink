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
import org.apache.flink.table.planner.utils.PlannerMocks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link FlinkCalciteSqlValidator}. */
class FlinkCalciteSqlValidatorTest {

    private final PlannerMocks plannerMocks =
            PlannerMocks.create()
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
}
