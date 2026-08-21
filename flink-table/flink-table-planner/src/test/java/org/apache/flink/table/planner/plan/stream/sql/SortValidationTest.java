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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.utils.InternalConfigOptions;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan-time validation tests for streaming sort. A non-time-attribute sort is unsupported and must
 * be rejected during optimization (so it surfaces at {@code COMPILE PLAN} / planning time), not
 * deferred to execution-plan translation. Valid temporal sorts are covered by {@code SortTest}.
 */
class SortValidationTest extends TableTestBase {

    private static final String MESSAGE =
            "requires the primary sort key to be a time attribute in ascending order";
    private static final String MESSAGE_DESC =
            "must be sorted in ascending order; descending order is not supported";

    private final JavaStreamTableTestUtil util = javaStreamTestUtil();

    @BeforeEach
    void setup() {
        util.addTable(
                "CREATE TABLE MyTable (\n"
                        + "  a INT,\n"
                        + "  b STRING,\n"
                        + "  c BIGINT,\n"
                        + "  proctime AS PROCTIME(),\n"
                        + "  rowtime TIMESTAMP(3),\n"
                        + "  WATERMARK FOR rowtime AS rowtime\n"
                        + ") WITH ('connector' = 'values')");
    }

    static Stream<Arguments> nonTemporalSorts() {
        return Stream.of(
                // primary sort key is not a time attribute -> message A
                Arguments.of("SELECT a FROM MyTable ORDER BY c", MESSAGE),
                Arguments.of("SELECT a FROM MyTable ORDER BY c, proctime", MESSAGE),
                Arguments.of("SELECT a FROM MyTable ORDER BY c, rowtime", MESSAGE),
                Arguments.of("SELECT a FROM MyTable ORDER BY c, proctime DESC", MESSAGE),
                Arguments.of("SELECT a FROM MyTable ORDER BY c, rowtime DESC", MESSAGE),
                // primary sort key is a time attribute but sorted descending -> message B
                Arguments.of("SELECT a FROM MyTable ORDER BY proctime DESC, c", MESSAGE_DESC),
                Arguments.of("SELECT a FROM MyTable ORDER BY rowtime DESC, c", MESSAGE_DESC));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonTemporalSorts")
    void testNonTemporalSortRejected(String query, String expectedMessage) {
        assertThatThrownBy(() -> util.verifyExecPlan(query))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(expectedMessage);
    }

    @Test
    void testMessageNamesColumnAndType() {
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(MESSAGE)
                .hasMessageContaining("'c' is BIGINT");
    }

    @Test
    void testNonTemporalSortAllowedWhenEnabled() {
        util.getTableEnv()
                .getConfig()
                .set(InternalConfigOptions.TABLE_EXEC_NON_TEMPORAL_SORT_ENABLED, true);
        // With the internal flag enabled, the plan-time check is skipped and the sort is accepted.
        assertThatCode(() -> util.getTableEnv().explainSql("SELECT a FROM MyTable ORDER BY c"))
                .doesNotThrowAnyException();
    }

    @Test
    void testCompilePlanRejectsNonTemporalSort() {
        util.addTable("CREATE TABLE MySink (a INT) WITH ('connector' = 'values')");
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .compilePlanSql(
                                                "INSERT INTO MySink SELECT a FROM MyTable ORDER BY c"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(MESSAGE);
    }
}
