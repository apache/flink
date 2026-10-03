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

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.table.planner.utils.InternalConfigOptions;
import org.apache.flink.table.planner.utils.JavaStreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan-time validation tests for streaming sort. A non-time-attribute sort is unsupported and is
 * rejected by {@code StreamPhysicalSortRule} during optimization, so it surfaces at {@code COMPILE
 * PLAN} / planning time. {@code StreamExecSort} keeps the same check as a backstop for compiled
 * plans loaded via {@code loadPlan}, which bypasses optimization. Valid temporal sorts are covered
 * by {@code SortTest}.
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
                Arguments.of("SELECT a FROM MyTable ORDER BY rowtime DESC, c", MESSAGE_DESC),
                // ordinals and aliases are expanded by the validator and hit the same rule
                Arguments.of("SELECT a FROM MyTable ORDER BY 1", MESSAGE),
                Arguments.of("SELECT c AS x FROM MyTable ORDER BY x", MESSAGE),
                Arguments.of("SELECT rowtime AS t, a FROM MyTable ORDER BY t DESC", MESSAGE_DESC),
                // an expression is projected into a generated column below the sort
                Arguments.of("SELECT a FROM MyTable ORDER BY c + 1", MESSAGE));
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
    void testMessageDescribesSqlExpressionKey() {
        // The SQL converter projects the ORDER BY expression as EXPR$n; the message must not
        // leak that alias. The Volcano wrapper prepends the plan (which does contain the
        // alias), so assert on the rule's own exception.
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT a FROM MyTable ORDER BY c + 1"))
                .rootCause()
                .isInstanceOf(TableException.class)
                .hasMessageContaining("the sort key expression is BIGINT")
                .hasMessageNotContaining("EXPR$");
    }

    @Test
    void testMessageDescribesTableApiExpressionKey() {
        // The Table API projects the ORDER BY expression as $fn instead of EXPR$n.
        Table table = util.getTableEnv().from("MyTable").orderBy($("c").plus(1));
        assertThatThrownBy(() -> util.verifyExecPlan(table))
                .rootCause()
                .isInstanceOf(TableException.class)
                .hasMessageContaining("the sort key expression is BIGINT")
                .hasMessageNotContaining("$f");
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

    @Test
    void testLoadedPlanRejectsNonTemporalSort() {
        String json =
                compileWithNonTemporalSortEnabled(
                        "INSERT INTO MySink SELECT a FROM MyTable ORDER BY c");
        assertTranslationRejected(json, MESSAGE);
    }

    @Test
    void testLoadedPlanRejectsDescendingTimeAttributeSort() {
        String json =
                compileWithNonTemporalSortEnabled(
                        "INSERT INTO MySink SELECT a FROM MyTable ORDER BY rowtime DESC");
        // The backstop can only tell the two cases apart if the time attribute survives the
        // round trip; fail on that premise rather than silently drifting to the other message.
        assertThat(json).containsPattern("\"kind\"\\s*:\\s*\"ROWTIME\"");
        assertTranslationRejected(json, MESSAGE_DESC);
    }

    /**
     * Compiles the insert with the internal flag enabled so the sort passes {@code
     * StreamPhysicalSortRule}. The flag is not persisted in the plan.
     */
    private String compileWithNonTemporalSortEnabled(String insert) {
        util.addTable("CREATE TABLE MySink (a INT) WITH ('connector' = 'values')");
        util.getTableEnv()
                .getConfig()
                .set(InternalConfigOptions.TABLE_EXEC_NON_TEMPORAL_SORT_ENABLED, true);
        return util.getTableEnv().compilePlanSql(insert).asJsonString();
    }

    /**
     * Loads the plan with the flag disabled and translates it, which reaches the backstop in {@code
     * StreamExecSort} without passing through {@code StreamPhysicalSortRule}.
     */
    private void assertTranslationRejected(String json, String expectedMessage) {
        TableEnvironment tEnv = util.getTableEnv();
        tEnv.getConfig().set(InternalConfigOptions.TABLE_EXEC_NON_TEMPORAL_SORT_ENABLED, false);
        CompiledPlan loaded = tEnv.loadPlan(PlanReference.fromJsonString(json));
        assertThatThrownBy(() -> CompiledPlanUtils.toTransformations(tEnv, loaded))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(expectedMessage);
    }
}
