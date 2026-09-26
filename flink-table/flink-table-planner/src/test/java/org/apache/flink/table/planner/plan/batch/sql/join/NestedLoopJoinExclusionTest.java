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

package org.apache.flink.table.planner.plan.batch.sql.join;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Plan tests for excluding {@code NestedLoopJoin} from the CBO's candidate set for equi-joins,
 * added by {@link
 * org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalJoinRuleBase#checkNestLoopJoin}.
 *
 * <p>For an equi-join, nested-loop join must never be picked over an equi-capable strategy, even
 * when the cost-based cardinality estimate makes it look cheap.
 */
public class NestedLoopJoinExclusionTest extends TableTestBase {

    private BatchTableTestUtil util;

    private static final TypeInformation<?> INT_INFO = BasicTypeInfo.INT_TYPE_INFO;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
    }

    @Test
    void testEquiJoinWithSkewedCardinalityDoesNotUseNestedLoopJoin() {
        // Left side's estimated row count is tiny relative to the right side.
        // NestedLoopJoin's L*R cost (~2M) looks cheaper than HashJoin's 8*(L+R) cost (~8M),
        // even though the join condition is a plain equality.
        util.addTableSource(
                "TinySide",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"a", "b"},
                FlinkStatistic.builder().tableStats(new TableStats(2L)).build());
        util.addTableSource(
                "HugeSide",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"d", "e"},
                FlinkStatistic.builder().tableStats(new TableStats(1_000_000L)).build());

        util.verifyRelPlan("SELECT a, d FROM TinySide, HugeSide WHERE a = d");
    }

    @Test
    void testEquiJoinOnComplexKeyStillProducesValidPlan() {
        // An equi-join on a complex (ARRAY) key still yields a valid plan via an
        // equi-capable strategy rather than being stranded with no candidate at all.
        TypeInformation<?> arrayInfo = PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO;
        util.addTableSource(
                "ArrLeft",
                new TypeInformation<?>[] {INT_INFO, arrayInfo},
                new String[] {"a", "r"},
                FlinkStatistic.UNKNOWN());
        util.addTableSource(
                "ArrRight",
                new TypeInformation<?>[] {INT_INFO, arrayInfo},
                new String[] {"d", "s"},
                FlinkStatistic.UNKNOWN());

        util.verifyRelPlan("SELECT a, d FROM ArrLeft, ArrRight WHERE r = s");
    }

    @Test
    void testEquiJoinFallsBackToNestedLoopJoinWhenOnlyInapplicableBroadcastIsEnabled() {
        // ShuffleHashJoin/SortMergeJoin disabled; BroadcastHashJoin stays enabled but cannot
        // apply since both sides have unknown size stats. NestedLoopJoin must remain available
        // as a fallback here, otherwise the equi-join is left with no valid physical strategy.
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin, SortMergeJoin");
        util.addTableSource(
                "LeftUnknown",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"a", "b"},
                FlinkStatistic.UNKNOWN());
        util.addTableSource(
                "RightUnknown",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"d", "e"},
                FlinkStatistic.UNKNOWN());

        util.verifyRelPlan("SELECT a, d FROM LeftUnknown, RightUnknown WHERE a = d");
    }

    @Test
    void testEquiJoinStillAvoidsNestedLoopJoinWhenBroadcastIsApplicable() {
        // Same skewed-cardinality setup as
        // testEquiJoinWithSkewedCardinalityDoesNotUseNestedLoopJoin, but with
        // ShuffleHashJoin/SortMergeJoin disabled so only BroadcastHashJoin is applicable
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin, SortMergeJoin");
        util.addTableSource(
                "TinySide",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"a", "b"},
                FlinkStatistic.builder().tableStats(new TableStats(2L)).build());
        util.addTableSource(
                "HugeSide",
                new TypeInformation<?>[] {INT_INFO, INT_INFO},
                new String[] {"d", "e"},
                FlinkStatistic.builder().tableStats(new TableStats(1_000_000L)).build());

        util.verifyRelPlan("SELECT a, d FROM TinySide, HugeSide WHERE a = d");
    }
}
