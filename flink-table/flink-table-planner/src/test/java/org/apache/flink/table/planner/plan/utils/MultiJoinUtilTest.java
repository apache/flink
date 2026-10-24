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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.operators.join.FlinkJoinType.INNER;
import static org.apache.flink.table.runtime.operators.join.FlinkJoinType.LEFT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MultiJoinUtil}. */
class MultiJoinUtilTest {

    private static final FlinkTypeFactory TYPE_FACTORY =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final RelDataType INT =
            TYPE_FACTORY.createTypeWithNullability(
                    TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), true);

    // Three inputs with two fields each: input 0 is $0, $1, input 1 is $2, $3, input 2 is $4, $5.
    private static final List<RowType> INPUT_TYPES =
            List.of(twoFieldRow(), twoFieldRow(), twoFieldRow());

    @Test
    void testInnerJoinFiltersBothSides() {
        assertThat(
                        nullFilterFields(
                                List.of(INNER, INNER, LEFT), List.of(equals(0, 2), equals(2, 4))))
                .isEqualTo(new int[][] {{0}, {0}, {0}});
    }

    @Test
    void testLeftJoinFiltersOnlyItsOwnInput() {
        // The preceding inputs of a left join are preserved, also when the condition compares
        // two of them.
        assertThat(
                        nullFilterFields(
                                List.of(INNER, LEFT, LEFT),
                                List.of(equals(0, 2), and(equals(0, 4), equals(1, 3)))))
                .isEqualTo(new int[][] {{}, {0}, {0}});
    }

    @Test
    void testLaterInnerJoinFiltersPrecedingInputs() {
        assertThat(
                        nullFilterFields(
                                List.of(INNER, LEFT, INNER),
                                List.of(equals(0, 2), and(equals(0, 4), equals(3, 5)))))
                .isEqualTo(new int[][] {{0}, {0, 1}, {0, 1}});
    }

    @Test
    void testIgnoresConditionsThatCanMatchNull() {
        final RexNode isNotDistinctFrom =
                REX_BUILDER.makeCall(
                        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, inputRef(0), inputRef(2));
        final RexNode or = REX_BUILDER.makeCall(SqlStdOperatorTable.OR, equals(1, 3), equals(0, 2));
        assertThat(nullFilterFields(List.of(INNER, INNER, INNER), List.of(isNotDistinctFrom, or)))
                .isEqualTo(new int[][] {{}, {}, {}});
    }

    @Test
    void testConditionFieldsIncludeNonEquiConditionsAndExcludeKeys() {
        final RexNode greaterThan =
                REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, inputRef(5), inputRef(1));
        // input 1 has the key (0), input 2 has the keys (0, 1) and (0)
        assertThat(
                        MultiJoinUtil.createConditionFields(
                                INPUT_TYPES,
                                Arrays.asList(null, equals(0, 2), and(equals(2, 4), greaterThan)),
                                List.of(
                                        List.of(),
                                        List.of(new int[] {0}),
                                        List.of(new int[] {0, 1}, new int[] {0}))))
                .isEqualTo(new int[][] {{0, 1}, {}, {1}});
    }

    private static int[][] nullFilterFields(
            final List<FlinkJoinType> joinTypes, final List<RexNode> conditions) {
        // The condition of input 0 is TRUE
        final List<RexNode> joinConditions =
                Arrays.asList(REX_BUILDER.makeLiteral(true), conditions.get(0), conditions.get(1));
        return MultiJoinUtil.createNullFilterFields(INPUT_TYPES, joinConditions, joinTypes);
    }

    private static RexNode equals(final int left, final int right) {
        return REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, inputRef(left), inputRef(right));
    }

    private static RexNode and(final RexNode left, final RexNode right) {
        return REX_BUILDER.makeCall(SqlStdOperatorTable.AND, left, right);
    }

    private static RexNode inputRef(final int index) {
        return REX_BUILDER.makeInputRef(INT, index);
    }

    private static RowType twoFieldRow() {
        return RowType.of(new LogicalType[] {new IntType(), new IntType()});
    }
}
