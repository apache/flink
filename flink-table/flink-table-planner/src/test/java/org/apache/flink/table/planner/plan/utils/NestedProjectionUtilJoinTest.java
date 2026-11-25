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

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NestedProjectionUtil#rewriteForJoin} and {@code hasPrunableNestedLeaf}. */
class NestedProjectionUtilJoinTest {

    private final FlinkTypeFactory typeFactory =
            new FlinkTypeFactory(
                    Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    private final RexBuilder rexBuilder = new FlinkRexBuilder(typeFactory);

    // ------------------------------------------------------------------------------------------
    // rewriteForJoin
    // ------------------------------------------------------------------------------------------

    @Test
    void testLeavesAreSplitPerSideInInputOrder() {
        // Structs are referenced out of their column order and two leaves of s1 are used. The
        // unused column k sits between them and passes through whole.
        final RelDataType left =
                row(
                        "id", intType(),
                        "s1", row("a", intType(), "b", intType()),
                        "k", intType(),
                        "s2", row("c", intType(), "d", intType()));
        final RelDataType right = row("id2", intType(), "t", row("e", intType(), "f", intType()));
        final RelDataType join = concat(left, right);

        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(
                                access(join, 3, "d"),
                                access(join, 1, "a"),
                                access(join, 1, "b"),
                                access(join, 5, "f")),
                        equals(ref(join, 0), ref(join, 4)),
                        join,
                        left,
                        right);

        assertThat(strings(result.leftProjects()))
                .containsExactly("$0", "$1.a", "$1.b", "$2", "$3.d");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.f");
        assertThat(strings(result.rewrittenProjects())).containsExactly("$4", "$1", "$2", "$6");
        assertThat(result.rewrittenCondition().toString()).isEqualTo("=($0, $5)");
    }

    @Test
    void testSidesWithSameSchema() {
        // like a self join: Calcite names the columns of the right side `id0` and `nested0`
        final RelDataType side = row("id", intType(), "nested", row("name", stringType()));
        final RelDataType join =
                SqlValidatorUtil.deriveJoinRowType(
                        side, side, JoinRelType.INNER, typeFactory, null, List.of());
        assertThat(join.getFieldNames()).containsExactly("id", "nested", "id0", "nested0");

        final RexNode leftName = access(join, 1, "name");
        final RexNode rightName = access(join, 3, "name");

        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(leftName, rightName),
                        equals(ref(join, 0), ref(join, 2)),
                        join,
                        side,
                        side);

        assertThat(strings(result.leftProjects())).containsExactly("$0", "$1.name");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.name");
        assertThat(strings(result.rewrittenProjects())).containsExactly("$1", "$3");
        assertThat(result.rewrittenCondition().toString()).isEqualTo("=($0, $2)");
    }

    @Test
    void testDeeplyNestedLeaves() {
        final RelDataType left =
                row(
                        "id",
                        intType(),
                        "s",
                        row(
                                "a",
                                row("b", row("c", intType(), "x", intType()), "y", intType()),
                                "z",
                                intType()));
        final RelDataType right = row("id2", intType());
        final RelDataType join = concat(left, right);

        // leaves at depth 3 (s.a.b.c), 2 (s.a.y), and 1 (s.z)
        final RexNode b = rexBuilder.makeFieldAccess(access(join, 1, "a"), "b", true);
        final RexNode c = rexBuilder.makeFieldAccess(b, "c", true);
        final RexNode y = rexBuilder.makeFieldAccess(access(join, 1, "a"), "y", true);
        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(c, access(join, 1, "z"), y),
                        equals(ref(join, 0), ref(join, 2)),
                        join,
                        left,
                        right);

        assertThat(strings(result.leftProjects()))
                .containsExactly("$0", "$1.a.b.c", "$1.a.y", "$1.z");
        assertThat(strings(result.rewrittenProjects())).containsExactly("$1", "$3", "$2");
        assertThat(result.rewrittenCondition().toString()).isEqualTo("=($0, $4)");
    }

    @Test
    void testSideWithoutReferencedColumnsPassesThrough() {
        final RelDataType left = row("s", row("x", intType(), "y", intType()));
        final RelDataType right = row("t", row("p", intType(), "q", intType()), "u", intType());
        final RelDataType join = concat(left, right);

        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(access(join, 0, "x")),
                        rexBuilder.makeLiteral(true),
                        join,
                        left,
                        right);

        // pruning the unused input is left to FlinkProjectJoinTransposeRule
        assertThat(strings(result.leftProjects())).containsExactly("$0.x");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1");
        assertThat(strings(result.rewrittenProjects())).containsExactly("$0");
        assertThat(result.rewrittenCondition().toString()).isEqualTo("true");
    }

    @Test
    void testFieldNamesDifferingOnlyInCase() {
        final RelDataType left = row("id", intType());
        final RelDataType right =
                row(
                        "id2",
                        intType(),
                        "info",
                        row("a", intType(), "A", stringType(), "b", intType()));
        final RelDataType join = concat(left, right);

        // `a` and `A` are different fields, they must not be mixed up
        final RexNode lowerA = rexBuilder.makeFieldAccess(ref(join, 2), 0);
        final RexNode upperA = rexBuilder.makeFieldAccess(ref(join, 2), 1);
        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(upperA, lowerA),
                        equals(ref(join, 0), ref(join, 1)),
                        join,
                        left,
                        right);

        assertThat(strings(result.leftProjects())).containsExactly("$0");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.A", "$1.a");
        assertThat(result.rightProjects().get(1).getType()).isEqualTo(stringType());
        assertThat(result.rightProjects().get(2).getType()).isEqualTo(intType());
        assertThat(strings(result.rewrittenProjects())).containsExactly("$2", "$3");
        assertThat(result.rewrittenProjects().get(0).getType()).isEqualTo(stringType());
        assertThat(result.rewrittenProjects().get(1).getType()).isEqualTo(intType());
    }

    @Test
    void testNotNullLeafOfNullableStructIsNullable() {
        // The leaf is declared NOT NULL, but is nullable when read through a nullable struct.
        final RelDataType left = row("id", intType(), "nested", nullable(row("v", intType())));
        final RelDataType right = row("id2", intType());
        final RelDataType join = concat(left, right);

        final RexNode leaf = access(join, 1, "v");
        assertThat(leaf.getType().isNullable()).isTrue();

        final JoinNestedProjectionResult result =
                rewrite(List.of(leaf), equals(ref(join, 0), ref(join, 2)), join, left, right);

        assertThat(result.leftProjects().get(1).getType().isNullable()).isTrue();
        assertThat(result.rewrittenProjects().get(0).getType()).isEqualTo(leaf.getType());
        assertThat(result.rewrittenProjects().get(0).getType().isNullable()).isTrue();
    }

    @Test
    void testStructUsedWholeAndAccessedDeeper() {
        // `info.addr` is used whole, and `info.addr.zip` reads into it. Both are nullable because
        // `info` is nullable, although `addr` and `zip` are declared NOT NULL.
        final RelDataType addr = row("zip", intType(), "st", stringType());
        final RelDataType left = row("id", intType(), "info", nullable(row("addr", addr)));
        final RelDataType right = row("id2", intType());
        final RelDataType join = concat(left, right);

        final RexNode wholeAddr = access(join, 1, "addr");
        final RexNode zip = rexBuilder.makeFieldAccess(wholeAddr, "zip", true);
        assertThat(zip.getType().isNullable()).isTrue();

        final JoinNestedProjectionResult result =
                rewrite(
                        List.of(wholeAddr, zip),
                        equals(ref(join, 0), ref(join, 2)),
                        join,
                        left,
                        right);

        assertThat(strings(result.leftProjects())).containsExactly("$0", "$1.addr");
        assertThat(strings(result.rewrittenProjects())).containsExactly("$1", "$1.zip");
        assertThat(result.rewrittenProjects().get(0).getType()).isEqualTo(wholeAddr.getType());
        assertThat(result.rewrittenProjects().get(1).getType()).isEqualTo(zip.getType());
        assertThat(result.rewrittenProjects().get(0).getType().isNullable()).isTrue();
        assertThat(result.rewrittenProjects().get(1).getType().isNullable()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("supportedJoinTypes")
    void testNullability(JoinRelType joinType) {
        // `v` and `w` are NOT NULL in NOT NULL structs. A join makes the columns of the side that
        // generates nulls nullable: the projects above the join read them, the condition does not.
        final boolean nullsOnLeft = joinType.generatesNullsOnLeft();
        final boolean nullsOnRight = joinType.generatesNullsOnRight();
        final RelDataType left = row("id", intType(), "s", row("v", intType()));
        final RelDataType right = row("id2", intType(), "t", row("w", intType()));
        final RelDataType plain = concat(left, right);

        final RelDataType join =
                SqlValidatorUtil.deriveJoinRowType(
                        left, right, joinType, typeFactory, null, List.of());

        final RexNode leftOut = rexBuilder.makeFieldAccess(ref(join, 1), "v", true);
        final RexNode rightOut = rexBuilder.makeFieldAccess(ref(join, 3), "w", true);
        assertThat(leftOut.getType().isNullable()).as("left input").isEqualTo(nullsOnLeft);
        assertThat(rightOut.getType().isNullable()).as("right input").isEqualTo(nullsOnRight);
        final RexNode leftCond = rexBuilder.makeFieldAccess(ref(plain, 1), "v", true);
        final RexNode rightCond = rexBuilder.makeFieldAccess(ref(plain, 3), "w", true);
        final RexNode cond = equals(leftCond, rightCond);

        final JoinNestedProjectionResult result =
                NestedProjectionUtil.rewriteForJoin(
                        List.of(leftOut, rightOut),
                        cond,
                        join,
                        left,
                        right,
                        nullsOnLeft,
                        nullsOnRight,
                        rexBuilder);

        // the projections below the join read the plain inputs
        assertThat(strings(result.leftProjects())).containsExactly("$0", "$1.v");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.w");
        assertThat(result.leftProjects().get(1).getType().isNullable())
                .as("left projection")
                .isFalse();
        assertThat(result.rightProjects().get(1).getType().isNullable())
                .as("right projection")
                .isFalse();

        // the condition reads the plain inputs
        final RexCall rewrittenCond = (RexCall) result.rewrittenCondition();
        assertThat(rewrittenCond.toString()).isEqualTo("=($1, $3)");
        assertThat(rewrittenCond.getOperands().get(0).getType())
                .as("left condition operand type")
                .isEqualTo(leftCond.getType());
        assertThat(rewrittenCond.getOperands().get(1).getType())
                .as("right condition operand type")
                .isEqualTo(rightCond.getType());
        assertThat(rewrittenCond.getOperands().get(0).getType().isNullable())
                .as("left condition operand")
                .isFalse();
        assertThat(rewrittenCond.getOperands().get(1).getType().isNullable())
                .as("right condition operand")
                .isFalse();

        // the projects read the join output, like the expressions they replace
        assertThat(strings(result.rewrittenProjects())).containsExactly("$1", "$3");
        assertThat(result.rewrittenProjects().get(0).getType())
                .as("left project")
                .isEqualTo(leftOut.getType());
        assertThat(result.rewrittenProjects().get(1).getType())
                .as("right project")
                .isEqualTo(rightOut.getType());
        assertThat(result.rewrittenProjects().get(0).getType().isNullable())
                .as("left project nullable")
                .isEqualTo(nullsOnLeft);
        assertThat(result.rewrittenProjects().get(1).getType().isNullable())
                .as("right project nullable")
                .isEqualTo(nullsOnRight);
    }

    private static Stream<JoinRelType> supportedJoinTypes() {
        return Stream.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL);
    }

    @Test
    void testAccessesInsideExpressions() {
        final RelDataType left = row("id", intType(), "s", row("a", intType(), "b", intType()));
        final RelDataType right = row("id2", intType(), "t", row("e", intType(), "f", intType()));
        final RelDataType join = concat(left, right);

        final RexNode sum =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS, access(join, 1, "a"), access(join, 3, "f"));
        final RexNode cond =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        equals(ref(join, 0), ref(join, 2)),
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.GREATER_THAN, access(join, 1, "b"), sum));

        final JoinNestedProjectionResult result = rewrite(List.of(sum), cond, join, left, right);

        assertThat(strings(result.leftProjects())).containsExactly("$0", "$1.a", "$1.b");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.f");
        assertThat(strings(result.rewrittenProjects())).containsExactly("+($1, $4)");
        assertThat(result.rewrittenCondition().toString())
                .isEqualTo("AND(=($0, $3), >($2, +($1, $4)))");
    }

    @Test
    void testAccessesAsOperandsOfCastAndScalarFunction() {
        final RelDataType left = row("id", intType(), "s", row("a", intType(), "n", stringType()));
        final RelDataType right = row("id2", intType(), "t", row("e", intType(), "f", intType()));
        final RelDataType join = concat(left, right);

        final RexNode cast =
                rexBuilder.makeCast(
                        typeFactory.createSqlType(SqlTypeName.BIGINT), access(join, 1, "a"));
        final RexNode upper = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, access(join, 1, "n"));
        final RexNode cond =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.GREATER_THAN, access(join, 3, "f"), ref(join, 0));

        final JoinNestedProjectionResult result =
                rewrite(List.of(cast, upper), cond, join, left, right);

        assertThat(strings(result.leftProjects())).containsExactly("$0", "$1.a", "$1.n");
        assertThat(strings(result.rightProjects())).containsExactly("$0", "$1.f");
        assertThat(strings(result.rewrittenProjects()))
                .containsExactly("CAST($1):BIGINT NOT NULL", "UPPER($2)");
        assertThat(result.rewrittenCondition().toString()).isEqualTo(">($4, $0)");
    }

    // ------------------------------------------------------------------------------------------
    // hasPrunableNestedLeaf
    // ------------------------------------------------------------------------------------------

    @Test
    void testPrunableOnlyIfSomeLeafIsUnused() {
        final RelDataType row = row("id", intType(), "s", row("a", intType(), "b", intType()));

        assertThat(prunable(row, access(row, 1, "a"))).isTrue();
        assertThat(prunable(row, access(row, 1, "a"), access(row, 1, "b"))).isFalse();
        assertThat(prunable(row, ref(row, 1), access(row, 1, "a"))).isFalse();
        assertThat(prunable(row, ref(row, 1))).isFalse();
        assertThat(prunable(row, ref(row, 0))).isFalse();
    }

    @Test
    void testPrunableCountsLeavesOfNestedStructs() {
        final RelDataType deep =
                row("s", row("a", row("b", intType(), "c", intType()), "d", intType()));

        final RexNode a = access(deep, 0, "a");
        final RexNode b = rexBuilder.makeFieldAccess(a, "b", true);
        final RexNode c = rexBuilder.makeFieldAccess(a, "c", true);
        final RexNode d = access(deep, 0, "d");

        assertThat(prunable(deep, a, b)).isTrue();
        assertThat(prunable(deep, b, c)).isTrue();
        assertThat(prunable(deep, c, d)).isTrue();
        assertThat(prunable(deep, b, c, d)).isFalse();
        assertThat(prunable(deep, a, d)).isFalse();
    }

    @Test
    void testPrunableReadsFieldsOfCondition() {
        final RelDataType row = row("id", intType(), "s", row("a", intType(), "b", intType()));
        final RexNode condOnB = equals(access(row, 1, "b"), ref(row, 0));
        final RexNode condOnA = equals(access(row, 1, "a"), ref(row, 0));

        // the condition reads the leaf that the projects leave unused, so no leaf is unused
        assertThat(prunable(List.of(access(row, 1, "a")), condOnB, row)).isFalse();
        // the condition reads one leaf of `s`, and the projects read none
        assertThat(prunable(List.of(ref(row, 0)), condOnA, row)).isTrue();
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private JoinNestedProjectionResult rewrite(
            List<RexNode> projects,
            RexNode condition,
            RelDataType join,
            RelDataType left,
            RelDataType right) {
        return NestedProjectionUtil.rewriteForJoin(
                projects, condition, join, left, right, false, false, rexBuilder);
    }

    private boolean prunable(RelDataType rowType, RexNode... projects) {
        return prunable(List.of(projects), rexBuilder.makeLiteral(true), rowType);
    }

    private boolean prunable(List<RexNode> projects, RexNode condition, RelDataType rowType) {
        return NestedProjectionUtil.hasPrunableNestedLeaf(projects, condition, rowType);
    }

    private RelDataType intType() {
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
    }

    private RelDataType stringType() {
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    private RelDataType nullable(RelDataType type) {
        return typeFactory.createTypeWithNullability(type, true);
    }

    /** Creates a row type from alternating field names and types. */
    private RelDataType row(Object... namesAndTypes) {
        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            names.add((String) namesAndTypes[i]);
            types.add((RelDataType) namesAndTypes[i + 1]);
        }
        return typeFactory.createStructType(types, names);
    }

    private RelDataType concat(RelDataType left, RelDataType right) {
        final List<String> names = new ArrayList<>(left.getFieldNames());
        names.addAll(right.getFieldNames());
        final List<RelDataType> types = new ArrayList<>();
        left.getFieldList().forEach(f -> types.add(f.getType()));
        right.getFieldList().forEach(f -> types.add(f.getType()));
        return typeFactory.createStructType(types, names);
    }

    private RexNode ref(RelDataType rowType, int index) {
        return rexBuilder.makeInputRef(rowType.getFieldList().get(index).getType(), index);
    }

    private RexNode access(RelDataType rowType, int index, String field) {
        return rexBuilder.makeFieldAccess(ref(rowType, index), field, true);
    }

    private RexNode equals(RexNode left, RexNode right) {
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
    }

    private static List<String> strings(List<RexNode> nodes) {
        return nodes.stream().map(RexNode::toString).collect(Collectors.toList());
    }
}
