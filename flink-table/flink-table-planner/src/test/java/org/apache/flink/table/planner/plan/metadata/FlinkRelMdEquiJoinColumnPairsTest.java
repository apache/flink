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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRelMdEquiJoinColumnPairs}. */
class FlinkRelMdEquiJoinColumnPairsTest extends FlinkRelMdHandlerTestBase {

    // TableSourceTable1, TableSourceTable2 and TableSourceTable3 all have the columns (a, b, c, d).

    @Test
    void testGetEquiJoinColumnPairsOnTableScan() {
        RelNode scan = relBuilder().scan("TableSourceTable1").build();
        assertThat(mq().getEquiJoinColumnPairs(scan)).isEmpty();
    }

    @Test
    void testGetEquiJoinColumnPairsOnInnerJoin() {
        // t1.a = t2.a AND t1.b = t2.b -> (0, 4), (1, 5)
        RelNode join =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.INNER, equalsCondition(0, 0), equalsCondition(1, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(join))
                .containsExactlyInAnyOrder(inner(0, 4), inner(1, 5));
    }

    @Test
    void testGetEquiJoinColumnPairsOnNonEquiCondition() {
        // only the equality of t1.a = t2.a AND t1.b > t2.b is a pair
        RelNode join =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(
                                JoinRelType.INNER,
                                equalsCondition(0, 0),
                                relBuilder()
                                        .greaterThan(
                                                relBuilder().field(2, 0, 1),
                                                relBuilder().field(2, 1, 1)))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(join)).containsExactlyInAnyOrder(inner(0, 4));
    }

    @Test
    void testGetEquiJoinColumnPairsOnOuterJoin() {
        // the pair is reported for outer joins too, marked as such: t2.a is null in the rows the
        // left join pads with nulls
        RelNode leftJoin =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.LEFT, equalsCondition(0, 0))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(leftJoin)).containsExactlyInAnyOrder(outer(0, 4));

        // in a full join, either column is null in the rows padded on its side
        RelNode fullJoin =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.FULL, equalsCondition(0, 0))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(fullJoin)).containsExactlyInAnyOrder(outer(0, 4));
    }

    @Test
    void testGetEquiJoinColumnPairsOnInnerJoinBelowOuterJoin() {
        // (t1 JOIN t2 ON t1.a = t2.a) LEFT JOIN t3 ON t1.b = t3.b: the inner join's (0, 4) stays
        // equal or both null in every row, the left join's own (1, 9) does not
        RelNode join =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.INNER, equalsCondition(0, 0))
                        .scan("TableSourceTable3")
                        .join(JoinRelType.LEFT, equalsCondition(1, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(join))
                .containsExactlyInAnyOrder(outer(1, 9), inner(0, 4));
    }

    @Test
    void testGetEquiJoinColumnPairsOnChainedJoins() {
        // (t1 JOIN t2 ON t1.a = t2.a) JOIN t3 ON t2.b = t3.b -> (5, 9) and the left's (0, 4)
        RelNode leftDeep =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.INNER, equalsCondition(0, 0))
                        .scan("TableSourceTable3")
                        .join(JoinRelType.INNER, equalsCondition(5, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(leftDeep))
                .containsExactlyInAnyOrder(inner(5, 9), inner(0, 4));

        // t1 JOIN (t2 JOIN t3 ON t2.a = t3.a) ON t1.b = t2.b -> (1, 5) and the right's (0, 4)
        // shifted to (4, 8)
        RelNode rightDeep =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .scan("TableSourceTable3")
                        .join(JoinRelType.INNER, equalsCondition(0, 0))
                        .join(JoinRelType.INNER, equalsCondition(1, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(rightDeep))
                .containsExactlyInAnyOrder(inner(1, 5), inner(4, 8));
    }

    @Test
    void testGetEquiJoinColumnPairsOnSemiAndAntiJoin() {
        // semi and anti joins output the left side only, so only the left's own pair (0, 4)
        // remains
        RelNode semiJoin =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.INNER, equalsCondition(0, 0))
                        .scan("TableSourceTable3")
                        .semiJoin(equalsCondition(1, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(semiJoin)).containsExactlyInAnyOrder(inner(0, 4));

        RelNode antiJoin =
                relBuilder()
                        .scan("TableSourceTable1")
                        .scan("TableSourceTable2")
                        .join(JoinRelType.INNER, equalsCondition(0, 0))
                        .scan("TableSourceTable3")
                        .antiJoin(equalsCondition(1, 1))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(antiJoin)).containsExactlyInAnyOrder(inner(0, 4));
    }

    @Test
    void testGetEquiJoinColumnPairsOnProject() {
        // project [t2.a, t1.a, t1.b, t1.a] over pairs (0, 4), (1, 5): t1.a is passed through at
        // 1 and 3, t2.a at 0, and t2.b is dropped -> (1, 0), (3, 0)
        RelNode project =
                relBuilder()
                        .push(innerJoinOnAAndB())
                        .project(
                                relBuilder().field(4),
                                relBuilder().field(0),
                                relBuilder().field(1),
                                relBuilder().field(0))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(project))
                .containsExactlyInAnyOrder(inner(1, 0), inner(3, 0));
    }

    @Test
    void testGetEquiJoinColumnPairsOnCalc() {
        // calc [t1.b, t2.b] over pairs (0, 4), (1, 5) -> (0, 1)
        RelNode join = innerJoinOnAAndB();
        relBuilder().push(join);
        List<RexNode> projects = List.of(relBuilder().field(1), relBuilder().field(5));
        RelDataType outputRowType = relBuilder().project(projects).build().getRowType();
        Calc calc = createLogicalCalc(join, outputRowType, projects, List.of());
        assertThat(mq().getEquiJoinColumnPairs(calc)).containsExactlyInAnyOrder(inner(0, 1));
    }

    @Test
    void testGetEquiJoinColumnPairsOnExchange() {
        RelNode join = innerJoinOnAAndB();
        FlinkRelDistribution hash = FlinkRelDistribution.hash(new int[] {0}, true);
        RelNode exchange =
                new StreamPhysicalExchange(
                        cluster(), streamPhysicalTraits().replace(hash), join, hash);
        assertThat(mq().getEquiJoinColumnPairs(exchange))
                .containsExactlyInAnyOrder(inner(0, 4), inner(1, 5));
    }

    @Test
    void testGetEquiJoinColumnPairsOnUnsupportedNode() {
        // an aggregate is not looked through
        RelNode aggregate =
                relBuilder()
                        .push(innerJoinOnAAndB())
                        .aggregate(relBuilder().groupKey(0, 4), relBuilder().countStar("cnt"))
                        .build();
        assertThat(mq().getEquiJoinColumnPairs(aggregate)).isEmpty();
    }

    /** t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b, with the pairs (0, 4), (1, 5). */
    private RelNode innerJoinOnAAndB() {
        return relBuilder()
                .scan("TableSourceTable1")
                .scan("TableSourceTable2")
                .join(JoinRelType.INNER, equalsCondition(0, 0), equalsCondition(1, 1))
                .build();
    }

    private static EquiJoinColumnPair inner(int source, int target) {
        return EquiJoinColumnPair.of(source, target, false);
    }

    private static EquiJoinColumnPair outer(int source, int target) {
        return EquiJoinColumnPair.of(source, target, true);
    }

    /** {@code left.leftField = right.rightField} over the two inputs on top of the stack. */
    private RexNode equalsCondition(int leftField, int rightField) {
        return relBuilder()
                .equals(relBuilder().field(2, 0, leftField), relBuilder().field(2, 1, rightField));
    }
}
