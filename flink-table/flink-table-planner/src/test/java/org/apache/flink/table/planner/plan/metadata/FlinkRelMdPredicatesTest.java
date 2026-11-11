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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRelMdPredicates}. */
class FlinkRelMdPredicatesTest extends FlinkRelMdHandlerTestBase {

    private static final int NAME = 1;
    private static final int SCORE = 2;
    private static final int AGE = 3;
    private static final int SEX = 5;
    private static final int CLASS = 6;

    @Test
    void testGetPredicatesOnStreamCalc() {
        // name and sex are projected away, and so is their predicate
        final RelNode calc =
                createStreamCalc(
                        studentStreamScan(),
                        List.of(CLASS, AGE),
                        and(equals(AGE, CLASS), equals(NAME, SEX)));

        assertThat(mq().getPulledUpPredicates(calc).pulledUpPredicates).hasToString("[=($1, $0)]");
    }

    @Test
    void testGetPredicatesOnStreamCalcFromInput() {
        final RelNode input =
                createStreamCalc(
                        studentStreamScan(), List.of(0, 1, 2, 3, 4, 5, 6), equals(AGE, CLASS));
        final RelNode calc = createStreamCalc(input, List.of(CLASS, AGE), null);

        assertThat(mq().getPulledUpPredicates(calc).pulledUpPredicates).hasToString("[=($1, $0)]");
    }

    @Test
    void testGetPredicatesOnStreamCalcWithNonDeterministicCondition() {
        final RexNode nonDeterministic =
                rexBuilder()
                        .makeCall(
                                SqlStdOperatorTable.GREATER_THAN,
                                inputRef(SCORE),
                                rexBuilder().makeCall(SqlStdOperatorTable.RAND));
        final RelNode calc =
                createStreamCalc(
                        studentStreamScan(),
                        List.of(AGE, CLASS, SCORE),
                        and(equals(AGE, CLASS), nonDeterministic));

        assertThat(mq().getPulledUpPredicates(calc).pulledUpPredicates).hasToString("[=($0, $1)]");
    }

    @Test
    void testGetPredicatesOnLogicalCalc() {
        // only StreamPhysicalCalc is handled, so logical rules don't see the condition
        final RelNode input = studentFlinkLogicalScan();
        final RelNode calc =
                createLogicalCalc(
                        input,
                        input.getRowType(),
                        rexBuilder().identityProjects(input.getRowType()),
                        List.of(equals(AGE, CLASS)));

        assertThat(mq().getPulledUpPredicates(calc).pulledUpPredicates).isEmpty();
    }

    private RelNode createStreamCalc(
            RelNode input, List<Integer> projects, @Nullable RexNode condition) {
        final RexProgramBuilder builder = new RexProgramBuilder(input.getRowType(), rexBuilder());
        for (int project : projects) {
            builder.addProject(project, input.getRowType().getFieldNames().get(project));
        }
        if (condition != null) {
            builder.addCondition(condition);
        }
        final RexProgram program = builder.getProgram();
        return new StreamPhysicalCalc(
                cluster(), streamPhysicalTraits(), input, program, program.getOutputRowType());
    }

    private RexNode equals(int left, int right) {
        return rexBuilder().makeCall(SqlStdOperatorTable.EQUALS, inputRef(left), inputRef(right));
    }

    private RexNode and(RexNode left, RexNode right) {
        return rexBuilder().makeCall(SqlStdOperatorTable.AND, left, right);
    }

    private RexNode inputRef(int index) {
        return rexBuilder().makeInputRef(studentStreamScan(), index);
    }
}
