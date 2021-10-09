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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.utils.RexNodeExtractor.extractRefInputFields;

/**
 * Pushes primary key filters through a {@link StreamPhysicalChangelogNormalize ChangelogNormalize}
 * operator to reduce its state size.
 *
 * <p>This rule looks for Calc → ChangelogNormalize where the {@link StreamPhysicalCalc Calc}
 * contains a filter condition. The condition is transformed into CNF and then each conjunction is
 * tested for whether it affects only primary key columns. If such conditions exist, they are moved
 * into a new, separate Calc and pushed through the ChangelogNormalize operator. ChangelogNormalize
 * keeps state for every unique key it encounters, thus pushing filters on the primary key in front
 * of it helps reduce the size of its state.
 *
 * <p>Note that pushing primary key filters is safe to do, but pushing any other filters can lead to
 * incorrect results.
 */
@Internal
public class PushFilterPastChangelogNormalizeRule
        extends RelRule<PushFilterPastChangelogNormalizeRule.Config> {

    public static final RelOptRule INSTANCE = Config.EMPTY.as(Config.class).onMatch().toRule();

    public PushFilterPastChangelogNormalizeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalCalc calc = call.rel(0);
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);

        final RexProgram program = calc.getProgram();
        final RexNode condition =
                RexUtil.toCnf(
                        call.builder().getRexBuilder(),
                        program.expandLocalRef(program.getCondition()));

        final Set<Integer> primaryKeyIndices =
                IntStream.of(changelogNormalize.uniqueKeys()).boxed().collect(Collectors.toSet());

        // Determine which filters can be pushed (= involve only primary key columns)
        final List<RexNode> primaryKeyPredicates = new ArrayList<>();
        final List<RexNode> otherPredicates = new ArrayList<>();
        partitionPrimaryKeyPredicates(
                RelOptUtil.conjunctions(condition),
                primaryKeyIndices,
                primaryKeyPredicates,
                otherPredicates);

        // Construct a new ChangelogNormalize which has primary key filters pushed into it
        final StreamPhysicalChangelogNormalize newChangelogNormalize =
                pushFiltersThroughChangelogNormalize(call, primaryKeyPredicates);

        // Retain only filters which haven't been pushed
        transformWithRemainingPredicates(call, newChangelogNormalize, otherPredicates);
    }

    /**
     * Separates the given {@param predicates} into filters which affect only the primary key and
     * anything else.
     */
    private void partitionPrimaryKeyPredicates(
            List<RexNode> predicates,
            Set<Integer> primaryKeyIndices,
            List<RexNode> primaryKeyPredicates,
            List<RexNode> remainingPredicates) {
        for (RexNode predicate : predicates) {
            int[] inputRefs = extractRefInputFields(Collections.singletonList(predicate));
            if (Arrays.stream(inputRefs).allMatch(primaryKeyIndices::contains)) {
                primaryKeyPredicates.add(predicate);
            } else {
                remainingPredicates.add(predicate);
            }
        }
    }

    /** Pushes {@param primaryKeyPredicates} into the {@link StreamPhysicalChangelogNormalize}. */
    private StreamPhysicalChangelogNormalize pushFiltersThroughChangelogNormalize(
            RelOptRuleCall call, List<RexNode> primaryKeyPredicates) {
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
        final StreamPhysicalExchange exchange = call.rel(2);

        if (primaryKeyPredicates.isEmpty()) {
            // There are no filters which can be pushed, so just return the existing node.
            return changelogNormalize;
        }

        final StreamPhysicalCalc pushedFiltersCalc =
                projectIdentityWithConditions(
                        call.builder(), exchange.getInput(), primaryKeyPredicates);

        final StreamPhysicalExchange newExchange =
                (StreamPhysicalExchange)
                        exchange.copy(
                                exchange.getTraitSet(),
                                Collections.singletonList(pushedFiltersCalc));

        return (StreamPhysicalChangelogNormalize)
                changelogNormalize.copy(
                        changelogNormalize.getTraitSet(), Collections.singletonList(newExchange));
    }

    /**
     * Returns a {@link StreamPhysicalCalc} with the given {@param conditions} and an identity
     * projection.
     */
    private StreamPhysicalCalc projectIdentityWithConditions(
            RelBuilder relBuilder, RelNode newInput, List<RexNode> conditions) {

        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(newInput.getRowType(), relBuilder.getRexBuilder());
        programBuilder.addIdentity();

        final RexNode condition = relBuilder.and(conditions);
        if (!condition.isAlwaysTrue()) {
            programBuilder.addCondition(condition);
        }

        final RexProgram newProgram = programBuilder.getProgram();
        return new StreamPhysicalCalc(
                newInput.getCluster(),
                newInput.getTraitSet(),
                newInput,
                newProgram,
                newProgram.getOutputRowType());
    }

    /**
     * Returns a {@link StreamPhysicalCalc} which is a copy of {@param calc}, but with the
     * projections applied from {@param projectFromCalc}.
     */
    private StreamPhysicalCalc projectWith(
            RelBuilder relBuilder, StreamPhysicalCalc projectFromCalc, StreamPhysicalCalc calc) {
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(calc.getRowType(), relBuilder.getRexBuilder());
        if (calc.getProgram().getCondition() != null) {
            programBuilder.addCondition(
                    calc.getProgram().expandLocalRef(calc.getProgram().getCondition()));
        }

        for (Pair<RexLocalRef, String> projectRef :
                projectFromCalc.getProgram().getNamedProjects()) {
            final RexNode project = projectFromCalc.getProgram().expandLocalRef(projectRef.left);
            programBuilder.addProject(project, projectRef.right);
        }

        final RexProgram newProgram = programBuilder.getProgram();
        return (StreamPhysicalCalc) calc.copy(calc.getTraitSet(), calc.getInput(), newProgram);
    }

    /**
     * Transforms the {@link RelOptRuleCall} to use {@param changelogNormalize} as the new input to
     * a {@link StreamPhysicalCalc} which uses {@param predicates} for the condition.
     */
    private void transformWithRemainingPredicates(
            RelOptRuleCall call,
            StreamPhysicalChangelogNormalize changelogNormalize,
            List<RexNode> predicates) {
        final StreamPhysicalCalc calc = call.rel(0);
        final RelBuilder relBuilder = call.builder();

        final StreamPhysicalCalc newCalc =
                projectIdentityWithConditions(relBuilder, changelogNormalize, predicates);
        final StreamPhysicalCalc newProjectedCalc = projectWith(relBuilder, calc, newCalc);

        if (newProjectedCalc.getProgram().isTrivial()) {
            call.transformTo(changelogNormalize);
        } else {
            call.transformTo(newProjectedCalc);
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link PushFilterPastChangelogNormalizeRule}. */
    public interface Config extends RelRule.Config {

        @Override
        default RelOptRule toRule() {
            return new PushFilterPastChangelogNormalizeRule(this);
        }

        default Config onMatch() {
            final RelRule.OperandTransform exchangeTransform =
                    operandBuilder ->
                            operandBuilder.operand(StreamPhysicalExchange.class).anyInputs();

            final RelRule.OperandTransform changelogNormalizeTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalChangelogNormalize.class)
                                    .oneInput(exchangeTransform);

            final RelRule.OperandTransform calcTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(StreamPhysicalCalc.class)
                                    .predicate(calc -> calc.getProgram().getCondition() != null)
                                    .oneInput(changelogNormalizeTransform);

            return withOperandSupplier(calcTransform).as(Config.class);
        }
    }
}
