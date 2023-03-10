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
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.planner.plan.utils.RexNodeExtractor.extractRefInputFields;

/**
 * Pushes primary key filters and used fields project through a {@link
 * StreamPhysicalChangelogNormalize ChangelogNormalize} operator to reduce its state size.
 *
 * <p>This rule looks for Calc → ChangelogNormalize where the {@link StreamPhysicalCalc Calc}
 * contains a filter condition or a projection. The condition is transformed into CNF and then each
 * conjunction is tested for whether it affects only primary key columns. If such conditions or
 * projection exist, they are moved into a new, separate Calc and pushed through the
 * ChangelogNormalize operator. ChangelogNormalize keeps state for every unique key it encounters,
 * thus pushing filters on the primary key and projection on values in front of it helps reduce the
 * size of its state.
 *
 * <p>Note that pushing primary key filters is safe to do, but pushing any other filters can lead to
 * incorrect results.
 */
@Internal
@Value.Enclosing
public class PushCalcPastChangelogNormalizeRule
        extends RelRule<PushCalcPastChangelogNormalizeRule.Config> {

    public static final RelOptRule INSTANCE =
            new PushCalcPastChangelogNormalizeRule(Config.DEFAULT);

    public PushCalcPastChangelogNormalizeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalCalc calc = call.rel(0);
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
        final Set<Integer> primaryKeyIndices =
                IntStream.of(changelogNormalize.uniqueKeys()).boxed().collect(Collectors.toSet());

        // Determine which filters can be pushed (= involve only primary key columns)
        final List<RexNode> primaryKeyPredicates = new ArrayList<>();
        final List<RexNode> otherPredicates = new ArrayList<>();
        final RexProgram program = calc.getProgram();
        if (program.getCondition() != null) {
            final RexNode condition =
                    RexUtil.toCnf(
                            call.builder().getRexBuilder(),
                            program.expandLocalRef(program.getCondition()));
            partitionPrimaryKeyPredicates(
                    RelOptUtil.conjunctions(condition),
                    primaryKeyIndices,
                    primaryKeyPredicates,
                    otherPredicates);
        }

        // used input field indices
        int[] usedInputFields = extractUsedInputFields(calc, primaryKeyIndices);

        // Construct a new ChangelogNormalize which has used fields project
        // and primary key filters pushed into it
        final StreamPhysicalChangelogNormalize newChangelogNormalize =
                pushCalcThroughChangelogNormalize(call, primaryKeyPredicates, usedInputFields);

        // Retain only filters which haven't been pushed
        transformWithRemainingPredicates(
                call, newChangelogNormalize, otherPredicates, usedInputFields);
    }

    /** Extracts input fields which are used in the Calc node and the ChangelogNormalize node. */
    private int[] extractUsedInputFields(StreamPhysicalCalc calc, Set<Integer> primaryKeyIndices) {
        RexProgram program = calc.getProgram();
        List<RexNode> projectsAndCondition =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        if (program.getCondition() != null) {
            projectsAndCondition.add(program.expandLocalRef(program.getCondition()));
        }
        Set<Integer> projectedFields =
                Arrays.stream(extractRefInputFields(projectsAndCondition))
                        .boxed()
                        .collect(Collectors.toSet());
        // we can't project primary keys
        projectedFields.addAll(primaryKeyIndices);
        return projectedFields.stream().sorted().mapToInt(Integer::intValue).toArray();
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

    /**
     * Pushes {@param primaryKeyPredicates} and used fields project into the {@link
     * StreamPhysicalChangelogNormalize}.
     */
    private StreamPhysicalChangelogNormalize pushCalcThroughChangelogNormalize(
            RelOptRuleCall call, List<RexNode> primaryKeyPredicates, int[] usedInputFields) {
        final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
        final StreamPhysicalExchange exchange = call.rel(2);
        final Set<Integer> primaryKeyIndices =
                IntStream.of(changelogNormalize.uniqueKeys()).boxed().collect(Collectors.toSet());

        if (primaryKeyPredicates.isEmpty()
                && usedInputFields.length == changelogNormalize.getRowType().getFieldCount()) {
            // There are no filters and no project which can be pushed, so just return the existing
            // node.
            return changelogNormalize;
        }

        final StreamPhysicalCalc pushedCalc =
                projectUsedFieldsWithConditions(
                        call.builder(), exchange.getInput(), primaryKeyPredicates, usedInputFields);

        // build input field reference from old field index to new field index
        final Map<Integer, Integer> inputRefMapping = buildFieldsMapping(usedInputFields);
        final List<Integer> newPrimaryKeyIndices =
                primaryKeyIndices.stream().map(inputRefMapping::get).collect(Collectors.toList());

        final FlinkRelDistribution newDistribution =
                FlinkRelDistribution.hash(newPrimaryKeyIndices, true);
        final RelTraitSet newTraitSet = exchange.getTraitSet().replace(newDistribution);
        final StreamPhysicalExchange newExchange =
                exchange.copy(newTraitSet, pushedCalc, newDistribution);

        return (StreamPhysicalChangelogNormalize)
                changelogNormalize.copy(
                        changelogNormalize.getTraitSet(),
                        newExchange,
                        newPrimaryKeyIndices.stream().mapToInt(Integer::intValue).toArray());
    }

    /**
     * Builds a new {@link StreamPhysicalCalc} on the input node with the given {@param conditions}
     * and a used fields projection.
     */
    private StreamPhysicalCalc projectUsedFieldsWithConditions(
            RelBuilder relBuilder, RelNode input, List<RexNode> conditions, int[] usedFields) {
        final RelDataType inputRowType = input.getRowType();
        final List<String> inputFieldNames = inputRowType.getFieldNames();
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, relBuilder.getRexBuilder());

        // add project
        for (int fieldIndex : usedFields) {
            programBuilder.addProject(
                    programBuilder.makeInputRef(fieldIndex), inputFieldNames.get(fieldIndex));
        }

        // add conditions
        final RexNode condition = relBuilder.and(conditions);
        if (!condition.isAlwaysTrue()) {
            programBuilder.addCondition(condition);
        }

        final RexProgram newProgram = programBuilder.getProgram();
        return new StreamPhysicalCalc(
                input.getCluster(),
                input.getTraitSet(),
                input,
                newProgram,
                newProgram.getOutputRowType());
    }

    /**
     * Transforms the {@link RelOptRuleCall} to use {@param changelogNormalize} as the new input to
     * a {@link StreamPhysicalCalc} which uses {@param predicates} for the condition.
     */
    private void transformWithRemainingPredicates(
            RelOptRuleCall call,
            StreamPhysicalChangelogNormalize changelogNormalize,
            List<RexNode> predicates,
            int[] usedInputFields) {
        final StreamPhysicalCalc calc = call.rel(0);
        final RelBuilder relBuilder = call.builder();
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(changelogNormalize.getRowType(), relBuilder.getRexBuilder());

        final Map<Integer, Integer> inputRefMapping = buildFieldsMapping(usedInputFields);

        // add projects
        for (Pair<RexLocalRef, String> ref : calc.getProgram().getNamedProjects()) {
            RexNode shiftedProject =
                    adjustInputRef(calc.getProgram().expandLocalRef(ref.left), inputRefMapping);
            programBuilder.addProject(shiftedProject, ref.right);
        }

        // add conditions
        final List<RexNode> shiftedPredicates =
                predicates.stream()
                        .map(p -> adjustInputRef(p, inputRefMapping))
                        .collect(Collectors.toList());
        final RexNode condition = relBuilder.and(shiftedPredicates);
        if (!condition.isAlwaysTrue()) {
            programBuilder.addCondition(condition);
        }

        final RexProgram newProgram = programBuilder.getProgram();
        if (newProgram.isTrivial()) {
            call.transformTo(changelogNormalize);
        } else {
            final StreamPhysicalCalc newProjectedCalc =
                    new StreamPhysicalCalc(
                            changelogNormalize.getCluster(),
                            changelogNormalize.getTraitSet(),
                            changelogNormalize,
                            newProgram,
                            newProgram.getOutputRowType());
            call.transformTo(newProjectedCalc);
        }
    }

    /** Adjust the {@param expr} field indices according to the field index {@param mapping}. */
    private RexNode adjustInputRef(RexNode expr, Map<Integer, Integer> mapping) {
        return expr.accept(
                new RexShuttle() {

                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        Integer newIndex = mapping.get(inputRef.getIndex());
                        return new RexInputRef(newIndex, inputRef.getType());
                    }
                });
    }

    /** Build field reference mapping from old field index to new field index after projection. */
    private Map<Integer, Integer> buildFieldsMapping(int[] projectedInputRefs) {
        final Map<Integer, Integer> fieldsOldToNewIndexMapping = new HashMap<>();
        for (int i = 0; i < projectedInputRefs.length; i++) {
            fieldsOldToNewIndexMapping.put(projectedInputRefs[i], i);
        }
        return fieldsOldToNewIndexMapping;
    }

    // ---------------------------------------------------------------------------------------------

    /** Configuration for {@link PushCalcPastChangelogNormalizeRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutablePushCalcPastChangelogNormalizeRule.Config.builder().build().onMatch();

        @Override
        default RelOptRule toRule() {
            return new PushCalcPastChangelogNormalizeRule(this);
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
                                    .oneInput(changelogNormalizeTransform);

            return withOperandSupplier(calcTransform).as(Config.class);
        }
    }
}
