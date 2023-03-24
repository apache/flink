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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.planner.typeutils.RowTypeUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Transpose {@link StreamPhysicalWatermarkAssigner} past into {@link
 * StreamPhysicalChangelogNormalize}.
 */
@Value.Enclosing
public class WatermarkAssignerChangelogNormalizeTransposeRule
        extends RelRule<WatermarkAssignerChangelogNormalizeTransposeRule.Config> {

    public static final RelOptRule WITH_CALC =
            new WatermarkAssignerChangelogNormalizeTransposeRule(
                    Config.DEFAULT
                            .withDescription(
                                    "WatermarkAssignerChangelogNormalizeTransposeRuleWithCalc")
                            .as(Config.class)
                            .withCalc());

    public static final RelOptRule WITHOUT_CALC =
            new WatermarkAssignerChangelogNormalizeTransposeRule(
                    Config.DEFAULT
                            .withDescription(
                                    "WatermarkAssignerChangelogNormalizeTransposeRuleWithoutCalc")
                            .as(Config.class)
                            .withoutCalc());

    public WatermarkAssignerChangelogNormalizeTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalWatermarkAssigner watermark = call.rel(0);
        final RelNode node = call.rel(1);
        RelNode newTree;
        if (node instanceof StreamPhysicalCalc) {
            // with calc
            final StreamPhysicalCalc calc = call.rel(1);
            final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(2);
            final StreamPhysicalExchange exchange = call.rel(3);
            final Mappings.TargetMapping calcMapping = buildMapping(calc.getProgram());
            final RelDistribution exchangeDistribution = exchange.getDistribution();
            final RelDistribution newExchangeDistribution = exchangeDistribution.apply(calcMapping);
            final boolean shuffleKeysAreKeptByCalc =
                    newExchangeDistribution.getType() == exchangeDistribution.getType()
                            && newExchangeDistribution.getKeys().size()
                                    == exchangeDistribution.getKeys().size();
            if (shuffleKeysAreKeptByCalc) {
                // Pushes down WatermarkAssigner/Calc as a whole if shuffle keys of
                // Exchange are all kept by Calc
                newTree =
                        pushDownOriginalWatermarkAndCalc(
                                watermark,
                                calc,
                                changelogNormalize,
                                exchange,
                                newExchangeDistribution);
            } else {
                // 1. Creates a new Calc which contains all shuffle keys
                // 2. Pushes down new WatermarkAssigner/new Calc
                // 3. Adds a top Calc to remove new added shuffle keys in step 1
                newTree =
                        pushDownTransformedWatermarkAndCalc(
                                watermark,
                                calc,
                                changelogNormalize,
                                exchange,
                                exchangeDistribution.getKeys(),
                                calcMapping);
            }
        } else if (node instanceof StreamPhysicalChangelogNormalize) {
            // without calc
            final StreamPhysicalChangelogNormalize changelogNormalize = call.rel(1);
            final StreamPhysicalExchange exchange = call.rel(2);
            newTree =
                    buildTreeInOrder(
                            exchange.getInput(),
                            // Clears distribution on new WatermarkAssigner
                            Tuple2.of(
                                    watermark,
                                    watermark.getTraitSet().plus(FlinkRelDistribution.DEFAULT())),
                            Tuple2.of(exchange, exchange.getTraitSet()),
                            Tuple2.of(changelogNormalize, changelogNormalize.getTraitSet()));
        } else {
            throw new IllegalStateException(
                    this.getClass().getName()
                            + " matches a wrong relation tree: "
                            + RelOptUtil.toString(watermark));
        }
        call.transformTo(newTree);
    }

    private RelNode pushDownOriginalWatermarkAndCalc(
            StreamPhysicalWatermarkAssigner watermark,
            StreamPhysicalCalc calc,
            StreamPhysicalChangelogNormalize changelogNormalize,
            StreamPhysicalExchange exchange,
            RelDistribution newExchangeDistribution) {
        return buildTreeInOrder(
                exchange.getInput(),
                // Clears distribution on new Calc/WatermarkAssigner
                Tuple2.of(calc, calc.getTraitSet().plus(FlinkRelDistribution.DEFAULT())),
                Tuple2.of(watermark, watermark.getTraitSet().plus(FlinkRelDistribution.DEFAULT())),
                // Updates distribution on new Exchange/Normalize based on field
                // mapping of Calc
                Tuple2.of(exchange, exchange.getTraitSet().plus(newExchangeDistribution)),
                Tuple2.of(
                        changelogNormalize,
                        changelogNormalize.getTraitSet().plus(newExchangeDistribution)));
    }

    private RelNode pushDownTransformedWatermarkAndCalc(
            StreamPhysicalWatermarkAssigner watermark,
            StreamPhysicalCalc calc,
            StreamPhysicalChangelogNormalize changelogNormalize,
            StreamPhysicalExchange exchange,
            List<Integer> completeShuffleKeys,
            Mappings.TargetMapping calcMapping) {
        final List<Integer> projectedOutShuffleKeys = new ArrayList<>();
        for (Integer key : completeShuffleKeys) {
            int targetIdx = calcMapping.getTargetOpt(key);
            if (targetIdx < 0) {
                projectedOutShuffleKeys.add(key);
            }
        }
        // Creates a new Program which contains all shuffle keys
        final RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        final RexProgram newPushDownProgram =
                createTransformedProgramWithAllShuffleKeys(
                        calc.getProgram(), projectedOutShuffleKeys, rexBuilder);
        if (newPushDownProgram.isPermutation()) {
            // Pushes down transformed WatermarkAssigner alone if new pushDown program is a
            // permutation of its inputs
            return pushDownTransformedWatermark(
                    watermark, calc, changelogNormalize, exchange, calcMapping, rexBuilder);
        } else {
            // 1. Pushes down transformed WatermarkAssigner and transformed Calc
            // 2. Adds a top Calc to remove new added shuffle keys
            return pushDownTransformedWatermarkAndCalc(
                    newPushDownProgram, watermark, exchange, changelogNormalize, calc);
        }
    }

    private RexProgram createTransformedProgramWithAllShuffleKeys(
            RexProgram program, List<Integer> projectsOutShuffleKeys, RexBuilder rexBuilder) {
        RelDataType oldInputRowType = program.getInputRowType();
        List<String> visitedProjectNames = new ArrayList<>();
        RexProgramBuilder newProgramBuilder = new RexProgramBuilder(oldInputRowType, rexBuilder);
        program.getNamedProjects()
                .forEach(
                        pair -> {
                            newProgramBuilder.addProject(
                                    program.expandLocalRef(pair.left), pair.right);
                            visitedProjectNames.add(pair.right);
                        });
        List<RelDataTypeField> oldFieldList = oldInputRowType.getFieldList();
        for (Integer projectsOutShuffleKey : projectsOutShuffleKeys) {
            RelDataTypeField oldField = oldFieldList.get(projectsOutShuffleKey);
            String oldFieldName = oldField.getName();
            String newProjectName = RowTypeUtils.getUniqueName(oldFieldName, visitedProjectNames);
            newProgramBuilder.addProject(
                    new RexInputRef(projectsOutShuffleKey, oldField.getType()), newProjectName);
            visitedProjectNames.add(newProjectName);
        }
        if (program.getCondition() != null) {
            newProgramBuilder.addCondition(program.expandLocalRef(program.getCondition()));
        }
        return newProgramBuilder.getProgram();
    }

    private RelNode pushDownTransformedWatermarkAndCalc(
            RexProgram newPushDownProgram,
            StreamPhysicalWatermarkAssigner watermark,
            StreamPhysicalExchange exchange,
            StreamPhysicalChangelogNormalize changelogNormalize,
            StreamPhysicalCalc calc) {
        final RelNode pushDownCalc =
                calc.copy(
                        // Clears distribution on new Calc
                        calc.getTraitSet().plus(FlinkRelDistribution.DEFAULT()),
                        exchange.getInput(),
                        newPushDownProgram);
        final Mappings.TargetMapping mappingOfPushDownCalc = buildMapping(newPushDownProgram);
        final RelDistribution newDistribution =
                exchange.getDistribution().apply(mappingOfPushDownCalc);
        final RelNode newChangelogNormalize =
                buildTreeInOrder(
                        pushDownCalc,
                        Tuple2.of(
                                watermark,
                                watermark.getTraitSet().plus(FlinkRelDistribution.DEFAULT())),
                        // updates distribution on new Exchange/Normalize based on field
                        // mapping of Calc
                        Tuple2.of(exchange, exchange.getTraitSet().plus(newDistribution)),
                        Tuple2.of(
                                changelogNormalize,
                                changelogNormalize.getTraitSet().plus(newDistribution)));
        final List<String> newInputFieldNames = newChangelogNormalize.getRowType().getFieldNames();
        final RexProgramBuilder topProgramBuilder =
                new RexProgramBuilder(
                        newChangelogNormalize.getRowType(),
                        changelogNormalize.getCluster().getRexBuilder());
        for (int fieldIdx = 0; fieldIdx < calc.getRowType().getFieldCount(); fieldIdx++) {
            topProgramBuilder.addProject(
                    RexInputRef.of(fieldIdx, newChangelogNormalize.getRowType()),
                    newInputFieldNames.get(fieldIdx));
        }
        final RexProgram topProgram = topProgramBuilder.getProgram();
        return calc.copy(calc.getTraitSet(), newChangelogNormalize, topProgram);
    }

    private RelNode pushDownTransformedWatermark(
            StreamPhysicalWatermarkAssigner watermark,
            StreamPhysicalCalc calc,
            StreamPhysicalChangelogNormalize changelogNormalize,
            StreamPhysicalExchange exchange,
            Mappings.TargetMapping calcMapping,
            RexBuilder rexBuilder) {
        Mappings.TargetMapping inversedMapping = calcMapping.inverse();
        final int newRowTimeFieldIndex =
                inversedMapping.getTargetOpt(watermark.rowtimeFieldIndex());
        // Updates watermark properties after push down before Calc
        // 1. rewrites watermark expression
        // 2. clears distribution
        // 3. updates row time field index
        RexNode newWatermarkExpr = watermark.watermarkExpr();
        if (watermark.watermarkExpr() != null) {
            newWatermarkExpr = RexUtil.apply(inversedMapping, watermark.watermarkExpr());
        }
        final RelNode newWatermark =
                watermark.copy(
                        watermark.getTraitSet().plus(FlinkRelDistribution.DEFAULT()),
                        exchange.getInput(),
                        newRowTimeFieldIndex,
                        newWatermarkExpr);
        final RelNode newChangelogNormalize =
                buildTreeInOrder(
                        newWatermark,
                        Tuple2.of(exchange, exchange.getTraitSet()),
                        Tuple2.of(changelogNormalize, changelogNormalize.getTraitSet()));
        // Rewrites Calc program because the field type of row time
        // field is changed after watermark pushed down
        final RexProgram oldProgram = calc.getProgram();
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(newChangelogNormalize.getRowType(), rexBuilder);
        final Function<RexNode, RexNode> rexShuttle =
                e ->
                        e.accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        if (inputRef.getIndex() == newRowTimeFieldIndex) {
                                            return RexInputRef.of(
                                                    newRowTimeFieldIndex,
                                                    newChangelogNormalize.getRowType());
                                        } else {
                                            return inputRef;
                                        }
                                    }
                                });
        oldProgram
                .getNamedProjects()
                .forEach(
                        pair ->
                                programBuilder.addProject(
                                        rexShuttle.apply(oldProgram.expandLocalRef(pair.left)),
                                        pair.right));
        if (oldProgram.getCondition() != null) {
            programBuilder.addCondition(
                    rexShuttle.apply(oldProgram.expandLocalRef(oldProgram.getCondition())));
        }
        final RexProgram newProgram = programBuilder.getProgram();
        return calc.copy(calc.getTraitSet(), newChangelogNormalize, newProgram);
    }

    private Mappings.TargetMapping buildMapping(RexProgram program) {
        final Map<Integer, Integer> mapInToOutPos = new HashMap<>();
        final List<RexLocalRef> projects = program.getProjectList();
        for (int idx = 0; idx < projects.size(); idx++) {
            RexNode rexNode = program.expandLocalRef(projects.get(idx));
            if (rexNode instanceof RexInputRef) {
                mapInToOutPos.put(((RexInputRef) rexNode).getIndex(), idx);
            }
        }
        return Mappings.target(
                mapInToOutPos,
                program.getInputRowType().getFieldCount(),
                program.getOutputRowType().getFieldCount());
    }

    /**
     * Build a new {@link RelNode} tree in the given nodes order which is in bottom-up direction.
     */
    @SafeVarargs
    private final RelNode buildTreeInOrder(
            RelNode leafNode, Tuple2<RelNode, RelTraitSet>... nodeAndTraits) {
        checkArgument(nodeAndTraits.length >= 1);
        RelNode inputNode = leafNode;
        RelNode currentNode = null;
        for (Tuple2<RelNode, RelTraitSet> nodeAndTrait : nodeAndTraits) {
            currentNode = nodeAndTrait.f0;
            if (currentNode instanceof StreamPhysicalExchange) {
                currentNode =
                        ((StreamPhysicalExchange) currentNode)
                                .copy(
                                        nodeAndTrait.f1,
                                        inputNode,
                                        nodeAndTrait.f1.getTrait(
                                                FlinkRelDistributionTraitDef.INSTANCE()));
            } else if (currentNode instanceof StreamPhysicalChangelogNormalize) {
                final List<String> inputNodeFields = inputNode.getRowType().getFieldNames();
                final List<String> currentNodeFields = currentNode.getRowType().getFieldNames();
                int[] remappedUniqueKeys =
                        Arrays.stream(((StreamPhysicalChangelogNormalize) currentNode).uniqueKeys())
                                .map(ukIdx -> inputNodeFields.indexOf(currentNodeFields.get(ukIdx)))
                                .toArray();
                currentNode =
                        ((StreamPhysicalChangelogNormalize) currentNode)
                                .copy(nodeAndTrait.f1, inputNode, remappedUniqueKeys);
            } else {
                currentNode =
                        currentNode.copy(nodeAndTrait.f1, Collections.singletonList(inputNode));
            }
            inputNode = currentNode;
        }
        return currentNode;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableWatermarkAssignerChangelogNormalizeTransposeRule.Config.builder().build();

        @Override
        default WatermarkAssignerChangelogNormalizeTransposeRule toRule() {
            return new WatermarkAssignerChangelogNormalizeTransposeRule(this);
        }

        default Config withCalc() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWatermarkAssigner.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(StreamPhysicalCalc.class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalChangelogNormalize
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    b3 ->
                                                                                                            b3.operand(
                                                                                                                            StreamPhysicalExchange
                                                                                                                                    .class)
                                                                                                                    .anyInputs()))))
                    .as(Config.class);
        }

        default Config withoutCalc() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(StreamPhysicalWatermarkAssigner.class)
                                            .oneInput(
                                                    b1 ->
                                                            b1.operand(
                                                                            StreamPhysicalChangelogNormalize
                                                                                    .class)
                                                                    .oneInput(
                                                                            b2 ->
                                                                                    b2.operand(
                                                                                                    StreamPhysicalExchange
                                                                                                            .class)
                                                                                            .anyInputs())))
                    .as(Config.class);
        }
    }
}
