/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMultiJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Rule that converts {@link FlinkLogicalMultiJoin} to {@link StreamPhysicalMultiJoin}. */
public class StreamPhysicalMultiJoinRule extends ConverterRule {
    public static final RelOptRule INSTANCE = new StreamPhysicalMultiJoinRule();

    private StreamPhysicalMultiJoinRule() {
        super(
                Config.INSTANCE.withConversion(
                        FlinkLogicalMultiJoin.class,
                        FlinkConventions.LOGICAL(),
                        FlinkConventions.STREAM_PHYSICAL(),
                        "StreamPhysicalMultiJoinRule"));
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final FlinkLogicalMultiJoin multiJoin = call.rel(0);

        if (isTemporalJoin(multiJoin)) {
            return false;
        }

        // Time attributes are not allowed in regular joins, as they are used for time-based
        // operations like windowing. See FLINK-37962 for more details.
        return !hasTimeAttributes(multiJoin);
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final FlinkLogicalMultiJoin multiJoin = (FlinkLogicalMultiJoin) rel;
        final List<RelNode> newInputs =
                multiJoin.getInputs().stream()
                        .map(
                                input ->
                                        RelOptRule.convert(
                                                input,
                                                input.getTraitSet()
                                                        .replace(FlinkConventions.STREAM_PHYSICAL())
                                                        .simplify()))
                        .collect(Collectors.toList());

        final RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap =
                createJoinAttributeMap(multiJoin);

        final JoinKeyExtractor keyExtractor;
        final List<RowType> inputRowTypes =
                newInputs.stream()
                        .map(i -> FlinkTypeFactory.toLogicalRowType(i.getRowType()))
                        .collect(Collectors.toList());
        keyExtractor = new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes);

        return new StreamPhysicalMultiJoin(
                multiJoin.getCluster(),
                traitSet,
                newInputs,
                multiJoin.getJoinFilter(),
                multiJoin.getRowType(),
                multiJoin.getJoinConditions(),
                multiJoin.getJoinTypes(),
                joinAttributeMap,
                multiJoin.getPostJoinFilter(),
                multiJoin.getHints(),
                keyExtractor);
    }

    private boolean isTemporalJoin(final FlinkLogicalMultiJoin multiJoin) {
        // Not supported
        return multiJoin.getInputs().stream()
                .anyMatch(input -> input instanceof FlinkLogicalSnapshot);
    }

    private boolean hasTimeAttributes(final FlinkLogicalMultiJoin multiJoin) {
        // Time attributes must not be in the output type of regular join.
        final boolean timeAttrInOutput =
                multiJoin.getRowType().getFieldList().stream()
                        .anyMatch(f -> FlinkTypeFactory.isTimeIndicatorType(f.getType()));
        if (timeAttrInOutput) {
            return true;
        }

        // Join condition must not access time attributes.
        final List<RelNode> inputs = multiJoin.getInputs();
        if (inputs.isEmpty()) {
            return false;
        }

        final RelDataType inputsRowType = createInputsRowType(multiJoin);
        return JoinUtil.accessesTimeAttribute(multiJoin.getJoinFilter(), inputsRowType);
    }

    private RelDataType createInputsRowType(final FlinkLogicalMultiJoin multiJoin) {
        final List<RelDataType> inputTypes =
                multiJoin.getInputs().stream()
                        .map(RelNode::getRowType)
                        .collect(Collectors.toList());
        final RelDataTypeFactory typeFactory = multiJoin.getCluster().getTypeFactory();

        RelDataType connectedInputsRowType = inputTypes.get(0);
        for (int i = 1; i < inputTypes.size(); i++) {
            connectedInputsRowType =
                    SqlValidatorUtil.createJoinType(
                            typeFactory,
                            connectedInputsRowType,
                            inputTypes.get(i),
                            null,
                            Collections.emptyList());
        }
        return connectedInputsRowType;
    }

    private Map<Integer, List<ConditionAttributeRef>> createJoinAttributeMap(
            final FlinkLogicalMultiJoin multiJoin) {
        final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap = new HashMap<>();
        final List<Integer> inputFieldCounts =
                multiJoin.getInputs().stream()
                        .map(input -> input.getRowType().getFieldCount())
                        .collect(Collectors.toList());

        final List<Integer> inputOffsets = new ArrayList<>();
        int currentOffset = 0;
        for (final Integer count : inputFieldCounts) {
            inputOffsets.add(currentOffset);
            currentOffset += count;
        }

        final List<? extends RexNode> joinConditions = multiJoin.getJoinConditions();
        for (final RexNode condition : joinConditions) {
            extractEqualityConditions(condition, inputOffsets, inputFieldCounts, joinAttributeMap);
        }
        return joinAttributeMap;
    }

    private void extractEqualityConditions(
            final RexNode condition,
            final List<Integer> inputOffsets,
            final List<Integer> inputFieldCounts,
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap) {
        if (!(condition instanceof RexCall)) {
            return;
        }

        final RexCall call = (RexCall) condition;
        final SqlKind kind = call.getOperator().getKind();

        if (kind != SqlKind.EQUALS) {
            for (final RexNode operand : call.getOperands()) {
                extractEqualityConditions(
                        operand, inputOffsets, inputFieldCounts, joinAttributeMap);
            }
            return;
        }

        if (call.getOperands().size() != 2) {
            return;
        }

        final RexNode op1 = call.getOperands().get(0);
        final RexNode op2 = call.getOperands().get(1);

        if (!(op1 instanceof RexInputRef) || !(op2 instanceof RexInputRef)) {
            return;
        }

        final InputRef inputRef1 =
                findInputRef(((RexInputRef) op1).getIndex(), inputOffsets, inputFieldCounts);
        final InputRef inputRef2 =
                findInputRef(((RexInputRef) op2).getIndex(), inputOffsets, inputFieldCounts);

        if (inputRef1 == null || inputRef2 == null) {
            return;
        }

        final InputRef leftRef;
        final InputRef rightRef;
        if (inputRef1.inputIndex < inputRef2.inputIndex) {
            leftRef = inputRef1;
            rightRef = inputRef2;
        } else {
            leftRef = inputRef2;
            rightRef = inputRef1;
        }

        final ConditionAttributeRef attrRef =
                new ConditionAttributeRef(
                        leftRef.inputIndex,
                        leftRef.attributeIndex,
                        rightRef.inputIndex,
                        rightRef.attributeIndex);
        joinAttributeMap.computeIfAbsent(rightRef.inputIndex, k -> new ArrayList<>()).add(attrRef);
    }

    private @Nullable InputRef findInputRef(
            final int fieldIndex,
            final List<Integer> inputOffsets,
            final List<Integer> inputFieldCounts) {
        for (int i = 0; i < inputOffsets.size(); i++) {
            final int offset = inputOffsets.get(i);
            if (fieldIndex >= offset && fieldIndex < offset + inputFieldCounts.get(i)) {
                return new InputRef(i, fieldIndex - offset);
            }
        }
        return null;
    }

    private static final class InputRef {
        private final int inputIndex;
        private final int attributeIndex;

        private InputRef(final int inputIndex, final int attributeIndex) {
            this.inputIndex = inputIndex;
            this.attributeIndex = attributeIndex;
        }
    }
}
