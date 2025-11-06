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

import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiJoinUtil {
    public static Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
            createJoinAttributeMap(
                    List<RelNode> joinInputs, List<? extends RexNode> joinConditions) {
        final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                joinAttributeMap = new HashMap<>();
        final List<Integer> inputFieldCounts =
                joinInputs.stream()
                        .map(input -> input.getRowType().getFieldCount())
                        .collect(Collectors.toList());

        final List<Integer> inputOffsets = new ArrayList<>();
        int currentOffset = 0;
        for (final Integer count : inputFieldCounts) {
            inputOffsets.add(currentOffset);
            currentOffset += count;
        }

        for (final RexNode condition : joinConditions) {
            extractEqualityConditions(condition, inputOffsets, inputFieldCounts, joinAttributeMap);
        }
        return joinAttributeMap;
    }

    private static void extractEqualityConditions(
            final RexNode condition,
            final List<Integer> inputOffsets,
            final List<Integer> inputFieldCounts,
            final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                    joinAttributeMap) {
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

        // Special case for input 0:
        // Since we are building attribute references that do left -> right index,
        // we need a special base case for input 0 which has no input to the left.
        // So we do {-1, -1} -> {0, attributeIndex}
        if (leftRef.inputIndex == 0) {
            final AttributeBasedJoinKeyExtractor.ConditionAttributeRef firstAttrRef =
                    new AttributeBasedJoinKeyExtractor.ConditionAttributeRef(
                            -1, -1, leftRef.inputIndex, leftRef.attributeIndex);
            joinAttributeMap
                    .computeIfAbsent(leftRef.inputIndex, k -> new ArrayList<>())
                    .add(firstAttrRef);
        }

        final AttributeBasedJoinKeyExtractor.ConditionAttributeRef attrRef =
                new AttributeBasedJoinKeyExtractor.ConditionAttributeRef(
                        leftRef.inputIndex,
                        leftRef.attributeIndex,
                        rightRef.inputIndex,
                        rightRef.attributeIndex);
        joinAttributeMap.computeIfAbsent(rightRef.inputIndex, k -> new ArrayList<>()).add(attrRef);
    }

    private static @Nullable InputRef findInputRef(
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
