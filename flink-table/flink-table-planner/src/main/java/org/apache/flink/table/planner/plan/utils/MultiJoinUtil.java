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

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptUtil;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            forEachEquality(
                    condition,
                    inputOffsets,
                    inputFieldCounts,
                    (leftRef, rightRef) -> addAttributeRefs(leftRef, rightRef, joinAttributeMap));
        }
        return joinAttributeMap;
    }

    /**
     * Returns, for each input, the indices of the fields in which a NULL means that a row of this
     * input can never be part of a join result together with a row of another input.
     *
     * <p>A field qualifies if it is compared with {@code =} in a conjunct of the condition that
     * joins input {@code k}, and either the field belongs to input {@code k} or the join of input
     * {@code k} is an inner join. {@code IS NOT DISTINCT FROM} never qualifies.
     *
     * @param inputRowTypes the row types of all inputs
     * @param joinConditions the join condition of each input, over the fields of all inputs up to
     *     and including it; the condition of input 0 is not used
     * @param joinTypes the join type of each input
     */
    public static int[][] createNullFilterFields(
            final List<RowType> inputRowTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<FlinkJoinType> joinTypes) {
        final List<Integer> inputFieldCounts =
                inputRowTypes.stream().map(RowType::getFieldCount).collect(Collectors.toList());
        final List<Integer> inputOffsets = inputOffsets(inputFieldCounts);

        final List<Set<Integer>> nullFilterFields = new ArrayList<>();
        for (int i = 0; i < inputRowTypes.size(); i++) {
            nullFilterFields.add(new TreeSet<>());
        }
        for (int k = 1; k < joinConditions.size(); k++) {
            final RexNode condition = joinConditions.get(k);
            if (condition == null) {
                continue;
            }
            final int joinedInput = k;
            final boolean isInnerJoin = joinTypes.get(k) == FlinkJoinType.INNER;
            forEachEquality(
                    condition,
                    inputOffsets,
                    inputFieldCounts,
                    (leftRef, rightRef) -> {
                        for (final InputRef ref : List.of(leftRef, rightRef)) {
                            if (isInnerJoin || ref.inputIndex == joinedInput) {
                                nullFilterFields.get(ref.inputIndex).add(ref.attributeIndex);
                            }
                        }
                    });
        }
        return nullFilterFields.stream()
                .map(fields -> fields.stream().mapToInt(Integer::intValue).toArray())
                .toArray(int[][]::new);
    }

    /**
     * Returns, for each input, the indices of the fields that the join conditions read and that are
     * not part of every key of the input. Two versions of a record have the same key, so only these
     * fields can make the join conditions match them differently.
     *
     * @param inputRowTypes the row types of all inputs
     * @param joinConditions the join condition of each input, over the fields of all inputs up to
     *     and including it
     * @param inputKeys the keys of each input
     */
    public static int[][] createConditionFields(
            final List<RowType> inputRowTypes,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<List<int[]>> inputKeys) {
        final List<Integer> inputFieldCounts =
                inputRowTypes.stream().map(RowType::getFieldCount).collect(Collectors.toList());
        final List<Integer> inputOffsets = inputOffsets(inputFieldCounts);

        final List<Set<Integer>> conditionFields = new ArrayList<>();
        for (int i = 0; i < inputRowTypes.size(); i++) {
            conditionFields.add(new TreeSet<>());
        }
        for (final RexNode condition : joinConditions) {
            if (condition == null) {
                continue;
            }
            for (final int index : RelOptUtil.InputFinder.bits(condition)) {
                final InputRef ref = findInputRef(index, inputOffsets, inputFieldCounts);
                if (ref != null) {
                    conditionFields.get(ref.inputIndex).add(ref.attributeIndex);
                }
            }
        }
        for (int i = 0; i < inputRowTypes.size(); i++) {
            final List<int[]> keys = inputKeys.get(i);
            if (keys.isEmpty()) {
                continue;
            }
            final Set<Integer> fieldsOfEveryKey = toSet(keys.get(0));
            keys.forEach(key -> fieldsOfEveryKey.retainAll(toSet(key)));
            conditionFields.get(i).removeAll(fieldsOfEveryKey);
        }
        return conditionFields.stream()
                .map(fields -> fields.stream().mapToInt(Integer::intValue).toArray())
                .toArray(int[][]::new);
    }

    private static List<Integer> inputOffsets(final List<Integer> inputFieldCounts) {
        final List<Integer> inputOffsets = new ArrayList<>();
        int currentOffset = 0;
        for (final Integer count : inputFieldCounts) {
            inputOffsets.add(currentOffset);
            currentOffset += count;
        }
        return inputOffsets;
    }

    private static Set<Integer> toSet(final int[] fields) {
        return IntStream.of(fields).boxed().collect(Collectors.toSet());
    }

    /**
     * Calls {@code consumer} for each {@code =} between two input fields in the conjuncts of {@code
     * condition}, with the field of the input with the lower index first.
     */
    private static void forEachEquality(
            final RexNode condition,
            final List<Integer> inputOffsets,
            final List<Integer> inputFieldCounts,
            final BiConsumer<InputRef, InputRef> consumer) {
        if (!(condition instanceof RexCall)) {
            return;
        }

        final RexCall call = (RexCall) condition;
        final SqlKind kind = call.getOperator().getKind();

        if (kind != SqlKind.EQUALS) {
            // Only conjunctions (AND) can contain equality conditions that are valid for multijoin.
            // All other condition types are deferred to the postJoinFilter.
            if (kind == SqlKind.AND) {
                for (final RexNode operand : call.getOperands()) {
                    forEachEquality(operand, inputOffsets, inputFieldCounts, consumer);
                }
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

        if (inputRef1.inputIndex < inputRef2.inputIndex) {
            consumer.accept(inputRef1, inputRef2);
        } else {
            consumer.accept(inputRef2, inputRef1);
        }
    }

    private static void addAttributeRefs(
            final InputRef leftRef,
            final InputRef rightRef,
            final Map<Integer, List<AttributeBasedJoinKeyExtractor.ConditionAttributeRef>>
                    joinAttributeMap) {
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
