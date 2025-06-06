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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins represent strictly binary joins.
 */
public final class FlinkMultiJoinNode extends AbstractRelNode {
    // ~ Instance fields --------------------------------------------------------

    private final List<RelNode> inputs;
    private final RexNode joinFilter;

    @SuppressWarnings("HidingField")
    private final RelDataType rowType;

    private final List<@Nullable RexNode> joinConditions;
    private final ImmutableList<JoinRelType> joinTypes;
    private final @NonNull Set<String> commonJoinKeys;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Constructs a MultiJoin.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multi-join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param joinConditions join conditions associated with each join input
     * @param joinTypes the join type corresponding to each input
     */
    public FlinkMultiJoinNode(
            RelOptCluster cluster,
            List<RelNode> inputs,
            RexNode joinFilter,
            RelDataType rowType,
            List<? extends @Nullable RexNode> joinConditions,
            List<JoinRelType> joinTypes,
            @NonNull Set<String> commonJoinKeys) {
        super(cluster, cluster.traitSetOf(Convention.NONE));
        this.inputs = Lists.newArrayList(inputs);
        this.joinFilter = joinFilter;
        this.rowType = rowType;
        this.joinConditions = ImmutableNullableList.copyOf(joinConditions);
        this.joinTypes = ImmutableList.copyOf(joinTypes);
        this.commonJoinKeys = commonJoinKeys;
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        inputs.set(ordinalInParent, p);
        recomputeDigest();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new FlinkMultiJoinNode(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                joinConditions,
                joinTypes,
                commonJoinKeys);
    }

    public RelNode copy(List<RexNode> joinFilters) {
        RelOptCluster cluster = getCluster();
        return new FlinkMultiJoinNode(
                cluster,
                inputs,
                RexUtil.composeConjunction(cluster.getRexBuilder(), joinFilters),
                rowType,
                joinFilters,
                joinTypes,
                commonJoinKeys);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> joinTypeNames =
                joinTypes.stream().map(Enum::name).collect(Collectors.toList());
        List<String> joinConds =
                joinConditions.stream()
                        .filter(Objects::nonNull)
                        .map(RexNode::toString)
                        .collect(Collectors.toList());

        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("joinFilter", joinFilter)
                .item("joinTypes", joinTypeNames)
                .item("joinConditions", joinConds)
                .item("commonJoinKeys", commonJoinKeys);
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode joinFilter = shuttle.apply(this.joinFilter);
        List<@Nullable RexNode> outerJoinConditions = shuttle.apply(this.joinConditions);

        if (joinFilter == this.joinFilter && outerJoinConditions == this.joinConditions) {
            return this;
        }

        return new FlinkMultiJoinNode(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                outerJoinConditions,
                joinTypes,
                commonJoinKeys);
    }

    /** Returns join filters associated with this MultiJoin. */
    public RexNode getJoinFilter() {
        return joinFilter;
    }

    /** Returns outer join conditions for null-generating inputs. */
    public List<@Nullable RexNode> getJoinConditions() {
        return joinConditions;
    }

    /** Returns join types of each input. */
    public List<JoinRelType> getJoinTypes() {
        return joinTypes;
    }

    public @NonNull Set<String> getCommonJoinKeys() {
        return commonJoinKeys;
    }

    public Tuple2<RelNode, Integer> getInputByFieldIdx(int fieldNum) {
        RelNode targetInput = null;
        int idxInTargetInput = 0;
        int inputFieldEnd = 0;
        for (RelNode input : inputs) {
            inputFieldEnd += input.getRowType().getFieldCount();
            if (fieldNum < inputFieldEnd) {
                targetInput = input;
                int targetInputStartIdx = inputFieldEnd - input.getRowType().getFieldCount();
                idxInTargetInput = fieldNum - targetInputStartIdx;
                break;
            }
        }

        assert targetInput != null;

        return new Tuple2<>(targetInput, idxInTargetInput);
    }
}
