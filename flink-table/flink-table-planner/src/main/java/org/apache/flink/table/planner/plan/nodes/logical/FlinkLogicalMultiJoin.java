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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.hint.StateTtlHint;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.hint.StateTtlHint.STATE_TTL;

/**
 * A relational expression that represents a multi-input join in Flink.
 *
 * <p>This node is used to represent a join of multiple inputs, which can be seen as a
 * generalization of the binary {@link FlinkLogicalJoin}. It will be translated into a {@link
 * StreamPhysicalMultiJoin} by the optimizer.
 */
@Internal
public class FlinkLogicalMultiJoin extends AbstractRelNode implements FlinkLogicalRel {

    public static final Converter CONVERTER =
            new Converter(
                    ConverterRule.Config.INSTANCE.withConversion(
                            MultiJoin.class,
                            Convention.NONE,
                            FlinkConventions.LOGICAL(),
                            "FlinkLogicalMultiJoinConverter"));

    private List<RelNode> inputs;
    private final RexNode joinFilter;
    private final List<JoinRelType> joinTypes;
    private final List<? extends @Nullable RexNode> joinConditions;
    private final @Nullable RexNode postJoinFilter;
    private final List<RelHint> hints;

    public FlinkLogicalMultiJoin(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final List<RelNode> inputs,
            final RexNode joinFilter,
            final RelDataType rowType,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<JoinRelType> joinTypes,
            final @Nullable RexNode postJoinFilter,
            final List<RelHint> hints) {
        super(cluster, traitSet);
        this.inputs = inputs;
        this.rowType = rowType;
        this.joinFilter = joinFilter;
        this.joinTypes = joinTypes;
        this.joinConditions = joinConditions;
        this.postJoinFilter = postJoinFilter;
        this.hints = hints;
    }

    public static FlinkLogicalMultiJoin create(
            final RelOptCluster cluster,
            final List<RelNode> inputs,
            final RexNode joinFilter,
            final RelDataType rowType,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<JoinRelType> joinTypes,
            final @Nullable RexNode postJoinFilter,
            final List<RelHint> hints) {
        final RelTraitSet traitSet = cluster.traitSetOf(FlinkConventions.LOGICAL()).simplify();
        return new FlinkLogicalMultiJoin(
                cluster,
                traitSet,
                inputs,
                joinFilter,
                rowType,
                joinConditions,
                joinTypes,
                postJoinFilter,
                hints);
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public void replaceInput(final int ordinalInParent, final RelNode p) {
        assert ordinalInParent >= 0 && ordinalInParent < inputs.size();
        final List<RelNode> newInputs = new ArrayList<>(inputs);
        newInputs.set(ordinalInParent, p);
        this.inputs = List.copyOf(newInputs);
        recomputeDigest();
    }

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return new FlinkLogicalMultiJoin(
                getCluster(),
                traitSet,
                inputs,
                joinFilter,
                rowType,
                joinConditions,
                joinTypes,
                postJoinFilter,
                hints);
    }

    @Override
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        double rowCount = 1.0;
        double cpu = 0.0;
        double io = 0.0;

        for (final RelNode input : inputs) {
            final Double inputRowCount = mq.getRowCount(input);
            if (inputRowCount == null) {
                return planner.getCostFactory().makeHugeCost();
            }

            final Double averageRowSize = mq.getAverageRowSize(input);
            final double dAverageRowSize = averageRowSize == null ? 100.0 : averageRowSize;
            rowCount += inputRowCount;
            cpu += inputRowCount;
            io += inputRowCount * dAverageRowSize;
        }

        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    public RexNode getJoinFilter() {
        return joinFilter;
    }

    public List<JoinRelType> getJoinTypes() {
        return joinTypes;
    }

    public List<? extends RexNode> getJoinConditions() {
        return joinConditions;
    }

    public @Nullable RexNode getPostJoinFilter() {
        return postJoinFilter;
    }

    public List<RelHint> getHints() {
        return hints;
    }

    public List<RelNode> getChildren() {
        return getInputs();
    }

    /** Converter rule that converts a {@link MultiJoin} to a {@link FlinkLogicalMultiJoin}. */
    @Internal
    public static class Converter extends ConverterRule {
        public Converter(final Config config) {
            super(config);
        }

        @Override
        public RelNode convert(final RelNode rel) {
            final MultiJoin multiJoin = (MultiJoin) rel;
            final List<RelNode> newInputs =
                    multiJoin.getInputs().stream()
                            .map(input -> RelOptRule.convert(input, FlinkConventions.LOGICAL()))
                            .collect(Collectors.toList());
            final List<RelHint> hints = normalizeStateTtlHints(multiJoin.getHints());

            return FlinkLogicalMultiJoin.create(
                    multiJoin.getCluster(),
                    newInputs,
                    multiJoin.getJoinFilter(),
                    multiJoin.getRowType(),
                    multiJoin.getOuterJoinConditions(),
                    multiJoin.getJoinTypes(),
                    multiJoin.getPostJoinFilter(),
                    hints);
        }

        private List<RelHint> normalizeStateTtlHints(final List<RelHint> originalHints) {
            final boolean containsValidStateTtlHint =
                    originalHints.stream()
                            .filter(hint -> hint.hintName.equals(STATE_TTL.getHintName()))
                            .anyMatch(
                                    hint ->
                                            hint.listOptions.stream()
                                                    .anyMatch(
                                                            option ->
                                                                    !option.equals(
                                                                            StateTtlHint
                                                                                    .NO_STATE_TTL)));

            if (containsValidStateTtlHint) {
                return originalHints;
            } else {
                return originalHints.stream()
                        .filter(hint -> !hint.hintName.equals(STATE_TTL.getHintName()))
                        .collect(Collectors.toList());
            }
        }
    }
}
