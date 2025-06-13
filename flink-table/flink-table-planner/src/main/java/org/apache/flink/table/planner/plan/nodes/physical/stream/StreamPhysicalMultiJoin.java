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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * {@link StreamPhysicalRel} for a multi-input regular join.
 *
 * <p>This node takes multiple inputs and joins them based on a given filter condition. It supports
 * different join types for each pair of inputs. This node will be translated into a {@link
 * StreamExecMultiJoin}.
 */
public class StreamPhysicalMultiJoin extends AbstractRelNode implements StreamPhysicalRel {

    private List<RelNode> inputs;
    private final RexNode joinFilter;
    private final List<JoinRelType> joinTypes;
    private final List<? extends @Nullable RexNode> joinConditions;
    private final JoinKeyExtractor keyExtractor;

    /**
     * A map from input index to a list of {@link ConditionAttributeRef}. Each {@link
     * ConditionAttributeRef} represents a join condition associated with the input.
     */
    private final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap;

    private final @Nullable RexNode postJoinFilter;
    private final List<RelHint> hints;

    public StreamPhysicalMultiJoin(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final List<RelNode> inputs,
            final RexNode joinFilter,
            final RelDataType rowType,
            final List<? extends @Nullable RexNode> joinConditions,
            final List<JoinRelType> joinTypes,
            final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap,
            final @Nullable RexNode postJoinFilter,
            final List<RelHint> hints) {
        super(cluster, traitSet);
        this.inputs = inputs;
        this.rowType = rowType;
        this.joinFilter = joinFilter;
        this.joinTypes = joinTypes;
        this.joinConditions = joinConditions;
        this.joinAttributeMap = joinAttributeMap;
        this.postJoinFilter = postJoinFilter;
        this.hints = hints;
        final List<RowType> inputRowTypes =
                inputs.stream()
                        .map(i -> FlinkTypeFactory.toLogicalRowType(i.getRowType()))
                        .collect(Collectors.toList());
        this.keyExtractor = new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes);
    }

    @Override
    public boolean requireWatermark() {
        return false;
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
        return new StreamPhysicalMultiJoin(
                getCluster(),
                traitSet,
                inputs,
                joinFilter,
                getRowType(),
                joinConditions,
                joinTypes,
                joinAttributeMap,
                postJoinFilter,
                hints);
    }

    @Override
    public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
        final double elementRate = 100.0d * getInputs().size();
        return planner.getCostFactory().makeCost(elementRate, elementRate, 0);
    }

    @Override
    public RelWriter explainTerms(final RelWriter pw) {
        super.explainTerms(pw);
        for (final Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("joinFilter", joinFilter)
                .item("joinTypes", joinTypes)
                .item("joinConditions", joinConditions)
                .itemIf("postJoinFilter", postJoinFilter, postJoinFilter != null)
                .item("select", String.join(",", getRowType().getFieldNames()))
                .item("rowType", getRowType());
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        final RexNode multiJoinCondition = createMultiJoinCondition();
        final List<List<int[]>> inputUpsertKeys = getUpsertKeysForInputs();
        final List<FlinkJoinType> execJoinTypes = getExecJoinTypes();

        return new StreamExecMultiJoin(
                unwrapTableConfig(this),
                execJoinTypes,
                joinConditions,
                multiJoinCondition,
                joinAttributeMap,
                inputUpsertKeys,
                Collections.emptyMap(), // TODO Enable hint-based state ttl. See ticket
                // TODO https://issues.apache.org/jira/browse/FLINK-37936
                inputs.stream().map(i -> InputProperty.DEFAULT).collect(Collectors.toList()),
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    private RexNode createMultiJoinCondition() {
        final List<RexNode> conjunctions = new ArrayList<>();
        conjunctions.add(joinFilter);
        if (postJoinFilter != null) {
            conjunctions.add(postJoinFilter);
        }
        return RexUtil.composeConjunction(getCluster().getRexBuilder(), conjunctions, true);
    }

    private List<List<int[]>> getUpsertKeysForInputs() {
        return inputs.stream()
                .map(
                        input -> {
                            final Set<ImmutableBitSet> upsertKeys = getUpsertKeys(input);

                            if (upsertKeys == null) {
                                return Collections.<int[]>emptyList();
                            }
                            return upsertKeys.stream()
                                    .map(ImmutableBitSet::toArray)
                                    .collect(Collectors.toList());
                        })
                .collect(Collectors.toList());
    }

    private @Nullable Set<ImmutableBitSet> getUpsertKeys(RelNode input) {
        final FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
        return fmq.getUpsertKeys(input);
    }

    private List<FlinkJoinType> getExecJoinTypes() {
        return joinTypes.stream()
                .map(
                        joinType -> {
                            if (joinType == JoinRelType.INNER) {
                                return FlinkJoinType.INNER;
                            } else if (joinType == JoinRelType.LEFT) {
                                return FlinkJoinType.LEFT;
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unsupported join type: " + joinType);
                            }
                        })
                .collect(Collectors.toList());
    }

    public List<JoinRelType> getJoinTypes() {
        return joinTypes;
    }

    /**
     * This is mainly used in `FlinkChangelogModeInferenceProgram.SatisfyUpdateKindTraitVisitor`. If
     * the unique key of input is a superset of the common join key, then we can ignore
     * UPDATE_BEFORE. Otherwise, it we can't ignore UPDATE_BEFORE.
     *
     * <p>For example, if the input schema is [id, name, cnt] with the unique key (id) and the
     * common join key is (id, name) across joins, then an insert and update on the id:
     *
     * <p>+I(1001, Tim, 10) -U(1001, Tim, 10) +U(1001, Timo, 11)
     *
     * <p>If the UPDATE_BEFORE is ignored, the `+I(1001, Tim, 10)` record in join will never be
     * retracted. Therefore, if we want to ignore UPDATE_BEFORE, the unique key must contain join
     * key.
     *
     * <p>This is similar to {@link StreamPhysicalJoin#inputUniqueKeyContainsJoinKey(int)} but here
     * we use the common join key, since the multi join operator partitions on the common join key.
     */
    public boolean inputUniqueKeyContainsCommonJoinKey(int inputId) {
        final RelNode input = getInputs().get(inputId);
        final Set<ImmutableBitSet> inputUniqueKeys = getUpsertKeys(input);
        if (inputUniqueKeys == null || inputUniqueKeys.isEmpty()) {
            return false;
        }

        final int[] commonJoinKeyIndices = keyExtractor.getCommonJoinKeyIndices(inputId);
        if (commonJoinKeyIndices.length == 0) {
            return false;
        }

        final ImmutableBitSet commonJoinKeys = ImmutableBitSet.of(commonJoinKeyIndices);

        return inputUniqueKeys.stream().anyMatch(uniqueKey -> uniqueKey.contains(commonJoinKeys));
    }
}
