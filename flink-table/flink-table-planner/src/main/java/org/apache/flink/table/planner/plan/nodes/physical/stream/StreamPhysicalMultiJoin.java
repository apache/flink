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
import org.apache.flink.table.planner.hint.StateTtlHint;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMultiJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistributionTraitDef;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;

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

    // Cached derived properties to avoid recomputation
    private @Nullable RexNode multiJoinCondition;
    private @Nullable List<List<int[]>> inputUniqueKeys;

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
            final List<RelHint> hints,
            final JoinKeyExtractor keyExtractor) {
        super(cluster, traitSet);
        this.inputs = inputs;
        this.rowType = rowType;
        this.joinFilter = joinFilter;
        this.joinTypes = joinTypes;
        this.joinConditions = joinConditions;
        this.joinAttributeMap = joinAttributeMap;
        this.postJoinFilter = postJoinFilter;
        this.hints = hints;
        this.keyExtractor = keyExtractor;
        this.multiJoinCondition = getMultiJoinCondition();
        this.inputUniqueKeys = getUniqueKeysForInputs();
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
        // Invalidate cached derived properties since inputs changed
        this.multiJoinCondition = null;
        this.inputUniqueKeys = null;
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
                hints,
                keyExtractor);
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
                .item("joinAttributeMap", joinAttributeMap)
                .itemIf("postJoinFilter", postJoinFilter, postJoinFilter != null)
                .item("select", String.join(",", getRowType().getFieldNames()))
                .item("rowType", getRowType())
                .itemIf("stateTtlHints", RelExplainUtil.hintsToString(hints), !hints.isEmpty());
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        final RexNode multijoinCondition = getMultiJoinCondition();
        final List<List<int[]>> localInputUniqueKeys = getUniqueKeysForInputs();
        final List<FlinkJoinType> execJoinTypes = getExecJoinTypes();
        final List<InputProperty> inputProperties = createInputProperties();

        return new StreamExecMultiJoin(
                unwrapTableConfig(this),
                execJoinTypes,
                joinConditions,
                multijoinCondition,
                joinAttributeMap,
                localInputUniqueKeys,
                StateTtlHint.getStateTtlFromHintOnMultiRel(this.hints),
                inputProperties,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    private RexNode createMultiJoinCondition() {
        final List<RexNode> conjunctions = new ArrayList<>();

        for (RexNode joinCondition : joinConditions) {
            if (joinCondition != null) {
                conjunctions.add(joinCondition);
            }
        }

        conjunctions.add(joinFilter);

        if (postJoinFilter != null) {
            conjunctions.add(postJoinFilter);
        }

        return RexUtil.composeConjunction(getCluster().getRexBuilder(), conjunctions, true);
    }

    public List<List<int[]>> getUniqueKeysForInputs() {
        if (inputUniqueKeys == null) {
            final List<List<int[]>> computed =
                    inputs.stream()
                            .map(
                                    input -> {
                                        final Set<ImmutableBitSet> uniqueKeys =
                                                getUniqueKeys(input);

                                        if (uniqueKeys == null) {
                                            return Collections.<int[]>emptyList();
                                        }

                                        return uniqueKeys.stream()
                                                .map(ImmutableBitSet::toArray)
                                                .collect(Collectors.toList());
                                    })
                            .collect(Collectors.toList());
            inputUniqueKeys = Collections.unmodifiableList(computed);
        }
        return inputUniqueKeys;
    }

    public int[] getJoinKeyIndices(int inputId) {
        return keyExtractor.getJoinKeyIndices(inputId);
    }

    private @Nullable Set<ImmutableBitSet> getUniqueKeys(RelNode input) {
        final FlinkRelMetadataQuery fmq =
                FlinkRelMetadataQuery.reuseOrCreate(input.getCluster().getMetadataQuery());
        return fmq.getUniqueKeys(input);
    }

    public RexNode getMultiJoinCondition() {
        if (multiJoinCondition == null) {
            multiJoinCondition = createMultiJoinCondition();
        }
        return multiJoinCondition;
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
     * UPDATE_BEFORE. Otherwise, we can't ignore UPDATE_BEFORE.
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
        final Set<ImmutableBitSet> inputUniqueKeysSet = getUniqueKeys(input);
        if (inputUniqueKeysSet == null || inputUniqueKeysSet.isEmpty()) {
            return false;
        }

        final int[] commonJoinKeyIndices = keyExtractor.getCommonJoinKeyIndices(inputId);
        if (commonJoinKeyIndices.length == 0) {
            return false;
        }

        final ImmutableBitSet commonJoinKeys = ImmutableBitSet.of(commonJoinKeyIndices);
        return inputUniqueKeysSet.stream()
                .anyMatch(uniqueKey -> uniqueKey.contains(commonJoinKeys));
    }

    private List<InputProperty> createInputProperties() {
        final List<InputProperty> inputProperties = new ArrayList<>();

        for (int i = 0; i < inputs.size(); i++) {
            final InputProperty inputProperty = createInputPropertyFromTrait(getInput(i), i);
            inputProperties.add(inputProperty);
        }

        return inputProperties;
    }

    private InputProperty createInputPropertyFromTrait(final RelNode input, final int inputIndex) {
        final FlinkRelDistribution distribution =
                input.getTraitSet().getTrait(FlinkRelDistributionTraitDef.INSTANCE());

        if (distribution == null) {
            return InputProperty.DEFAULT;
        }

        final InputProperty.RequiredDistribution requiredDistribution;
        switch (distribution.getType()) {
            case HASH_DISTRIBUTED:
                final int[] keys = distribution.getKeys().toIntArray();
                if (keys.length == 0) {
                    requiredDistribution = InputProperty.SINGLETON_DISTRIBUTION;
                } else {
                    requiredDistribution = InputProperty.hashDistribution(keys);
                }
                break;
            case SINGLETON:
                requiredDistribution = InputProperty.SINGLETON_DISTRIBUTION;
                break;
            default:
                return InputProperty.DEFAULT;
        }

        return InputProperty.builder()
                .requiredDistribution(requiredDistribution)
                .damBehavior(InputProperty.DamBehavior.PIPELINED)
                .priority(inputIndex)
                .build();
    }
}
