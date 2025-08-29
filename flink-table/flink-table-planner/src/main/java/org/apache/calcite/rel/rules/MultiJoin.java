/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A MultiJoin represents a join of N inputs, whereas regular Joins represent strictly binary joins.
 */
public final class MultiJoin extends AbstractRelNode {
    // ~ Instance fields --------------------------------------------------------

    private final List<RelNode> inputs;
    private final RexNode joinFilter;

    @SuppressWarnings("HidingField")
    private final RelDataType rowType;

    private final boolean isFullOuterJoin;
    private final List<@Nullable RexNode> outerJoinConditions;
    private final ImmutableList<JoinRelType> joinTypes;
    private final List<@Nullable ImmutableBitSet> projFields;
    public final ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap;
    private final @Nullable RexNode postJoinFilter;
    private final List<Integer> levels;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Constructs a MultiJoin.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multi-join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each join input, if the input
     *     is null-generating in a left or right outer join; null otherwise
     * @param joinTypes the join type corresponding to each input; if an input is null-generating in
     *     a left or right outer join, the entry indicates the type of outer join; otherwise, the
     *     entry is set to INNER
     * @param projFields fields that will be projected from each input; if null, projection
     *     information is not available yet so it's assumed that all fields from the input are
     *     projected
     * @param joinFieldRefCountsMap counters of the number of times each field is referenced in join
     *     conditions, indexed by the input #
     * @param postJoinFilter filter to be applied after the joins are
     */
    public MultiJoin(
            RelOptCluster cluster,
            List<RelNode> inputs,
            RexNode joinFilter,
            RelDataType rowType,
            boolean isFullOuterJoin,
            List<? extends @Nullable RexNode> outerJoinConditions,
            List<JoinRelType> joinTypes,
            List<? extends @Nullable ImmutableBitSet> projFields,
            ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
            @Nullable RexNode postJoinFilter) {
        this(
                cluster,
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter,
                Collections.emptyList());
    }

    /**
     * Constructs a {@code MultiJoin}, which represents a flattened tree of joins for use by the
     * {@code JoinToMultiJoinRule}.
     *
     * @param cluster cluster that join belongs to
     * @param inputs inputs into this multi-join
     * @param joinFilter join filter applicable to this join node
     * @param rowType row type of the join result of this node
     * @param isFullOuterJoin true if the join is a full outer join
     * @param outerJoinConditions outer join condition associated with each join input, if the input
     *     is null-generating in a left or right outer join; null otherwise
     * @param joinTypes the join type corresponding to each input; if an input is null-generating in
     *     a left or right outer join, the entry indicates the type of outer join; otherwise, the
     *     entry is set to INNER
     * @param projFields fields that will be projected from each input; if null, projection
     *     information is not available yet so it's assumed that all fields from the input are
     *     projected
     * @param joinFieldRefCountsMap counters of the number of times each field is referenced in join
     *     conditions, indexed by the input #
     * @param postJoinFilter filter to be applied after the joins are
     * @param levels join tree levels
     */
    public MultiJoin(
            RelOptCluster cluster,
            List<RelNode> inputs,
            RexNode joinFilter,
            RelDataType rowType,
            boolean isFullOuterJoin,
            List<? extends @Nullable RexNode> outerJoinConditions,
            List<JoinRelType> joinTypes,
            List<? extends @Nullable ImmutableBitSet> projFields,
            ImmutableMap<Integer, ImmutableIntList> joinFieldRefCountsMap,
            @Nullable RexNode postJoinFilter,
            List<Integer> levels) {
        super(cluster, cluster.traitSetOf(Convention.NONE));
        this.inputs = Lists.newArrayList(inputs);
        this.joinFilter = joinFilter;
        this.rowType = rowType;
        this.isFullOuterJoin = isFullOuterJoin;
        this.outerJoinConditions = ImmutableNullableList.copyOf(outerJoinConditions);
        this.levels = levels;
        assert outerJoinConditions.size() == inputs.size();
        this.joinTypes = ImmutableList.copyOf(joinTypes);
        this.projFields = ImmutableNullableList.copyOf(projFields);
        this.joinFieldRefCountsMap = joinFieldRefCountsMap;
        this.postJoinFilter = postJoinFilter;
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
        return new MultiJoin(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter,
                levels);
    }

    /** Returns a deep copy of {@link #joinFieldRefCountsMap}. */
    private Map<Integer, int[]> cloneJoinFieldRefCountsMap() {
        Map<Integer, int[]> clonedMap = new HashMap<>();
        for (int i = 0; i < inputs.size(); i++) {
            clonedMap.put(i, requireNonNull(joinFieldRefCountsMap.get(i)).toIntArray());
        }
        return clonedMap;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> joinTypeNames = new ArrayList<>();
        List<String> outerJoinConds = new ArrayList<>();
        List<String> projFieldObjects = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            joinTypeNames.add(joinTypes.get(i).name());
            RexNode outerJoinCondition = outerJoinConditions.get(i);
            if (outerJoinCondition == null) {
                outerJoinConds.add("NULL");
            } else {
                outerJoinConds.add(outerJoinCondition.toString());
            }
            ImmutableBitSet projField = projFields.get(i);
            if (projField == null) {
                projFieldObjects.add("ALL");
            } else {
                projFieldObjects.add(projField.toString());
            }
        }

        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("joinFilter", joinFilter)
                .item("isFullOuterJoin", isFullOuterJoin)
                .item("joinTypes", joinTypeNames)
                .item("outerJoinConditions", outerJoinConds)
                .item("projFields", projFieldObjects)
                .itemIf("postJoinFilter", postJoinFilter, postJoinFilter != null);
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
        List<@Nullable RexNode> outerJoinConditions = shuttle.apply(this.outerJoinConditions);
        RexNode postJoinFilter = shuttle.apply(this.postJoinFilter);

        if (joinFilter == this.joinFilter
                && outerJoinConditions == this.outerJoinConditions
                && postJoinFilter == this.postJoinFilter) {
            return this;
        }

        return new MultiJoin(
                getCluster(),
                inputs,
                joinFilter,
                rowType,
                isFullOuterJoin,
                outerJoinConditions,
                joinTypes,
                projFields,
                joinFieldRefCountsMap,
                postJoinFilter,
                levels);
    }

    /** Returns join filters associated with this MultiJoin. */
    public RexNode getJoinFilter() {
        return joinFilter;
    }

    /** Returns true if the MultiJoin corresponds to a full outer join. */
    public boolean isFullOuterJoin() {
        return isFullOuterJoin;
    }

    /** Returns outer join conditions for null-generating inputs. */
    public List<@Nullable RexNode> getOuterJoinConditions() {
        return outerJoinConditions;
    }

    /** Returns join types of each input. */
    public List<JoinRelType> getJoinTypes() {
        return joinTypes;
    }

    /**
     * Returns bitmaps representing the fields projected from each input; if an entry is null, all
     * fields are projected.
     */
    public List<@Nullable ImmutableBitSet> getProjFields() {
        return projFields;
    }

    /**
     * Returns the map of reference counts for each input, representing the fields accessed in join
     * conditions.
     */
    public ImmutableMap<Integer, ImmutableIntList> getJoinFieldRefCountsMap() {
        return joinFieldRefCountsMap;
    }

    /**
     * Returns a copy of the map of reference counts for each input, representing the fields
     * accessed in join conditions.
     */
    public Map<Integer, int[]> getCopyJoinFieldRefCountsMap() {
        return cloneJoinFieldRefCountsMap();
    }

    /** Returns post-join filter associated with this MultiJoin. */
    public @Nullable RexNode getPostJoinFilter() {
        return postJoinFilter;
    }

    boolean containsOuter() {
        for (JoinRelType joinType : joinTypes) {
            if (joinType.isOuterJoin()) {
                return true;
            }
        }
        return false;
    }

    public List<Integer> getLevels() {
        return levels;
    }
}
