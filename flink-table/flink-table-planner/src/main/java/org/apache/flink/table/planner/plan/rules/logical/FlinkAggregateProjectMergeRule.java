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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Extends calcite's AggregateProjectMergeRule, modification: consider the timeAttribute field when
 * analyzing interesting fields for the LogicalWindowAggregate node while normal Aggregate node
 * needn't.
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Should be removed after legacy groupWindowAggregate was removed: Lines 83 ~ 101
 * </ol>
 */
public class FlinkAggregateProjectMergeRule extends AggregateProjectMergeRule {

    public static final RelOptRule INSTANCE = new FlinkAggregateProjectMergeRule(Config.DEFAULT);

    protected FlinkAggregateProjectMergeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        final Project project = call.rel(1);
        RelNode x = newApply(call, aggregate, project);
        if (x != null) {
            call.transformTo(x);
        }
    }

    public static @Nullable RelNode newApply(
            RelOptRuleCall call, Aggregate aggregate, Project project) {
        // Find all fields which we need to be straightforward field projections.
        final Set<Integer> interestingFields = RelOptUtil.getAllFields(aggregate);

        // Should add the field of timeAttribute in a LogicalWindowAggregate node which uses rowTime
        if (aggregate instanceof LogicalWindowAggregate) {
            LogicalWindowAggregate winAgg = (LogicalWindowAggregate) aggregate;
            // isRowtimeAttribute can't be used here because the time_indicator phase comes later
            boolean isProcTime =
                    LogicalTypeChecks.isProctimeAttribute(
                            winAgg.getWindow()
                                    .timeAttribute()
                                    .getOutputDataType()
                                    .getLogicalType());
            if (!isProcTime) {
                // no need to consider the inputIndex because LogicalWindowAggregate is single input
                interestingFields.add(
                        ((LogicalWindowAggregate) aggregate)
                                .getWindow()
                                .timeAttribute()
                                .getFieldIndex());
            }
        }

        // Build the map from old to new; abort if any entry is not a
        // straightforward field projection.
        final Map<Integer, Integer> map = new HashMap<>();
        for (int source : interestingFields) {
            final RexNode rex = project.getProjects().get(source);
            if (!(rex instanceof RexInputRef)) {
                return null;
            }
            map.put(source, ((RexInputRef) rex).getIndex());
        }

        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            newGroupingSets =
                    ImmutableBitSet.ORDERING.immutableSortedCopy(
                            ImmutableBitSet.permute(aggregate.getGroupSets(), map));
        }

        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final int sourceCount = aggregate.getInput().getRowType().getFieldCount();
        final int targetCount = project.getInput().getRowType().getFieldCount();
        final Mappings.TargetMapping targetMapping = Mappings.target(map, sourceCount, targetCount);
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            aggCalls.add(aggregateCall.transform(targetMapping));
        }

        final Aggregate newAggregate =
                aggregate.copy(
                        aggregate.getTraitSet(),
                        project.getInput(),
                        newGroupSet,
                        newGroupingSets,
                        aggCalls.build());

        // Add a project if the group set is not in the same order or
        // contains duplicates.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(newAggregate);
        final List<Integer> newKeys =
                Util.transform(
                        aggregate.getGroupSet().asList(),
                        key ->
                                requireNonNull(
                                        map.get(key),
                                        () -> "no value found for key " + key + " in " + map));
        if (!newKeys.equals(newGroupSet.asList())) {
            final List<Integer> posList = new ArrayList<>();
            for (int newKey : newKeys) {
                posList.add(newGroupSet.indexOf(newKey));
            }
            for (int i = newAggregate.getGroupCount();
                    i < newAggregate.getRowType().getFieldCount();
                    i++) {
                posList.add(i);
            }
            relBuilder.project(relBuilder.fields(posList));
        }

        return relBuilder.build();
    }
}
