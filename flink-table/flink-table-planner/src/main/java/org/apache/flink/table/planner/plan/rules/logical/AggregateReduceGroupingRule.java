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

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Planner rule that reduces unless grouping columns.
 *
 * <p>Find (minimum) unique group for the grouping columns, and use it as new grouping columns.
 */
@Value.Enclosing
public class AggregateReduceGroupingRule
        extends RelRule<AggregateReduceGroupingRule.AggregateReduceGroupingRuleConfig> {

    public static final AggregateReduceGroupingRule INSTANCE =
            AggregateReduceGroupingRule.AggregateReduceGroupingRuleConfig.DEFAULT.toRule();

    protected AggregateReduceGroupingRule(AggregateReduceGroupingRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate agg = call.rel(0);
        return agg.getGroupCount() > 1 && agg.getGroupType() == Group.SIMPLE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate agg = call.rel(0);
        RelDataType aggRowType = agg.getRowType();
        RelNode input = agg.getInput();
        RelDataType inputRowType = input.getRowType();
        ImmutableBitSet originalGrouping = agg.getGroupSet();
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery());
        ImmutableBitSet newGrouping = fmq.getUniqueGroups(input, originalGrouping);
        ImmutableBitSet uselessGrouping = originalGrouping.except(newGrouping);
        if (uselessGrouping.isEmpty()) {
            return;
        }

        // new agg: new grouping + aggCalls for dropped grouping + original aggCalls
        Map<Integer, Integer> indexOldToNewMap = new HashMap<>();
        List<Integer> newGroupingList = newGrouping.toList();
        int idxOfNewGrouping = 0;
        int idxOfAggCallsForDroppedGrouping = newGroupingList.size();
        int index = 0;
        for (int column : originalGrouping) {
            if (newGroupingList.contains(column)) {
                indexOldToNewMap.put(index, idxOfNewGrouping);
                idxOfNewGrouping++;
            } else {
                indexOldToNewMap.put(index, idxOfAggCallsForDroppedGrouping);
                idxOfAggCallsForDroppedGrouping++;
            }
            index++;
        }

        assert (indexOldToNewMap.size() == originalGrouping.cardinality());

        // the indices of aggCalls (or NamedProperties for WindowAggregate) do not change
        for (int i = originalGrouping.cardinality(); i < aggRowType.getFieldCount(); i++) {
            indexOldToNewMap.put(i, i);
        }

        List<AggregateCall> aggCallsForDroppedGrouping =
                uselessGrouping.asList().stream()
                        .map(
                                column -> {
                                    RelDataType fieldType =
                                            inputRowType.getFieldList().get(column).getType();
                                    String fieldName = inputRowType.getFieldNames().get(column);
                                    return AggregateCall.create(
                                            FlinkSqlOperatorTable.AUXILIARY_GROUP,
                                            false,
                                            false,
                                            false,
                                            ImmutableList.of(),
                                            ImmutableList.of(column),
                                            -1,
                                            null,
                                            RelCollations.EMPTY,
                                            fieldType,
                                            fieldName);
                                })
                        .collect(Collectors.toList());

        aggCallsForDroppedGrouping.addAll(agg.getAggCallList());
        Aggregate newAgg =
                agg.copy(
                        agg.getTraitSet(),
                        input,
                        newGrouping,
                        ImmutableList.of(newGrouping),
                        aggCallsForDroppedGrouping);
        RelBuilder builder = call.builder();
        builder.push(newAgg);
        List<RexNode> projects =
                IntStream.range(0, newAgg.getRowType().getFieldCount())
                        .mapToObj(
                                i -> {
                                    Integer refIndex = indexOldToNewMap.get(i);
                                    if (refIndex == null) {
                                        throw new IllegalArgumentException("Illegal index: " + i);
                                    }
                                    return builder.field(refIndex);
                                })
                        .collect(Collectors.toList());
        builder.project(projects, aggRowType.getFieldNames());
        call.transformTo(builder.build());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface AggregateReduceGroupingRuleConfig extends RelRule.Config {
        AggregateReduceGroupingRule.AggregateReduceGroupingRuleConfig DEFAULT =
                ImmutableAggregateReduceGroupingRule.AggregateReduceGroupingRuleConfig.builder()
                        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .operandSupplier(b0 -> b0.operand(Aggregate.class).anyInputs())
                        .description("AggregateReduceGroupingRule")
                        .build();

        @Override
        default AggregateReduceGroupingRule toRule() {
            return new AggregateReduceGroupingRule(this);
        }
    }
}
