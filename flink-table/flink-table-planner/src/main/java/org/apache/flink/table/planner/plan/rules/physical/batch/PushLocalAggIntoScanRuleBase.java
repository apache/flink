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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.source.AggregatePushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that tries to push a local aggregator into an {@link BatchPhysicalTableSourceScan}
 * whose table is a {@link TableSourceTable} with a source supporting {@link
 * SupportsAggregatePushDown}.
 *
 * <p>The aggregate push down does not support a number of more complex statements at present:
 *
 * <ul>
 *   <li>complex grouping operations such as ROLLUP, CUBE, or GROUPING SETS.
 *   <li>expressions inside the aggregation function call: such as sum(a * b).
 *   <li>aggregations with ordering.
 *   <li>aggregations with filter.
 * </ul>
 */
public abstract class PushLocalAggIntoScanRuleBase extends RelOptRule {

    public PushLocalAggIntoScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    protected boolean canPushDown(
            RelOptRuleCall call,
            BatchPhysicalGroupAggregateBase aggregate,
            BatchPhysicalTableSourceScan tableSourceScan) {
        TableConfig tableConfig = ShortcutUtils.unwrapContext(call.getPlanner()).getTableConfig();
        if (!tableConfig
                .getConfiguration()
                .getBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED)) {
            return false;
        }

        if (aggregate.isFinal() || aggregate.getAggCallList().isEmpty()) {
            return false;
        }
        List<AggregateCall> aggCallList =
                JavaScalaConversionUtil.toJava(aggregate.getAggCallList());
        for (AggregateCall aggCall : aggCallList) {
            if (aggCall.isDistinct()
                    || aggCall.isApproximate()
                    || aggCall.getArgList().size() > 1
                    || aggCall.hasFilter()
                    || !aggCall.getCollation().getFieldCollations().isEmpty()) {
                return false;
            }
        }
        TableSourceTable tableSourceTable = tableSourceScan.tableSourceTable();
        // we can not push aggregates twice
        return tableSourceTable != null
                && tableSourceTable.tableSource() instanceof SupportsAggregatePushDown
                && Arrays.stream(tableSourceTable.abilitySpecs())
                        .noneMatch(spec -> spec instanceof AggregatePushDownSpec);
    }

    protected void pushLocalAggregateIntoScan(
            RelOptRuleCall call,
            BatchPhysicalGroupAggregateBase localAgg,
            BatchPhysicalTableSourceScan oldScan) {
        pushLocalAggregateIntoScan(call, localAgg, oldScan, null);
    }

    protected void pushLocalAggregateIntoScan(
            RelOptRuleCall call,
            BatchPhysicalGroupAggregateBase localAgg,
            BatchPhysicalTableSourceScan oldScan,
            int[] calcRefFields) {
        RowType inputType = FlinkTypeFactory.toLogicalRowType(oldScan.getRowType());
        List<int[]> groupingSets =
                Collections.singletonList(
                        ArrayUtils.addAll(localAgg.grouping(), localAgg.auxGrouping()));
        List<AggregateCall> aggCallList = JavaScalaConversionUtil.toJava(localAgg.getAggCallList());

        // map arg index in aggregate to field index in scan through referred fields by calc.
        if (calcRefFields != null) {
            groupingSets = translateGroupingArgIndex(groupingSets, calcRefFields);
            aggCallList = translateAggCallArgIndex(aggCallList, calcRefFields);
        }

        RowType producedType = FlinkTypeFactory.toLogicalRowType(localAgg.getRowType());

        TableSourceTable oldTableSourceTable = oldScan.tableSourceTable();
        DynamicTableSource newTableSource = oldScan.tableSource().copy();

        boolean isPushDownSuccess =
                AggregatePushDownSpec.apply(
                        inputType,
                        groupingSets,
                        aggCallList,
                        producedType,
                        newTableSource,
                        SourceAbilityContext.from(oldScan));

        if (!isPushDownSuccess) {
            // aggregate push down failed, just return without changing any nodes.
            return;
        }

        // create new source table with new spec and statistic.
        AggregatePushDownSpec aggregatePushDownSpec =
                new AggregatePushDownSpec(inputType, groupingSets, aggCallList, producedType);

        TableSourceTable newTableSourceTable =
                oldTableSourceTable
                        .copy(
                                newTableSource,
                                localAgg.getRowType(),
                                new SourceAbilitySpec[] {aggregatePushDownSpec})
                        .copy(FlinkStatistic.UNKNOWN());

        // transform to new nodes.
        BatchPhysicalTableSourceScan newScan =
                oldScan.copy(oldScan.getTraitSet(), newTableSourceTable);
        BatchPhysicalExchange oldExchange = call.rel(0);
        BatchPhysicalExchange newExchange =
                oldExchange.copy(oldExchange.getTraitSet(), newScan, oldExchange.getDistribution());
        call.transformTo(newExchange);
    }

    protected boolean isProjectionNotPushedDown(BatchPhysicalTableSourceScan tableSourceScan) {
        TableSourceTable tableSourceTable = tableSourceScan.tableSourceTable();
        return tableSourceTable != null
                && Arrays.stream(tableSourceTable.abilitySpecs())
                        .noneMatch(spec -> spec instanceof ProjectPushDownSpec);
    }

    /**
     * Currently, we only supports to push down aggregate above calc which has input ref only.
     *
     * @param calc BatchPhysicalCalc
     * @return true if OK to be pushed down
     */
    protected boolean isInputRefOnly(BatchPhysicalCalc calc) {
        RexProgram program = calc.getProgram();

        // check if condition exists. All filters should have been pushed down.
        if (program.getCondition() != null) {
            return false;
        }

        return !program.getProjectList().isEmpty()
                && program.getProjectList().stream()
                        .map(calc.getProgram()::expandLocalRef)
                        .allMatch(RexInputRef.class::isInstance);
    }

    protected int[] getRefFiledIndex(BatchPhysicalCalc calc) {
        List<RexNode> projects =
                calc.getProgram().getProjectList().stream()
                        .map(calc.getProgram()::expandLocalRef)
                        .collect(Collectors.toList());

        return RexNodeExtractor.extractRefInputFields(projects);
    }

    protected List<int[]> translateGroupingArgIndex(List<int[]> groupingSets, int[] refFields) {
        List<int[]> newGroupingSets = new ArrayList<>();
        groupingSets.forEach(
                grouping -> {
                    int[] newGrouping = new int[grouping.length];
                    for (int i = 0; i < grouping.length; i++) {
                        int argIndex = grouping[i];
                        newGrouping[i] = refFields[argIndex];
                    }
                    newGroupingSets.add(newGrouping);
                });

        return newGroupingSets;
    }

    protected List<AggregateCall> translateAggCallArgIndex(
            List<AggregateCall> aggCallList, int[] refFields) {
        List<AggregateCall> newAggCallList = new ArrayList<>();
        aggCallList.forEach(
                aggCall -> {
                    List<Integer> argList = new ArrayList<>();
                    for (int i = 0; i < aggCall.getArgList().size(); i++) {
                        int argIndex = aggCall.getArgList().get(i);
                        argList.add(refFields[argIndex]);
                    }
                    newAggCallList.add(aggCall.copy(argList, aggCall.filterArg, aggCall.collation));
                });

        return newAggCallList;
    }
}
