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
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.plan.abilities.source.AggregatePushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.AggregateInfo;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Planner rule that tries to push a local aggregator into an {@link BatchPhysicalTableSourceScan}
 * which table is a {@link TableSourceTable}. And the table source in the table is a {@link
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
public abstract class PushLocalAggIntoTableSourceScanRuleBase extends RelOptRule {

    public PushLocalAggIntoTableSourceScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    protected boolean isMatch(
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

        if (aggregate.isFinal() || aggregate.getAggCallList().size() < 1) {
            return false;
        }
        List<AggregateCall> aggCallList =
                JavaConverters.seqAsJavaListConverter(aggregate.getAggCallList()).asJava();
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
        RelDataType originalInputRowType = oldScan.deriveRowType();
        AggregateInfoList aggInfoList =
                AggregateUtil.transformToBatchAggregateInfoList(
                        FlinkTypeFactory.toLogicalRowType(localAgg.getInput().getRowType()),
                        localAgg.getAggCallList(),
                        null,
                        null);
        if (aggInfoList.aggInfos().length == 0) {
            // no agg function need to be pushed down
            return;
        }

        List<int[]> groupingSets = Collections.singletonList(localAgg.grouping());
        List<AggregateExpression> aggExpressions =
                buildAggregateExpressions(originalInputRowType, aggInfoList);
        RelDataType relDataType = localAgg.deriveRowType();

        TableSourceTable oldTableSourceTable = oldScan.tableSourceTable();
        DynamicTableSource newTableSource = oldScan.tableSource().copy();

        boolean isPushDownSuccess =
                AggregatePushDownSpec.apply(
                        groupingSets,
                        aggExpressions,
                        (RowType) FlinkTypeFactory.toLogicalType(relDataType),
                        newTableSource);

        if (!isPushDownSuccess) {
            // aggregate push down failed, just return without changing any nodes.
            return;
        }

        FlinkStatistic newFlinkStatistic = getNewFlinkStatistic(oldTableSourceTable);
        String[] newExtraDigests =
                getNewExtraDigests(originalInputRowType, localAgg.grouping(), aggExpressions);
        AggregatePushDownSpec aggregatePushDownSpec =
                new AggregatePushDownSpec(
                        groupingSets,
                        aggExpressions,
                        (RowType) FlinkTypeFactory.toLogicalType(relDataType));
        TableSourceTable newTableSourceTable =
                oldTableSourceTable
                        .copy(
                                newTableSource,
                                newFlinkStatistic,
                                newExtraDigests,
                                new SourceAbilitySpec[] {aggregatePushDownSpec})
                        .copy(relDataType);
        BatchPhysicalTableSourceScan newScan =
                oldScan.copy(oldScan.getTraitSet(), newTableSourceTable);
        BatchPhysicalExchange oldExchange = call.rel(0);
        BatchPhysicalExchange newExchange =
                oldExchange.copy(oldExchange.getTraitSet(), newScan, oldExchange.getDistribution());
        call.transformTo(newExchange);
    }

    private List<AggregateExpression> buildAggregateExpressions(
            RelDataType originalInputRowType, AggregateInfoList aggInfoList) {
        List<AggregateExpression> aggExpressions = new ArrayList<>();
        for (AggregateInfo aggInfo : aggInfoList.aggInfos()) {
            List<FieldReferenceExpression> arguments = new ArrayList<>(1);
            for (int argIndex : aggInfo.argIndexes()) {
                DataType argType =
                        TypeConversions.fromLogicalToDataType(
                                FlinkTypeFactory.toLogicalType(
                                        originalInputRowType
                                                .getFieldList()
                                                .get(argIndex)
                                                .getType()));
                FieldReferenceExpression field =
                        new FieldReferenceExpression(
                                originalInputRowType.getFieldNames().get(argIndex),
                                argType,
                                argIndex,
                                argIndex);
                arguments.add(field);
            }
            if (aggInfo.function() instanceof AvgAggFunction) {
                Tuple2<Sum0AggFunction, CountAggFunction> sum0AndCountFunction =
                        AggregateUtil.deriveSumAndCountFromAvg(aggInfo.function());
                AggregateExpression sum0Expression =
                        new AggregateExpression(
                                sum0AndCountFunction._1(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(sum0Expression);
                AggregateExpression countExpression =
                        new AggregateExpression(
                                sum0AndCountFunction._2(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(countExpression);
            } else {
                AggregateExpression aggregateExpression =
                        new AggregateExpression(
                                aggInfo.function(),
                                arguments,
                                null,
                                aggInfo.externalResultType(),
                                aggInfo.agg().isDistinct(),
                                aggInfo.agg().isApproximate(),
                                aggInfo.agg().ignoreNulls());
                aggExpressions.add(aggregateExpression);
            }
        }
        return aggExpressions;
    }

    private FlinkStatistic getNewFlinkStatistic(TableSourceTable tableSourceTable) {
        FlinkStatistic oldStatistic = tableSourceTable.getStatistic();
        FlinkStatistic newStatistic;
        if (oldStatistic == FlinkStatistic.UNKNOWN()) {
            newStatistic = oldStatistic;
        } else {
            // Remove tableStats after all of aggregate have been pushed down
            newStatistic =
                    FlinkStatistic.builder().statistic(oldStatistic).tableStats(null).build();
        }
        return newStatistic;
    }

    private String[] getNewExtraDigests(
            RelDataType originalInputRowType,
            int[] grouping,
            List<AggregateExpression> aggregateExpressions) {
        String extraDigest;
        String groupingStr = "null";
        if (grouping.length > 0) {
            groupingStr =
                    Arrays.stream(grouping)
                            .mapToObj(index -> originalInputRowType.getFieldNames().get(index))
                            .collect(Collectors.joining(","));
        }
        String aggFunctionsStr = "null";
        if (aggregateExpressions.size() > 0) {
            aggFunctionsStr =
                    aggregateExpressions.stream()
                            .map(AggregateExpression::asSummaryString)
                            .collect(Collectors.joining(","));
        }
        extraDigest =
                "aggregates=[grouping=["
                        + groupingStr
                        + "], aggFunctions=["
                        + aggFunctionsStr
                        + "]]";
        return new String[] {extraDigest};
    }
}
