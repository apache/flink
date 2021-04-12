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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;

/**
 * Planner rule that tries to push a filter into a {@link LogicalTableScan}, which table is a {@link
 * TableSourceTable}. And the table source in the table is a {@link SupportsFilterPushDown}.
 */
public class PushFilterIntoTableSourceScanRule extends RelOptRule {
    public static final PushFilterIntoTableSourceScanRule INSTANCE =
            new PushFilterIntoTableSourceScanRule();

    public PushFilterIntoTableSourceScanRule() {
        super(
                operand(Filter.class, operand(LogicalTableScan.class, none())),
                "PushFilterIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig config = ShortcutUtils.unwrapContext(call.getPlanner()).getTableConfig();
        if (!config.getConfiguration()
                .getBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED)) {
            return false;
        }

        Filter filter = call.rel(0);
        if (filter.getCondition() == null) {
            return false;
        }

        LogicalTableScan scan = call.rel(1);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        // we can not push filter twice
        return tableSourceTable != null
                && tableSourceTable.tableSource() instanceof SupportsFilterPushDown
                && Arrays.stream(tableSourceTable.abilitySpecs())
                        .noneMatch(spec -> spec instanceof FilterPushDownSpec);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalTableScan scan = call.rel(1);
        TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
        pushFilterIntoScan(call, filter, scan, table);
    }

    private void pushFilterIntoScan(
            RelOptRuleCall call,
            Filter filter,
            LogicalTableScan scan,
            FlinkPreparingTableBase relOptTable) {

        RelBuilder relBuilder = call.builder();
        FlinkContext context = ShortcutUtils.unwrapContext(scan);
        int maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan);
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        relBuilder.getRexBuilder(),
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        TimeZone.getTimeZone(context.getTableConfig().getLocalTimeZone()));

        Tuple2<RexNode[], RexNode[]> tuple2 =
                RexNodeExtractor.extractConjunctiveConditions(
                        filter.getCondition(),
                        maxCnfNodeCount,
                        relBuilder.getRexBuilder(),
                        converter);
        RexNode[] convertiblePredicates = tuple2._1;
        RexNode[] unconvertedPredicates = tuple2._2;
        if (convertiblePredicates.length == 0) {
            // no condition can be translated to expression
            return;
        }

        // record size before applyFilters for update statistics
        int originPredicatesSize = convertiblePredicates.length;

        // update DynamicTableSource
        TableSourceTable oldTableSourceTable = relOptTable.unwrap(TableSourceTable.class);
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();

        SupportsFilterPushDown.Result result =
                FilterPushDownSpec.apply(
                        Arrays.asList(convertiblePredicates),
                        newTableSource,
                        SourceAbilityContext.from(scan));

        relBuilder.push(scan);
        List<RexNode> acceptedPredicates =
                convertExpressionToRexNode(result.getAcceptedFilters(), relBuilder);
        FilterPushDownSpec filterPushDownSpec = new FilterPushDownSpec(acceptedPredicates);

        // record size after applyFilters for update statistics
        int updatedPredicatesSize = result.getRemainingFilters().size();
        // set the newStatistic newTableSource and extraDigests
        TableSourceTable newTableSourceTable =
                oldTableSourceTable.copy(
                        newTableSource,
                        getNewFlinkStatistic(
                                oldTableSourceTable, originPredicatesSize, updatedPredicatesSize),
                        getNewExtraDigests(result.getAcceptedFilters()),
                        new SourceAbilitySpec[] {filterPushDownSpec});
        TableScan newScan =
                LogicalTableScan.create(scan.getCluster(), newTableSourceTable, scan.getHints());
        // check whether framework still need to do a filter
        if (result.getRemainingFilters().isEmpty() && unconvertedPredicates.length == 0) {
            call.transformTo(newScan);
        } else {
            List<RexNode> remainingPredicates =
                    convertExpressionToRexNode(result.getRemainingFilters(), relBuilder);
            remainingPredicates.addAll(Arrays.asList(unconvertedPredicates));
            RexNode remainingCondition = relBuilder.and(remainingPredicates);
            Filter newFilter = filter.copy(filter.getTraitSet(), newScan, remainingCondition);
            call.transformTo(newFilter);
        }
    }

    private List<RexNode> convertExpressionToRexNode(
            List<ResolvedExpression> expressions, RelBuilder relBuilder) {
        ExpressionConverter exprConverter = new ExpressionConverter(relBuilder);
        return expressions.stream().map(e -> e.accept(exprConverter)).collect(Collectors.toList());
    }

    private FlinkStatistic getNewFlinkStatistic(
            TableSourceTable tableSourceTable,
            int originPredicatesSize,
            int updatedPredicatesSize) {
        FlinkStatistic oldStatistic = tableSourceTable.getStatistic();
        FlinkStatistic newStatistic = null;
        if (originPredicatesSize == updatedPredicatesSize) {
            // Keep all Statistics if no predicates can be pushed down
            newStatistic = oldStatistic;
        } else if (oldStatistic == FlinkStatistic.UNKNOWN()) {
            newStatistic = oldStatistic;
        } else {
            // Remove tableStats after predicates pushed down
            newStatistic =
                    FlinkStatistic.builder().statistic(oldStatistic).tableStats(null).build();
        }
        return newStatistic;
    }

    private String[] getNewExtraDigests(List<ResolvedExpression> acceptedFilters) {
        final String extraDigest;
        if (!acceptedFilters.isEmpty()) {
            // push filter successfully
            String pushedExpr =
                    acceptedFilters.stream()
                            .reduce(
                                    (l, r) ->
                                            new CallExpression(
                                                    AND, Arrays.asList(l, r), DataTypes.BOOLEAN()))
                            .get()
                            .toString();
            extraDigest = "filter=[" + pushedExpr + "]";
        } else {
            // push filter successfully, but nothing is accepted
            extraDigest = "filter=[]";
        }
        return new String[] {extraDigest};
    }
}
