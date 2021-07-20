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
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;

/** Base class for rules that push down filters into table scan. */
public abstract class PushFilterIntoSourceScanRuleBase extends RelOptRule {
    public PushFilterIntoSourceScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig config = ShortcutUtils.unwrapContext(call.getPlanner()).getTableConfig();
        return config.getConfiguration()
                .getBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED);
    }

    protected List<RexNode> convertExpressionToRexNode(
            List<ResolvedExpression> expressions, RelBuilder relBuilder) {
        ExpressionConverter exprConverter = new ExpressionConverter(relBuilder);
        return expressions.stream().map(e -> e.accept(exprConverter)).collect(Collectors.toList());
    }

    protected RexNode createRemainingCondition(
            RelBuilder relBuilder,
            List<ResolvedExpression> expressions,
            RexNode[] unconvertedPredicates) {
        List<RexNode> remainingPredicates = convertExpressionToRexNode(expressions, relBuilder);
        remainingPredicates.addAll(Arrays.asList(unconvertedPredicates));
        return relBuilder.and(remainingPredicates);
    }

    /**
     * Resolves filters using the underlying sources {@link SupportsFilterPushDown} and creates a
     * new {@link TableSourceTable} with the supplied predicates.
     *
     * @param convertiblePredicates Predicates to resolve
     * @param oldTableSourceTable TableSourceTable to copy
     * @param scan Underlying table scan to push to
     * @param relBuilder Builder to push the scan to
     * @return A tuple, constituting of the resolved filters and the newly created {@link
     *     TableSourceTable}
     */
    protected Tuple2<SupportsFilterPushDown.Result, TableSourceTable>
            resolveFiltersAndCreateTableSourceTable(
                    RexNode[] convertiblePredicates,
                    TableSourceTable oldTableSourceTable,
                    TableScan scan,
                    RelBuilder relBuilder) {
        // record size before applyFilters for update statistics
        int originPredicatesSize = convertiblePredicates.length;

        // update DynamicTableSource
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

        return new Tuple2<>(result, newTableSourceTable);
    }

    protected Tuple2<RexNode[], RexNode[]> extractPredicates(
            String[] inputNames, RexNode filterExpression, TableScan scan, RexBuilder rexBuilder) {
        FlinkContext context = ShortcutUtils.unwrapContext(scan);
        int maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan);
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        rexBuilder,
                        inputNames,
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        TimeZone.getTimeZone(context.getTableConfig().getLocalTimeZone()));

        return RexNodeExtractor.extractConjunctiveConditions(
                filterExpression, maxCnfNodeCount, rexBuilder, converter);
    }

    /**
     * Determines wether we can pushdown the filter into the source. we can not push filter twice,
     * make sure FilterPushDownSpec has not been assigned as a capability.
     *
     * @param tableSourceTable Table scan to attempt to push into
     * @return Whether we can push or not
     */
    protected boolean canPushdownFilter(TableSourceTable tableSourceTable) {
        return tableSourceTable != null
                && tableSourceTable.tableSource() instanceof SupportsFilterPushDown
                && Arrays.stream(tableSourceTable.abilitySpecs())
                        .noneMatch(spec -> spec instanceof FilterPushDownSpec);
    }

    protected FlinkStatistic getNewFlinkStatistic(
            TableSourceTable tableSourceTable,
            int originPredicatesSize,
            int updatedPredicatesSize) {
        FlinkStatistic oldStatistic = tableSourceTable.getStatistic();
        FlinkStatistic newStatistic;
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

    protected String[] getNewExtraDigests(List<ResolvedExpression> acceptedFilters) {
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
