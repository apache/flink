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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.sources.FilterableTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Planner rule that tries to push a filter into a {@link FilterableTableSource}. */
@Value.Enclosing
public class PushFilterIntoLegacyTableSourceScanRule
        extends RelRule<
                PushFilterIntoLegacyTableSourceScanRule
                        .PushFilterIntoLegacyTableSourceScanRuleConfig> {

    public static final PushFilterIntoLegacyTableSourceScanRule INSTANCE =
            PushFilterIntoLegacyTableSourceScanRule.PushFilterIntoLegacyTableSourceScanRuleConfig
                    .DEFAULT
                    .toRule();

    private PushFilterIntoLegacyTableSourceScanRule(
            PushFilterIntoLegacyTableSourceScanRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        if (!tableConfig.get(
                OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED)) {
            return false;
        }

        Filter filter = call.rel(0);
        if (filter.getCondition() == null) {
            return false;
        }

        LogicalTableScan scan = call.rel(1);
        LegacyTableSourceTable tableSourceTable =
                scan.getTable().unwrap(LegacyTableSourceTable.class);
        if (tableSourceTable != null) {
            if (tableSourceTable.tableSource() instanceof FilterableTableSource) {
                FilterableTableSource<?> filterableSource =
                        (FilterableTableSource<?>) tableSourceTable.tableSource();
                return !filterableSource.isFilterPushedDown();
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalTableScan scan = call.rel(1);
        LegacyTableSourceTable table = scan.getTable().unwrap(LegacyTableSourceTable.class);
        pushFilterIntoScan(call, filter, scan, table);
    }

    private void pushFilterIntoScan(
            RelOptRuleCall call,
            Filter filter,
            LogicalTableScan scan,
            FlinkPreparingTableBase relOptTable) {
        RelBuilder relBuilder = call.builder();
        FlinkContext context = unwrapContext(call);
        int maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(scan);
        Tuple2<Expression[], RexNode[]> extracted =
                RexNodeExtractor.extractConjunctiveConditions(
                        filter.getCondition(),
                        maxCnfNodeCount,
                        filter.getInput().getRowType().getFieldNames(),
                        relBuilder.getRexBuilder(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        TimeZone.getTimeZone(
                                TableConfigUtils.getLocalTimeZone(unwrapTableConfig(scan))));

        Expression[] predicates = extracted._1;
        RexNode[] unconvertedRexNodes = extracted._2;
        if (predicates.length == 0) {
            // no condition can be translated to expression
            return;
        }

        List<Expression> remainingPredicates = new LinkedList<>();
        Arrays.stream(predicates).forEach(e -> remainingPredicates.add(e));

        FlinkPreparingTableBase newRelOptTable =
                applyPredicate(remainingPredicates, relOptTable, relBuilder.getTypeFactory());
        TableSource newTableSource =
                newRelOptTable.unwrap(LegacyTableSourceTable.class).tableSource();
        TableSource oldTableSource = relOptTable.unwrap(LegacyTableSourceTable.class).tableSource();

        if (((FilterableTableSource) newTableSource).isFilterPushedDown()
                && newTableSource.explainSource().equals(oldTableSource.explainSource())) {
            throw new TableException(
                    "Failed to push filter into table source! "
                            + "table source with pushdown capability must override and change "
                            + "explainSource() API to explain the pushdown applied!");
        }

        LogicalTableScan newScan =
                new LogicalTableScan(scan.getCluster(), scan.getTraitSet(), newRelOptTable);

        // check whether framework still need to do a filter
        if (remainingPredicates.isEmpty() && unconvertedRexNodes.length == 0) {
            call.transformTo(newScan);
        } else {
            relBuilder.push(scan);
            ExpressionConverter converter = new ExpressionConverter(relBuilder);
            List<RexNode> remainingConditions =
                    remainingPredicates.stream()
                            .map(expr -> expr.accept(converter))
                            .collect(Collectors.toList());
            Arrays.stream(unconvertedRexNodes).forEach(rexNode -> remainingConditions.add(rexNode));
            RexNode remainingCondition = remainingConditions.stream().reduce(relBuilder::and).get();
            RexNode simplifiedRemainingCondition =
                    FlinkRexUtil.simplify(
                            relBuilder.getRexBuilder(),
                            remainingCondition,
                            filter.getCluster().getPlanner().getExecutor());
            Filter newFilter =
                    filter.copy(filter.getTraitSet(), newScan, simplifiedRemainingCondition);
            call.transformTo(newFilter);
        }
    }

    private FlinkPreparingTableBase applyPredicate(
            List<Expression> predicates,
            FlinkPreparingTableBase relOptTable,
            RelDataTypeFactory typeFactory) {
        int originPredicatesSize = predicates.size();
        LegacyTableSourceTable tableSourceTable = relOptTable.unwrap(LegacyTableSourceTable.class);
        FilterableTableSource filterableSource =
                (FilterableTableSource<?>) tableSourceTable.tableSource();
        TableSource newTableSource = filterableSource.applyPredicate(predicates);
        int updatedPredicatesSize = predicates.size();
        FlinkStatistic statistic = tableSourceTable.getStatistic();
        FlinkStatistic newStatistic;
        if (originPredicatesSize == updatedPredicatesSize) {
            // Keep all Statistics if no predicates can be pushed down
            newStatistic = statistic;
        } else if (statistic == FlinkStatistic.UNKNOWN()) {
            newStatistic = statistic;
        } else {
            // Remove tableStats after predicates pushed down
            newStatistic = FlinkStatistic.builder().statistic(statistic).tableStats(null).build();
        }
        return tableSourceTable.copy(newTableSource, newStatistic);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface PushFilterIntoLegacyTableSourceScanRuleConfig extends RelRule.Config {
        PushFilterIntoLegacyTableSourceScanRule.PushFilterIntoLegacyTableSourceScanRuleConfig
                DEFAULT =
                        ImmutablePushFilterIntoLegacyTableSourceScanRule
                                .PushFilterIntoLegacyTableSourceScanRuleConfig.builder()
                                .build()
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(Filter.class)
                                                        .oneInput(
                                                                b1 ->
                                                                        b1.operand(
                                                                                        LogicalTableScan
                                                                                                .class)
                                                                                .noInputs()))
                                .withDescription("PushFilterIntoLegacyTableSourceScanRule");

        @Override
        default PushFilterIntoLegacyTableSourceScanRule toRule() {
            return new PushFilterIntoLegacyTableSourceScanRule(this);
        }
    }
}
