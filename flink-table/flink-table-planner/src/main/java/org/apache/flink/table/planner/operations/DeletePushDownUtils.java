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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.operations.utils.ExecutableOperationUtils;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.rules.logical.SimplifyFilterConditionRule;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import scala.Option;
import scala.Tuple2;

/** A utility class for delete push down. */
public class DeletePushDownUtils {

    /**
     * Get the {@link DynamicTableSink} for the table to be modified. Return Optional.empty() if it
     * can't get the {@link DynamicTableSink}.
     */
    public static Optional<DynamicTableSink> getDynamicTableSink(
            ContextResolvedTable contextResolvedTable,
            LogicalTableModify tableModify,
            CatalogManager catalogManager) {
        final FlinkContext context = ShortcutUtils.unwrapContext(tableModify.getCluster());

        CatalogBaseTable catalogBaseTable = contextResolvedTable.getTable();
        // only consider DynamicTableSink
        if (catalogBaseTable instanceof CatalogTable) {
            ResolvedCatalogTable resolvedTable = contextResolvedTable.getResolvedTable();
            Optional<Catalog> optionalCatalog = contextResolvedTable.getCatalog();
            ObjectIdentifier objectIdentifier = contextResolvedTable.getIdentifier();
            boolean isTemporary = contextResolvedTable.isTemporary();
            // only consider the CatalogTable that doesn't use legacy connector sink option
            if (!contextResolvedTable.isAnonymous()
                    && !TableFactoryUtil.isLegacyConnectorOptions(
                            catalogManager
                                    .getCatalog(objectIdentifier.getCatalogName())
                                    .orElse(null),
                            context.getTableConfig(),
                            !context.isBatchMode(),
                            objectIdentifier,
                            resolvedTable,
                            isTemporary)) {
                // create table dynamic table sink
                DynamicTableSink tableSink =
                        ExecutableOperationUtils.createDynamicTableSink(
                                optionalCatalog.orElse(null),
                                () ->
                                        context.getModuleManager()
                                                .getFactory((Module::getTableSinkFactory)),
                                objectIdentifier,
                                resolvedTable,
                                Collections.emptyMap(),
                                context.getTableConfig(),
                                context.getClassLoader(),
                                contextResolvedTable.isTemporary());
                return Optional.of(tableSink);
            }
        }
        return Optional.empty();
    }

    /**
     * Get the resolved filter expressions from the {@code WHERE} clause in DELETE statement, return
     * Optional.empty() if {@code WHERE} clause contains sub-query.
     */
    public static Optional<List<ResolvedExpression>> getResolvedFilterExpressions(
            LogicalTableModify tableModify) {
        FlinkContext context = ShortcutUtils.unwrapContext(tableModify.getCluster());
        RelNode input = tableModify.getInput().getInput(0);
        // no WHERE clause, return an empty list
        if (input instanceof LogicalTableScan) {
            return Optional.of(Collections.emptyList());
        }
        if (!(input instanceof LogicalFilter)) {
            return Optional.empty();
        }

        Filter filter = (Filter) input;
        if (RexUtil.SubQueryFinder.containsSubQuery(filter)) {
            return Optional.empty();
        }

        // optimize the filter
        filter = prepareFilter(filter);

        // resolve the filter to get resolved expression
        List<ResolvedExpression> resolveExpression = resolveFilter(context, filter);
        return Optional.ofNullable(resolveExpression);
    }

    /** Prepare the filter with reducing && simplifying. */
    private static Filter prepareFilter(Filter filter) {
        // we try to reduce and simplify the filter
        ReduceExpressionsRuleProxy reduceExpressionsRuleProxy = ReduceExpressionsRuleProxy.INSTANCE;
        SimplifyFilterConditionRule simplifyFilterConditionRule =
                SimplifyFilterConditionRule.INSTANCE();
        // max iteration num for reducing and simplifying filter,
        // we use 5 as the max iteration num which is same with the iteration num in Flink's plan
        // optimizing.
        int maxIteration = 5;

        boolean changed = true;
        int iteration = 1;
        // iterate until it reaches max iteration num or there's no changes in one iterate
        while (changed && iteration <= maxIteration) {
            changed = false;
            // first apply the rule to reduce condition in filter
            RexNode newCondition = filter.getCondition();
            List<RexNode> expList = new ArrayList<>();
            expList.add(newCondition);
            if (reduceExpressionsRuleProxy.reduce(filter, expList)) {
                // get the new condition
                newCondition = expList.get(0);
                changed = true;
            }
            // create a new filter
            filter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
            // then apply the rule to simplify filter
            Option<Filter> changedFilter =
                    simplifyFilterConditionRule.simplify(filter, new boolean[] {false});
            if (changedFilter.isDefined()) {
                filter = changedFilter.get();
                changed = true;
            }
            iteration += 1;
        }
        return filter;
    }

    /**
     * A proxy for {@link ReduceExpressionsRule}, which enables us to call the method {@link
     * ReduceExpressionsRule#reduceExpressions(RelNode, List, RelOptPredicateList)}.
     */
    private static class ReduceExpressionsRuleProxy
            extends ReduceExpressionsRule<ReduceExpressionsRule.Config> {
        private static final ReduceExpressionsRule.Config config =
                FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT;
        private static final ReduceExpressionsRuleProxy INSTANCE = new ReduceExpressionsRuleProxy();

        public ReduceExpressionsRuleProxy() {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall relOptRuleCall) {
            throw new UnsupportedOperationException("This shouldn't be called");
        }

        private boolean reduce(RelNode rel, List<RexNode> expList) {
            return reduceExpressions(
                    rel,
                    expList,
                    RelOptPredicateList.EMPTY,
                    true,
                    config.matchNullability(),
                    config.treatDynamicCallsAsConstant());
        }
    }

    /** Return the ResolvedExpression according to Filter. */
    private static List<ResolvedExpression> resolveFilter(FlinkContext context, Filter filter) {
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                FlinkRexUtil.extractPredicates(
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        filter.getCondition(),
                        filter,
                        filter.getCluster().getRexBuilder());
        RexNode[] convertiblePredicates = extractedPredicates._1;
        RexNode[] unconvertedPredicates = extractedPredicates._2;
        if (unconvertedPredicates.length != 0) {
            // if contain any unconverted condition, return null
            return null;
        }
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        filter.getCluster().getRexBuilder(),
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        TimeZone.getTimeZone(
                                TableConfigUtils.getLocalTimeZone(context.getTableConfig())));
        List<Expression> filters =
                Arrays.stream(convertiblePredicates)
                        .map(
                                p -> {
                                    Option<ResolvedExpression> expr = p.accept(converter);
                                    if (expr.isDefined()) {
                                        return expr.get();
                                    } else {
                                        throw new TableException(
                                                String.format(
                                                        "%s can not be converted to Expression",
                                                        p));
                                    }
                                })
                        .collect(Collectors.toList());
        ExpressionResolver resolver =
                ExpressionResolver.resolverFor(
                                context.getTableConfig(),
                                context.getClassLoader(),
                                name -> Optional.empty(),
                                context.getFunctionCatalog()
                                        .asLookup(
                                                str -> {
                                                    throw new TableException(
                                                            "We should not need to lookup any expressions at this point");
                                                }),
                                context.getCatalogManager().getDataTypeFactory(),
                                (sqlExpression, inputRowType, outputType) -> {
                                    throw new TableException(
                                            "SQL expression parsing is not supported at this location.");
                                })
                        .build();
        return resolver.resolve(filters);
    }
}
