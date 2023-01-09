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
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.rules.logical.SimplifyFilterConditionRule;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
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
import org.apache.calcite.rex.RexBuilder;
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

/** Utils for delete statement. */
public class DeletePushDownUtil {

    public static Optional<DynamicTableSink> getDynamicTableSink(
            ContextResolvedTable contextResolvedTable,
            LogicalTableModify tableModify,
            CatalogManager catalogManager) {
        if (contextResolvedTable.isAnonymous()) {
            return Optional.empty();
        }
        final FlinkContext context = ShortcutUtils.unwrapContext(tableModify.getCluster());

        CatalogBaseTable catalogBaseTable = contextResolvedTable.getTable();
        // only consider DynamicTableSink
        if (catalogBaseTable instanceof CatalogTable) {
            ResolvedCatalogTable resolvedTable = contextResolvedTable.getResolvedTable();
            Optional<Catalog> optionalCatalog = contextResolvedTable.getCatalog();
            ObjectIdentifier objectIdentifier = contextResolvedTable.getIdentifier();
            boolean isTemporary = contextResolvedTable.isTemporary();
            if (!TableFactoryUtil.isLegacyConnectorOptions(
                    catalogManager.getCatalog(objectIdentifier.getCatalogName()).orElse(null),
                    context.getTableConfig(),
                    !context.isBatchMode(),
                    objectIdentifier,
                    resolvedTable,
                    isTemporary)) {
                DynamicTableSinkFactory dynamicTableSinkFactory = null;
                if (optionalCatalog.isPresent()
                        && optionalCatalog.get().getFactory().isPresent()
                        && optionalCatalog.get().getFactory().get()
                                instanceof DynamicTableSinkFactory) {
                    dynamicTableSinkFactory =
                            (DynamicTableSinkFactory) optionalCatalog.get().getFactory().get();
                }

                if (dynamicTableSinkFactory == null) {
                    Optional<DynamicTableSinkFactory> factoryFromModule =
                            context.getModuleManager().getFactory((Module::getTableSinkFactory));
                    dynamicTableSinkFactory = factoryFromModule.orElse(null);
                }
                DynamicTableSink tableSink =
                        FactoryUtil.createDynamicTableSink(
                                dynamicTableSinkFactory,
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

    public static Optional<List<ResolvedExpression>> extractFilters(
            LogicalTableModify tableModify) {
        FlinkContext context = ShortcutUtils.unwrapContext(tableModify.getCluster());
        RelNode input = tableModify.getInput().getInput(0);
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

        filter = prepareFilter(filter);

        List<ResolvedExpression> resolveExpression = resolveFilter(context, filter);
        return Optional.ofNullable(resolveExpression);
    }

    private static Tuple2<RexNode[], RexNode[]> extractPredicates(
            String[] inputNames, RexNode filterExpression, Filter filter, RexBuilder rexBuilder) {
        FlinkContext context = ShortcutUtils.unwrapContext(filter);
        int maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(filter);
        RexNodeToExpressionConverter converter =
                new RexNodeToExpressionConverter(
                        rexBuilder,
                        inputNames,
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        TimeZone.getTimeZone(
                                TableConfigUtils.getLocalTimeZone(context.getTableConfig())));

        return RexNodeExtractor.extractConjunctiveConditions(
                filterExpression, maxCnfNodeCount, rexBuilder, converter);
    }

    private static Filter prepareFilter(Filter filter) {
        FilterReduceExpressions filterReduceExpressions = FilterReduceExpressions.INSTANCE;
        SimplifyFilterConditionRule simplifyFilterConditionRule =
                SimplifyFilterConditionRule.INSTANCE();
        int maxIteration = 5;

        boolean changed = true;
        int iteration = 1;
        while (changed && iteration <= maxIteration) {
            changed = false;
            // first apply the rule to reduce condition in filter
            RexNode newCondition = filter.getCondition();
            List<RexNode> expList = new ArrayList<>();
            expList.add(newCondition);
            if (filterReduceExpressions.reduce(filter, expList)) {
                newCondition = expList.get(0);
                changed = true;
            }
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

    private static List<ResolvedExpression> resolveFilter(FlinkContext context, Filter filter) {
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                extractPredicates(
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
                                                        "%s can not be converted to Expression, please make sure %s can accept %s.",
                                                        p,
                                                        DeletePushDownUtil.class.getSimpleName(),
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

    private static class FilterReduceExpressions
            extends ReduceExpressionsRule<ReduceExpressionsRule.Config> {
        private static final ReduceExpressionsRule.Config config =
                FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT;
        private static final FilterReduceExpressions INSTANCE = new FilterReduceExpressions();

        public FilterReduceExpressions() {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall relOptRuleCall) {}

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
}
