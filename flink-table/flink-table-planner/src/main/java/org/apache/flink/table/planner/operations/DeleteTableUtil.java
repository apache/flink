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
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.RexNodeExtractor;
import org.apache.flink.table.planner.plan.utils.RexNodeToExpressionConverter;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
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
public class DeleteTableUtil {

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

        // todo: SimplifyFilterConditionRule.EXTENDED
        // todo: FlinkRewriteSubQueryRule don't need
        // todo: FlinkRewriteSubQueryRule.FILTER, don't need
        // todo: FlinkSubQueryRemoveRule.FILTER, don't need
        // todo: SimplifyFilterConditionRule.INSTANCE
        // todo: CoreRules.FILTER_SUB_QUERY_TO_CORRELATE, don't need
        // todo: RemoveUnreachableCoalesceArgumentsRule.FILTER_INSTANCE,
        // todo: CoreRules.FILTER_REDUCE_EXPRESSIONS, need
        // todo: CoreRules.FILTER_MERGE

        LogicalFilter filter = (LogicalFilter) input;
        if (RexUtil.SubQueryFinder.containsSubQuery(filter)) {
            return Optional.empty();
        }

        RexNode newCondition = filter.getCondition();
        List<RexNode> expList = new ArrayList<>();
        expList.add(newCondition);

        if (MyFilterReduceExpressions.reduce(filter, expList)) {
            newCondition = expList.get(0);
        }

        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        RexNode simplifiedCondition =
                FlinkRexUtil.simplify(
                        rexBuilder, newCondition, filter.getCluster().getPlanner().getExecutor());
        newCondition = RexUtil.pullFactors(rexBuilder, simplifiedCondition);
        filter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                extractPredicates(
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        filter.getCondition(),
                        filter,
                        filter.getCluster().getRexBuilder());
        RexNode[] convertiblePredicates = extractedPredicates._1;
        RexNode[] unconvertedPredicates = extractedPredicates._2;
        if (convertiblePredicates.length == 0) {
            // no condition can be translated to expression
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
                                                        p.toString(),
                                                        DeleteTableUtil.class.getSimpleName(),
                                                        p.toString()));
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
        return Optional.of(resolver.resolve(filters));
    }

    private static Tuple2<RexNode[], RexNode[]> extractPredicates(
            String[] inputNames,
            RexNode filterExpression,
            LogicalFilter filter,
            RexBuilder rexBuilder) {
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

    private static class MyFilterReduceExpressions extends ReduceExpressionsRule {

        protected MyFilterReduceExpressions(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall relOptRuleCall) {}

        public static boolean reduce(RelNode rel, List<RexNode> expList) {
            ReduceExpressionsRule.Config config =
                    FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT;
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
