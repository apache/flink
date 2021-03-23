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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.AggregatedTable;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.FlatAggregateTable;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.OverWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WindowGroupedTable;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.resolver.LookupCallResolver;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.functions.TemporalTableFunctionImpl;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils;
import org.apache.flink.table.operations.utils.OperationExpressionsUtils.CategorizedExpressions;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.lit;

/** Implementation for {@link Table}. */
@Internal
public class TableImpl implements Table {

    private static final AtomicInteger uniqueId = new AtomicInteger(0);

    private final TableEnvironmentInternal tableEnvironment;
    private final QueryOperation operationTree;
    private final OperationTreeBuilder operationTreeBuilder;
    private final LookupCallResolver lookupResolver;

    private String tableName = null;

    public TableEnvironment getTableEnvironment() {
        return tableEnvironment;
    }

    private TableImpl(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation operationTree,
            OperationTreeBuilder operationTreeBuilder,
            LookupCallResolver lookupResolver) {
        this.tableEnvironment = tableEnvironment;
        this.operationTree = operationTree;
        this.operationTreeBuilder = operationTreeBuilder;
        this.lookupResolver = lookupResolver;
    }

    public static TableImpl createTable(
            TableEnvironmentInternal tableEnvironment,
            QueryOperation operationTree,
            OperationTreeBuilder operationTreeBuilder,
            FunctionLookup functionLookup) {
        return new TableImpl(
                tableEnvironment,
                operationTree,
                operationTreeBuilder,
                new LookupCallResolver(functionLookup));
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return operationTree.getResolvedSchema();
    }

    @Override
    public void printSchema() {
        System.out.println(getResolvedSchema());
    }

    @Override
    public QueryOperation getQueryOperation() {
        return operationTree;
    }

    @Override
    public Table select(String fields) {
        return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
    }

    @Override
    public Table select(Expression... fields) {
        List<Expression> expressionsWithResolvedCalls = preprocessExpressions(fields);
        CategorizedExpressions extracted =
                OperationExpressionsUtils.extractAggregationsAndProperties(
                        expressionsWithResolvedCalls);

        if (!extracted.getWindowProperties().isEmpty()) {
            throw new ValidationException("Window properties can only be used on windowed tables.");
        }

        if (!extracted.getAggregations().isEmpty()) {
            QueryOperation aggregate =
                    operationTreeBuilder.aggregate(
                            Collections.emptyList(), extracted.getAggregations(), operationTree);
            return createTable(
                    operationTreeBuilder.project(extracted.getProjections(), aggregate, false));
        } else {
            return createTable(
                    operationTreeBuilder.project(
                            expressionsWithResolvedCalls, operationTree, false));
        }
    }

    @Override
    public TemporalTableFunction createTemporalTableFunction(
            String timeAttribute, String primaryKey) {
        return createTemporalTableFunction(
                ExpressionParser.parseExpression(timeAttribute),
                ExpressionParser.parseExpression(primaryKey));
    }

    @Override
    public TemporalTableFunction createTemporalTableFunction(
            Expression timeAttribute, Expression primaryKey) {
        Expression resolvedTimeAttribute =
                operationTreeBuilder.resolveExpression(timeAttribute, operationTree);
        Expression resolvedPrimaryKey =
                operationTreeBuilder.resolveExpression(primaryKey, operationTree);

        return TemporalTableFunctionImpl.create(
                operationTree, resolvedTimeAttribute, resolvedPrimaryKey);
    }

    @Override
    public Table as(String field, String... fields) {
        final List<Expression> fieldsExprs;
        if (fields.length == 0 && operationTree.getResolvedSchema().getColumnCount() > 1) {
            fieldsExprs = ExpressionParser.parseExpressionList(field);
        } else {
            fieldsExprs = new ArrayList<>();
            fieldsExprs.add(lit(field));
            for (String extraField : fields) {
                fieldsExprs.add(lit(extraField));
            }
        }
        return createTable(operationTreeBuilder.alias(fieldsExprs, operationTree));
    }

    @Override
    public Table as(Expression... fields) {
        return createTable(operationTreeBuilder.alias(Arrays.asList(fields), operationTree));
    }

    @Override
    public Table filter(String predicate) {
        return filter(ExpressionParser.parseExpression(predicate));
    }

    @Override
    public Table filter(Expression predicate) {
        Expression resolvedCallPredicate = predicate.accept(lookupResolver);
        return createTable(operationTreeBuilder.filter(resolvedCallPredicate, operationTree));
    }

    @Override
    public Table where(String predicate) {
        return filter(predicate);
    }

    @Override
    public Table where(Expression predicate) {
        return filter(predicate);
    }

    @Override
    public GroupedTable groupBy(String fields) {
        return new GroupedTableImpl(this, ExpressionParser.parseExpressionList(fields));
    }

    @Override
    public GroupedTable groupBy(Expression... fields) {
        return new GroupedTableImpl(this, Arrays.asList(fields));
    }

    @Override
    public Table distinct() {
        return createTable(operationTreeBuilder.distinct(operationTree));
    }

    @Override
    public Table join(Table right) {
        return joinInternal(right, Optional.empty(), JoinType.INNER);
    }

    @Override
    public Table join(Table right, String joinPredicate) {
        return join(right, ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table join(Table right, Expression joinPredicate) {
        return joinInternal(right, Optional.of(joinPredicate), JoinType.INNER);
    }

    @Override
    public Table leftOuterJoin(Table right) {
        return joinInternal(right, Optional.empty(), JoinType.LEFT_OUTER);
    }

    @Override
    public Table leftOuterJoin(Table right, String joinPredicate) {
        return leftOuterJoin(right, ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table leftOuterJoin(Table right, Expression joinPredicate) {
        return joinInternal(right, Optional.of(joinPredicate), JoinType.LEFT_OUTER);
    }

    @Override
    public Table rightOuterJoin(Table right, String joinPredicate) {
        return rightOuterJoin(right, ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table rightOuterJoin(Table right, Expression joinPredicate) {
        return joinInternal(right, Optional.of(joinPredicate), JoinType.RIGHT_OUTER);
    }

    @Override
    public Table fullOuterJoin(Table right, String joinPredicate) {
        return fullOuterJoin(right, ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table fullOuterJoin(Table right, Expression joinPredicate) {
        return joinInternal(right, Optional.of(joinPredicate), JoinType.FULL_OUTER);
    }

    private TableImpl joinInternal(
            Table right, Optional<Expression> joinPredicate, JoinType joinType) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.join(
                        this.operationTree,
                        right.getQueryOperation(),
                        joinType,
                        joinPredicate,
                        false));
    }

    private void verifyTableCompatible(Table right) {
        // check that the TableEnvironment of right table is not null
        // and right table belongs to the same TableEnvironment
        if (((TableImpl) right).getTableEnvironment() != this.tableEnvironment) {
            throw new ValidationException(
                    "Only tables from the same TableEnvironment can be joined.");
        }
    }

    @Override
    public Table joinLateral(String tableFunctionCall) {
        return joinLateral(ExpressionParser.parseExpression(tableFunctionCall));
    }

    @Override
    public Table joinLateral(Expression tableFunctionCall) {
        return joinLateralInternal(tableFunctionCall, Optional.empty(), JoinType.INNER);
    }

    @Override
    public Table joinLateral(String tableFunctionCall, String joinPredicate) {
        return joinLateral(
                ExpressionParser.parseExpression(tableFunctionCall),
                ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table joinLateral(Expression tableFunctionCall, Expression joinPredicate) {
        return joinLateralInternal(tableFunctionCall, Optional.of(joinPredicate), JoinType.INNER);
    }

    @Override
    public Table leftOuterJoinLateral(String tableFunctionCall) {
        return leftOuterJoinLateral(ExpressionParser.parseExpression(tableFunctionCall));
    }

    @Override
    public Table leftOuterJoinLateral(Expression tableFunctionCall) {
        return joinLateralInternal(tableFunctionCall, Optional.empty(), JoinType.LEFT_OUTER);
    }

    @Override
    public Table leftOuterJoinLateral(String tableFunctionCall, String joinPredicate) {
        return leftOuterJoinLateral(
                ExpressionParser.parseExpression(tableFunctionCall),
                ExpressionParser.parseExpression(joinPredicate));
    }

    @Override
    public Table leftOuterJoinLateral(Expression tableFunctionCall, Expression joinPredicate) {
        return joinLateralInternal(
                tableFunctionCall, Optional.of(joinPredicate), JoinType.LEFT_OUTER);
    }

    private TableImpl joinLateralInternal(
            Expression callExpr, Optional<Expression> joinPredicate, JoinType joinType) {

        // check join type
        if (joinType != JoinType.INNER && joinType != JoinType.LEFT_OUTER) {
            throw new ValidationException(
                    "Table functions are currently only supported for inner and left outer lateral joins.");
        }

        return createTable(
                operationTreeBuilder.joinLateral(
                        this.operationTree, callExpr, joinType, joinPredicate));
    }

    @Override
    public Table minus(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.minus(operationTree, right.getQueryOperation(), false));
    }

    @Override
    public Table minusAll(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.minus(operationTree, right.getQueryOperation(), true));
    }

    @Override
    public Table union(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.union(operationTree, right.getQueryOperation(), false));
    }

    @Override
    public Table unionAll(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.union(operationTree, right.getQueryOperation(), true));
    }

    @Override
    public Table intersect(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.intersect(operationTree, right.getQueryOperation(), false));
    }

    @Override
    public Table intersectAll(Table right) {
        verifyTableCompatible(right);

        return createTable(
                operationTreeBuilder.intersect(operationTree, right.getQueryOperation(), true));
    }

    @Override
    public Table orderBy(String fields) {
        return createTable(
                operationTreeBuilder.sort(
                        ExpressionParser.parseExpressionList(fields), operationTree));
    }

    @Override
    public Table orderBy(Expression... fields) {
        return createTable(operationTreeBuilder.sort(Arrays.asList(fields), operationTree));
    }

    @Override
    public Table offset(int offset) {
        return createTable(operationTreeBuilder.limitWithOffset(offset, operationTree));
    }

    @Override
    public Table fetch(int fetch) {
        if (fetch < 0) {
            throw new ValidationException("FETCH count must be equal or larger than 0.");
        }
        return createTable(operationTreeBuilder.limitWithFetch(fetch, operationTree));
    }

    @Override
    public void insertInto(String tablePath) {
        tableEnvironment.insertInto(tablePath, this);
    }

    @Override
    public GroupWindowedTable window(GroupWindow groupWindow) {
        return new GroupWindowedTableImpl(this, groupWindow);
    }

    @Override
    public OverWindowedTable window(OverWindow... overWindows) {

        if (overWindows.length != 1) {
            throw new TableException("Currently, only a single over window is supported.");
        }

        return new OverWindowedTableImpl(this, Arrays.asList(overWindows));
    }

    @Override
    public Table addColumns(String fields) {
        return addColumnsOperation(false, ExpressionParser.parseExpressionList(fields));
    }

    @Override
    public Table addColumns(Expression... fields) {
        return addColumnsOperation(false, Arrays.asList(fields));
    }

    @Override
    public Table addOrReplaceColumns(String fields) {
        return addColumnsOperation(true, ExpressionParser.parseExpressionList(fields));
    }

    @Override
    public Table addOrReplaceColumns(Expression... fields) {
        return addColumnsOperation(true, Arrays.asList(fields));
    }

    private Table addColumnsOperation(boolean replaceIfExist, List<Expression> fields) {
        List<Expression> expressionsWithResolvedCalls = preprocessExpressions(fields);
        CategorizedExpressions extracted =
                OperationExpressionsUtils.extractAggregationsAndProperties(
                        expressionsWithResolvedCalls);

        List<Expression> aggNames = extracted.getAggregations();

        if (!aggNames.isEmpty()) {
            throw new ValidationException(
                    "The added field expression cannot be an aggregation, found: "
                            + aggNames.get(0));
        }

        return createTable(
                operationTreeBuilder.addColumns(
                        replaceIfExist, expressionsWithResolvedCalls, operationTree));
    }

    @Override
    public Table renameColumns(String fields) {
        return createTable(
                operationTreeBuilder.renameColumns(
                        ExpressionParser.parseExpressionList(fields), operationTree));
    }

    @Override
    public Table renameColumns(Expression... fields) {
        return createTable(
                operationTreeBuilder.renameColumns(Arrays.asList(fields), operationTree));
    }

    @Override
    public Table dropColumns(String fields) {
        return createTable(
                operationTreeBuilder.dropColumns(
                        ExpressionParser.parseExpressionList(fields), operationTree));
    }

    @Override
    public Table dropColumns(Expression... fields) {
        return createTable(operationTreeBuilder.dropColumns(Arrays.asList(fields), operationTree));
    }

    @Override
    public Table map(String mapFunction) {
        return map(ExpressionParser.parseExpression(mapFunction));
    }

    @Override
    public Table map(Expression mapFunction) {
        return createTable(operationTreeBuilder.map(mapFunction, operationTree));
    }

    @Override
    public Table flatMap(String tableFunction) {
        return flatMap(ExpressionParser.parseExpression(tableFunction));
    }

    @Override
    public Table flatMap(Expression tableFunction) {
        return createTable(operationTreeBuilder.flatMap(tableFunction, operationTree));
    }

    @Override
    public AggregatedTable aggregate(String aggregateFunction) {
        return aggregate(ExpressionParser.parseExpression(aggregateFunction));
    }

    @Override
    public AggregatedTable aggregate(Expression aggregateFunction) {
        return groupBy().aggregate(aggregateFunction);
    }

    @Override
    public FlatAggregateTable flatAggregate(String tableAggregateFunction) {
        return groupBy().flatAggregate(tableAggregateFunction);
    }

    @Override
    public FlatAggregateTable flatAggregate(Expression tableAggregateFunction) {
        return groupBy().flatAggregate(tableAggregateFunction);
    }

    @Override
    public TableResult executeInsert(String tablePath) {
        return executeInsert(tablePath, false);
    }

    @Override
    public TableResult executeInsert(String tablePath, boolean overwrite) {
        UnresolvedIdentifier unresolvedIdentifier =
                tableEnvironment.getParser().parseIdentifier(tablePath);
        ObjectIdentifier objectIdentifier =
                tableEnvironment.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        ModifyOperation operation =
                new CatalogSinkModifyOperation(
                        objectIdentifier,
                        getQueryOperation(),
                        Collections.emptyMap(),
                        overwrite,
                        Collections.emptyMap());

        return tableEnvironment.executeInternal(Collections.singletonList(operation));
    }

    @Override
    public TableResult execute() {
        return tableEnvironment.executeInternal(getQueryOperation());
    }

    @Override
    public String explain(ExplainDetail... extraDetails) {
        return tableEnvironment.explainInternal(
                Collections.singletonList(getQueryOperation()), extraDetails);
    }

    @Override
    public String toString() {
        if (tableName == null) {
            tableName = "UnnamedTable$" + uniqueId.getAndIncrement();
            tableEnvironment.registerTable(tableName, this);
        }
        return tableName;
    }

    private TableImpl createTable(QueryOperation operation) {
        return new TableImpl(tableEnvironment, operation, operationTreeBuilder, lookupResolver);
    }

    private List<Expression> preprocessExpressions(List<Expression> expressions) {
        return preprocessExpressions(expressions.toArray(new Expression[0]));
    }

    private List<Expression> preprocessExpressions(Expression[] expressions) {
        return Arrays.stream(expressions)
                .map(f -> f.accept(lookupResolver))
                .collect(Collectors.toList());
    }

    private static final class GroupedTableImpl implements GroupedTable {

        private final TableImpl table;
        private final List<Expression> groupKeys;

        private GroupedTableImpl(TableImpl table, List<Expression> groupKeys) {
            this.table = table;
            this.groupKeys = groupKeys;
        }

        @Override
        public Table select(String fields) {
            return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public Table select(Expression... fields) {
            List<Expression> expressionsWithResolvedCalls = table.preprocessExpressions(fields);
            CategorizedExpressions extracted =
                    OperationExpressionsUtils.extractAggregationsAndProperties(
                            expressionsWithResolvedCalls);

            if (!extracted.getWindowProperties().isEmpty()) {
                throw new ValidationException(
                        "Window properties can only be used on windowed tables.");
            }

            return table.createTable(
                    table.operationTreeBuilder.project(
                            extracted.getProjections(),
                            table.operationTreeBuilder.aggregate(
                                    groupKeys, extracted.getAggregations(), table.operationTree)));
        }

        @Override
        public AggregatedTable aggregate(String aggregateFunction) {
            return aggregate(ExpressionParser.parseExpression(aggregateFunction));
        }

        @Override
        public AggregatedTable aggregate(Expression aggregateFunction) {
            return new AggregatedTableImpl(table, groupKeys, aggregateFunction);
        }

        @Override
        public FlatAggregateTable flatAggregate(String tableAggFunction) {
            return flatAggregate(ExpressionParser.parseExpression(tableAggFunction));
        }

        @Override
        public FlatAggregateTable flatAggregate(Expression tableAggFunction) {
            return new FlatAggregateTableImpl(table, groupKeys, tableAggFunction);
        }
    }

    private static final class AggregatedTableImpl implements AggregatedTable {
        private final TableImpl table;
        private final List<Expression> groupKeys;
        private final Expression aggregateFunction;

        private AggregatedTableImpl(
                TableImpl table, List<Expression> groupKeys, Expression aggregateFunction) {
            this.table = table;
            this.groupKeys = groupKeys;
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public Table select(String fields) {
            return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public Table select(Expression... fields) {
            return table.createTable(
                    table.operationTreeBuilder.project(
                            Arrays.asList(fields),
                            table.operationTreeBuilder.aggregate(
                                    groupKeys, aggregateFunction, table.operationTree)));
        }
    }

    private static final class FlatAggregateTableImpl implements FlatAggregateTable {

        private final TableImpl table;
        private final List<Expression> groupKey;
        private final Expression tableAggregateFunction;

        private FlatAggregateTableImpl(
                TableImpl table, List<Expression> groupKey, Expression tableAggregateFunction) {
            this.table = table;
            this.groupKey = groupKey;
            this.tableAggregateFunction = tableAggregateFunction;
        }

        @Override
        public Table select(String fields) {
            return table.createTable(
                    table.operationTreeBuilder.project(
                            ExpressionParser.parseExpressionList(fields),
                            table.operationTreeBuilder.tableAggregate(
                                    groupKey,
                                    tableAggregateFunction.accept(table.lookupResolver),
                                    table.operationTree)));
        }

        @Override
        public Table select(Expression... fields) {
            return table.createTable(
                    table.operationTreeBuilder.project(
                            Arrays.asList(fields),
                            table.operationTreeBuilder.tableAggregate(
                                    groupKey,
                                    tableAggregateFunction.accept(table.lookupResolver),
                                    table.operationTree)));
        }
    }

    private static final class GroupWindowedTableImpl implements GroupWindowedTable {
        private final TableImpl table;
        private final GroupWindow window;

        private GroupWindowedTableImpl(TableImpl table, GroupWindow window) {
            this.table = table;
            this.window = window;
        }

        @Override
        public WindowGroupedTable groupBy(String fields) {
            return groupBy(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public WindowGroupedTable groupBy(Expression... fields) {
            List<Expression> fieldsWithoutWindow =
                    table.preprocessExpressions(fields).stream()
                            .filter(f -> !window.getAlias().equals(f))
                            .collect(Collectors.toList());
            if (fields.length != fieldsWithoutWindow.size() + 1) {
                throw new ValidationException("GroupBy must contain exactly one window alias.");
            }

            return new WindowGroupedTableImpl(table, fieldsWithoutWindow, window);
        }
    }

    private static final class WindowGroupedTableImpl implements WindowGroupedTable {

        private final TableImpl table;
        private final List<Expression> groupKeys;
        private final GroupWindow window;

        private WindowGroupedTableImpl(
                TableImpl table, List<Expression> groupKeys, GroupWindow window) {
            this.table = table;
            this.groupKeys = groupKeys;
            this.window = window;
        }

        @Override
        public Table select(String fields) {
            return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public Table select(Expression... fields) {
            List<Expression> expressionsWithResolvedCalls = table.preprocessExpressions(fields);
            CategorizedExpressions extracted =
                    OperationExpressionsUtils.extractAggregationsAndProperties(
                            expressionsWithResolvedCalls);

            return table.createTable(
                    table.operationTreeBuilder.project(
                            extracted.getProjections(),
                            table.operationTreeBuilder.windowAggregate(
                                    groupKeys,
                                    window,
                                    extracted.getWindowProperties(),
                                    extracted.getAggregations(),
                                    table.operationTree),
                            // required for proper resolution of the time attribute in multi-windows
                            true));
        }

        @Override
        public AggregatedTable aggregate(String aggregateFunction) {
            return aggregate(ExpressionParser.parseExpression(aggregateFunction));
        }

        @Override
        public AggregatedTable aggregate(Expression aggregateFunction) {
            return new WindowAggregatedTableImpl(table, groupKeys, aggregateFunction, window);
        }

        @Override
        public FlatAggregateTable flatAggregate(String tableAggregateFunction) {
            return flatAggregate(ExpressionParser.parseExpression(tableAggregateFunction));
        }

        @Override
        public FlatAggregateTable flatAggregate(Expression tableAggregateFunction) {
            return new WindowFlatAggregateTableImpl(
                    table, groupKeys, tableAggregateFunction, window);
        }
    }

    private static final class WindowAggregatedTableImpl implements AggregatedTable {
        private final TableImpl table;
        private final List<Expression> groupKeys;
        private final Expression aggregateFunction;
        private final GroupWindow window;

        private WindowAggregatedTableImpl(
                TableImpl table,
                List<Expression> groupKeys,
                Expression aggregateFunction,
                GroupWindow window) {
            this.table = table;
            this.groupKeys = groupKeys;
            this.aggregateFunction = aggregateFunction;
            this.window = window;
        }

        @Override
        public Table select(String fields) {
            return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public Table select(Expression... fields) {
            List<Expression> expressionsWithResolvedCalls = table.preprocessExpressions(fields);
            CategorizedExpressions extracted =
                    OperationExpressionsUtils.extractAggregationsAndProperties(
                            expressionsWithResolvedCalls);

            if (!extracted.getAggregations().isEmpty()) {
                throw new ValidationException(
                        "Aggregate functions cannot be used in the select right "
                                + "after the aggregate.");
            }

            if (extracted.getProjections().stream()
                    .anyMatch(
                            p ->
                                    (p instanceof UnresolvedReferenceExpression)
                                            && "*"
                                                    .equals(
                                                            ((UnresolvedReferenceExpression) p)
                                                                    .getName()))) {
                throw new ValidationException("Can not use * for window aggregate!");
            }

            return table.createTable(
                    table.operationTreeBuilder.project(
                            extracted.getProjections(),
                            table.operationTreeBuilder.windowAggregate(
                                    groupKeys,
                                    window,
                                    extracted.getWindowProperties(),
                                    aggregateFunction,
                                    table.operationTree)));
        }
    }

    private static final class WindowFlatAggregateTableImpl implements FlatAggregateTable {

        private final TableImpl table;
        private final List<Expression> groupKeys;
        private final Expression tableAggFunction;
        private final GroupWindow window;

        private WindowFlatAggregateTableImpl(
                TableImpl table,
                List<Expression> groupKeys,
                Expression tableAggFunction,
                GroupWindow window) {
            this.table = table;
            this.groupKeys = groupKeys;
            this.tableAggFunction = tableAggFunction;
            this.window = window;
        }

        @Override
        public Table select(String fields) {
            return select(ExpressionParser.parseExpressionList(fields).toArray(new Expression[0]));
        }

        @Override
        public Table select(Expression... fields) {
            List<Expression> expressionsWithResolvedCalls = table.preprocessExpressions(fields);
            CategorizedExpressions extracted =
                    OperationExpressionsUtils.extractAggregationsAndProperties(
                            expressionsWithResolvedCalls);

            if (!extracted.getAggregations().isEmpty()) {
                throw new ValidationException(
                        "Aggregate functions cannot be used in the select right "
                                + "after the flatAggregate.");
            }

            if (extracted.getProjections().stream()
                    .anyMatch(
                            p ->
                                    (p instanceof UnresolvedReferenceExpression)
                                            && "*"
                                                    .equals(
                                                            ((UnresolvedReferenceExpression) p)
                                                                    .getName()))) {
                throw new ValidationException("Can not use * for window aggregate!");
            }

            return table.createTable(
                    table.operationTreeBuilder.project(
                            extracted.getProjections(),
                            table.operationTreeBuilder.windowTableAggregate(
                                    groupKeys,
                                    window,
                                    extracted.getWindowProperties(),
                                    tableAggFunction,
                                    table.operationTree),
                            // required for proper resolution of the time attribute in multi-windows
                            true));
        }
    }

    private static final class OverWindowedTableImpl implements OverWindowedTable {

        private final TableImpl table;
        private final List<OverWindow> overWindows;

        private OverWindowedTableImpl(TableImpl table, List<OverWindow> overWindows) {
            this.table = table;
            this.overWindows = overWindows;
        }

        @Override
        public Table select(String fields) {
            return table.createTable(
                    table.operationTreeBuilder.project(
                            ExpressionParser.parseExpressionList(fields),
                            table.operationTree,
                            overWindows));
        }

        @Override
        public Table select(Expression... fields) {
            return table.createTable(
                    table.operationTreeBuilder.project(
                            Arrays.asList(fields), table.operationTree, overWindows));
        }
    }
}
