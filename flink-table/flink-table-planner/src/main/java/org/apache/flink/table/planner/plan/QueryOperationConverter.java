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

package org.apache.flink.table.planner.plan;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.bridge.PlannerExternalQueryOperation;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.DataStreamQueryOperation;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.ExternalQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.SourceQueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.operations.utils.QueryOperationDefaultVisitor;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.expressions.SqlAggFunctionVisitor;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.operations.InternalDataStreamQueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.DataStreamTable;
import org.apache.flink.table.planner.plan.schema.DataStreamTable$;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TypedFlinkTableFunction;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.ProctimeAttribute;
import org.apache.flink.table.runtime.groupwindow.RowtimeAttribute;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowReference;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataToLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Converter from Flink's specific relational representation: {@link QueryOperation} to Calcite's
 * specific relational representation: {@link RelNode}.
 */
@Internal
public class QueryOperationConverter extends QueryOperationDefaultVisitor<RelNode> {

    private final FlinkRelBuilder relBuilder;
    private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
    private final ExpressionConverter expressionConverter;
    private final AggregateVisitor aggregateVisitor = new AggregateVisitor();
    private final TableAggregateVisitor tableAggregateVisitor = new TableAggregateVisitor();
    private final JoinExpressionVisitor joinExpressionVisitor = new JoinExpressionVisitor();
    private final boolean isBatchMode;

    public QueryOperationConverter(FlinkRelBuilder relBuilder, boolean isBatchMode) {
        this.relBuilder = relBuilder;
        this.expressionConverter = new ExpressionConverter(relBuilder);
        this.isBatchMode = isBatchMode;
    }

    @Override
    public RelNode defaultMethod(QueryOperation other) {
        other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
        return other.accept(singleRelVisitor);
    }

    private class SingleRelVisitor implements QueryOperationVisitor<RelNode> {

        @Override
        public RelNode visit(ProjectQueryOperation projection) {
            List<RexNode> rexNodes = convertToRexNodes(projection.getProjectList());

            return relBuilder
                    .project(rexNodes, projection.getResolvedSchema().getColumnNames(), true)
                    .build();
        }

        @Override
        public RelNode visit(AggregateQueryOperation aggregate) {
            List<AggCall> aggregations =
                    aggregate.getAggregateExpressions().stream()
                            .map(this::getAggCall)
                            .collect(toList());

            List<RexNode> groupings = convertToRexNodes(aggregate.getGroupingExpressions());
            GroupKey groupKey = relBuilder.groupKey(groupings);
            return relBuilder.aggregate(groupKey, aggregations).build();
        }

        @Override
        public RelNode visit(WindowAggregateQueryOperation windowAggregate) {
            List<AggCall> aggregations =
                    windowAggregate.getAggregateExpressions().stream()
                            .map(this::getAggCall)
                            .collect(toList());

            List<RexNode> groupings = convertToRexNodes(windowAggregate.getGroupingExpressions());
            LogicalWindow logicalWindow = toLogicalWindow(windowAggregate.getGroupWindow());
            WindowReference windowReference = logicalWindow.aliasAttribute();
            List<NamedWindowProperty> windowProperties =
                    windowAggregate.getWindowPropertiesExpressions().stream()
                            .map(expr -> convertToWindowProperty(expr, windowReference))
                            .collect(toList());
            GroupKey groupKey = relBuilder.groupKey(groupings);
            return relBuilder
                    .windowAggregate(logicalWindow, groupKey, windowProperties, aggregations)
                    .build();
        }

        private NamedWindowProperty convertToWindowProperty(
                Expression expression, WindowReference windowReference) {
            Preconditions.checkArgument(
                    expression instanceof CallExpression, "This should never happened");
            CallExpression aliasExpr = (CallExpression) expression;
            Preconditions.checkArgument(
                    BuiltInFunctionDefinitions.AS == aliasExpr.getFunctionDefinition(),
                    "This should never happened");
            String name =
                    ((ValueLiteralExpression) aliasExpr.getChildren().get(1))
                            .getValueAs(String.class)
                            .orElseThrow(() -> new TableException("Invalid literal."));
            Expression windowPropertyExpr = aliasExpr.getChildren().get(0);
            Preconditions.checkArgument(
                    windowPropertyExpr instanceof CallExpression, "This should never happened");
            CallExpression windowPropertyCallExpr = (CallExpression) windowPropertyExpr;
            FunctionDefinition fd = windowPropertyCallExpr.getFunctionDefinition();
            if (BuiltInFunctionDefinitions.WINDOW_START == fd) {
                return new NamedWindowProperty(name, new WindowStart(windowReference));
            } else if (BuiltInFunctionDefinitions.WINDOW_END == fd) {
                return new NamedWindowProperty(name, new WindowEnd(windowReference));
            } else if (BuiltInFunctionDefinitions.PROCTIME == fd) {
                return new NamedWindowProperty(name, new ProctimeAttribute(windowReference));
            } else if (BuiltInFunctionDefinitions.ROWTIME == fd) {
                return new NamedWindowProperty(name, new RowtimeAttribute(windowReference));
            } else {
                throw new TableException("Invalid literal.");
            }
        }

        /** Get the {@link AggCall} correspond to the aggregate or table aggregate expression. */
        private AggCall getAggCall(Expression aggregateExpression) {
            if (isFunctionOfKind(aggregateExpression, TABLE_AGGREGATE)) {
                return aggregateExpression.accept(tableAggregateVisitor);
            } else {
                return aggregateExpression.accept(aggregateVisitor);
            }
        }

        @Override
        public RelNode visit(JoinQueryOperation join) {
            final Set<CorrelationId> corSet;
            if (join.isCorrelated()) {
                corSet = Collections.singleton(relBuilder.peek().getCluster().createCorrel());
            } else {
                corSet = Collections.emptySet();
            }

            return relBuilder
                    .join(
                            convertJoinType(join.getJoinType()),
                            join.getCondition().accept(joinExpressionVisitor),
                            corSet)
                    .build();
        }

        @Override
        public RelNode visit(SetQueryOperation setOperation) {
            switch (setOperation.getType()) {
                case INTERSECT:
                    relBuilder.intersect(setOperation.isAll());
                    break;
                case MINUS:
                    relBuilder.minus(setOperation.isAll());
                    break;
                case UNION:
                    relBuilder.union(setOperation.isAll());
                    break;
            }
            return relBuilder.build();
        }

        @Override
        public RelNode visit(FilterQueryOperation filter) {
            RexNode rexNode = convertExprToRexNode(filter.getCondition());
            return relBuilder.filter(rexNode).build();
        }

        @Override
        public RelNode visit(DistinctQueryOperation distinct) {
            return relBuilder.distinct().build();
        }

        @Override
        public RelNode visit(SortQueryOperation sort) {
            List<RexNode> rexNodes = convertToRexNodes(sort.getOrder());
            return relBuilder.sortLimit(sort.getOffset(), sort.getFetch(), rexNodes).build();
        }

        @Override
        public RelNode visit(CalculatedQueryOperation calculatedTable) {
            final ContextResolvedFunction resolvedFunction = calculatedTable.getResolvedFunction();
            final List<RexNode> parameters = convertToRexNodes(calculatedTable.getArguments());

            final FunctionDefinition functionDefinition = resolvedFunction.getDefinition();
            if (functionDefinition instanceof TableFunctionDefinition) {
                final FlinkTypeFactory typeFactory = relBuilder.getTypeFactory();
                return convertLegacyTableFunction(
                        calculatedTable,
                        (TableFunctionDefinition) functionDefinition,
                        parameters,
                        typeFactory);
            }

            final BridgingSqlFunction sqlFunction =
                    BridgingSqlFunction.of(relBuilder.getCluster(), resolvedFunction);

            FlinkRelBuilder.pushFunctionScan(
                    relBuilder,
                    sqlFunction,
                    0,
                    parameters,
                    calculatedTable.getResolvedSchema().getColumnNames());

            return relBuilder.build();
        }

        private RelNode convertLegacyTableFunction(
                CalculatedQueryOperation calculatedTable,
                TableFunctionDefinition functionDefinition,
                List<RexNode> parameters,
                FlinkTypeFactory typeFactory) {
            List<String> fieldNames = calculatedTable.getResolvedSchema().getColumnNames();

            TableFunction<?> tableFunction = functionDefinition.getTableFunction();
            DataType resultType = fromLegacyInfoToDataType(functionDefinition.getResultType());
            TypedFlinkTableFunction function =
                    new TypedFlinkTableFunction(
                            tableFunction, fieldNames.toArray(new String[0]), resultType);

            final TableSqlFunction sqlFunction =
                    new TableSqlFunction(
                            calculatedTable.getResolvedFunction().getIdentifier().orElse(null),
                            tableFunction.toString(),
                            tableFunction,
                            resultType,
                            typeFactory,
                            function,
                            scala.Option.empty());
            return LogicalTableFunctionScan.create(
                    relBuilder.peek().getCluster(),
                    Collections.emptyList(),
                    relBuilder
                            .getRexBuilder()
                            .makeCall(function.getRowType(typeFactory), sqlFunction, parameters),
                    function.getElementType(null),
                    function.getRowType(typeFactory),
                    null);
        }

        @Override
        public RelNode visit(SourceQueryOperation queryOperation) {
            ContextResolvedTable contextResolvedTable = queryOperation.getContextResolvedTable();
            if (contextResolvedTable.isAnonymous()) {
                return CatalogSourceTable.createAnonymous(
                                relBuilder, contextResolvedTable, isBatchMode)
                        .toRel(ViewExpanders.simpleContext(relBuilder.getCluster()));
            }
            Map<String, String> dynamicOptions = queryOperation.getDynamicOptions();
            if (dynamicOptions != null) {
                return relBuilder
                        .scan(contextResolvedTable.getIdentifier(), dynamicOptions)
                        .build();
            }
            return relBuilder
                    .scan(queryOperation.getContextResolvedTable().getIdentifier().toList())
                    .build();
        }

        @Override
        public RelNode visit(ValuesQueryOperation values) {
            RelDataType rowType =
                    relBuilder
                            .getTypeFactory()
                            .buildRelNodeRowType(
                                    TableSchema.fromResolvedSchema(values.getResolvedSchema()));
            if (values.getValues().isEmpty()) {
                relBuilder.values(rowType);
                return relBuilder.build();
            }

            List<List<RexLiteral>> rexLiterals = new ArrayList<>();
            List<List<RexNode>> rexProjections = new ArrayList<>();

            splitToProjectionsAndLiterals(values, rexLiterals, rexProjections);

            int inputs = 0;
            if (rexLiterals.size() != 0) {
                inputs += 1;
                relBuilder.values(rexLiterals, rowType);
            }

            if (rexProjections.size() != 0) {
                inputs += rexProjections.size();
                applyProjections(values, rexProjections);
            }

            if (inputs > 1) {
                relBuilder.union(true, inputs);
            }
            return relBuilder.build();
        }

        private void applyProjections(
                ValuesQueryOperation values, List<List<RexNode>> rexProjections) {
            List<RelNode> relNodes =
                    rexProjections.stream()
                            .map(
                                    exprs -> {
                                        relBuilder.push(
                                                LogicalValues.createOneRow(
                                                        relBuilder.getCluster()));
                                        relBuilder.project(
                                                exprs, values.getResolvedSchema().getColumnNames());
                                        return relBuilder.build();
                                    })
                            .collect(toList());
            relBuilder.pushAll(relNodes);
        }

        private void splitToProjectionsAndLiterals(
                ValuesQueryOperation values,
                List<List<RexLiteral>> rexValues,
                List<List<RexNode>> rexProjections) {
            values.getValues().stream()
                    .map(this::convertToRexNodes)
                    .forEach(
                            row -> {
                                boolean allLiterals =
                                        row.stream().allMatch(expr -> expr instanceof RexLiteral);
                                if (allLiterals) {
                                    rexValues.add(
                                            row.stream()
                                                    .map(expr -> (RexLiteral) expr)
                                                    .collect(toList()));
                                } else {
                                    rexProjections.add(row);
                                }
                            });
        }

        @Override
        public RelNode visit(QueryOperation other) {
            if (other instanceof PlannerQueryOperation) {
                return ((PlannerQueryOperation) other).getCalciteTree();
            } else if (other instanceof PlannerExternalQueryOperation) {
                return ((PlannerExternalQueryOperation) other).getCalciteTree();
            } else if (other instanceof InternalDataStreamQueryOperation) {
                return convertToDataStreamScan((InternalDataStreamQueryOperation<?>) other);
            } else if (other instanceof ExternalQueryOperation) {
                final ExternalQueryOperation<?> externalQueryOperation =
                        (ExternalQueryOperation<?>) other;
                return convertToExternalScan(
                        externalQueryOperation.getContextResolvedTable(),
                        externalQueryOperation.getDataStream(),
                        externalQueryOperation.getPhysicalDataType(),
                        externalQueryOperation.isTopLevelRecord(),
                        externalQueryOperation.getChangelogMode());
            }
            // legacy
            else if (other instanceof DataStreamQueryOperation) {
                DataStreamQueryOperation<?> dataStreamQueryOperation =
                        (DataStreamQueryOperation<?>) other;
                return convertToDataStreamScan(
                        dataStreamQueryOperation.getDataStream(),
                        dataStreamQueryOperation.getFieldIndices(),
                        dataStreamQueryOperation.getResolvedSchema(),
                        dataStreamQueryOperation.getIdentifier());
            }

            throw new TableException("Unknown table operation: " + other);
        }

        @Override
        public <U> RelNode visit(TableSourceQueryOperation<U> tableSourceOperation) {
            TableSource<?> tableSource = tableSourceOperation.getTableSource();
            boolean isBatch;
            if (tableSource instanceof LookupableTableSource) {
                isBatch = tableSourceOperation.isBatch();
            } else if (tableSource instanceof StreamTableSource) {
                isBatch = ((StreamTableSource<?>) tableSource).isBounded();
            } else {
                throw new TableException(
                        String.format(
                                "%s is not supported.", tableSource.getClass().getSimpleName()));
            }

            FlinkStatistic statistic;
            ObjectIdentifier tableIdentifier;
            if (tableSourceOperation instanceof RichTableSourceQueryOperation
                    && ((RichTableSourceQueryOperation<U>) tableSourceOperation).getIdentifier()
                            != null) {
                tableIdentifier =
                        ((RichTableSourceQueryOperation<U>) tableSourceOperation).getIdentifier();
                statistic =
                        ((RichTableSourceQueryOperation<U>) tableSourceOperation).getStatistic();
            } else {
                statistic = FlinkStatistic.UNKNOWN();
                // TableSourceScan requires a unique name of a Table for computing a digest.
                // We are using the identity hash of the TableSource object.
                String refId = "Unregistered_TableSource_" + System.identityHashCode(tableSource);
                CatalogManager catalogManager =
                        relBuilder
                                .getCluster()
                                .getPlanner()
                                .getContext()
                                .unwrap(FlinkContext.class)
                                .getCatalogManager();
                tableIdentifier = catalogManager.qualifyIdentifier(UnresolvedIdentifier.of(refId));
            }

            RelDataType rowType =
                    TableSourceUtil.getSourceRowTypeFromSource(
                            relBuilder.getTypeFactory(), tableSource, !isBatch);
            LegacyTableSourceTable<?> tableSourceTable =
                    new LegacyTableSourceTable<>(
                            relBuilder.getRelOptSchema(),
                            tableIdentifier,
                            rowType,
                            statistic,
                            tableSource,
                            !isBatch,
                            ConnectorCatalogTable.source(tableSource, isBatch));
            return LogicalTableScan.create(
                    relBuilder.getCluster(), tableSourceTable, Collections.emptyList());
        }

        private RelNode convertToExternalScan(
                ContextResolvedTable contextResolvedTable,
                DataStream<?> dataStream,
                DataType physicalDataType,
                boolean isTopLevelRecord,
                ChangelogMode changelogMode) {
            final FlinkContext flinkContext = ShortcutUtils.unwrapContext(relBuilder);
            return DynamicSourceUtils.convertDataStreamToRel(
                    flinkContext.isBatchMode(),
                    flinkContext.getTableConfig(),
                    relBuilder,
                    contextResolvedTable,
                    dataStream,
                    physicalDataType,
                    isTopLevelRecord,
                    changelogMode);
        }

        private RelNode convertToDataStreamScan(InternalDataStreamQueryOperation<?> operation) {
            List<String> names;
            ObjectIdentifier identifier = operation.getIdentifier();
            if (identifier != null) {
                names =
                        Arrays.asList(
                                identifier.getCatalogName(),
                                identifier.getDatabaseName(),
                                identifier.getObjectName());
            } else {
                String refId =
                        String.format(
                                "Unregistered_DataStream_%s", operation.getDataStream().getId());
                names = Collections.singletonList(refId);
            }

            final RelDataType rowType =
                    DataStreamTable$.MODULE$.getRowType(
                            relBuilder.getTypeFactory(),
                            operation.getDataStream(),
                            operation.getResolvedSchema().getColumnNames().toArray(new String[0]),
                            operation.getFieldIndices(),
                            scala.Option.apply(operation.getFieldNullables()));
            DataStreamTable<?> dataStreamTable =
                    new DataStreamTable<>(
                            relBuilder.getRelOptSchema(),
                            names,
                            rowType,
                            operation.getDataStream(),
                            operation.getFieldIndices(),
                            operation.getResolvedSchema().getColumnNames().toArray(new String[0]),
                            operation.getStatistic(),
                            scala.Option.apply(operation.getFieldNullables()));
            return LogicalTableScan.create(
                    relBuilder.getCluster(), dataStreamTable, Collections.emptyList());
        }

        private RelNode convertToDataStreamScan(
                DataStream<?> dataStream,
                int[] fieldIndices,
                ResolvedSchema resolvedSchema,
                Optional<ObjectIdentifier> identifier) {
            List<String> names;
            if (identifier.isPresent()) {
                names =
                        Arrays.asList(
                                identifier.get().getCatalogName(),
                                identifier.get().getDatabaseName(),
                                identifier.get().getObjectName());
            } else {
                String refId = String.format("Unregistered_DataStream_%s", dataStream.getId());
                names = Collections.singletonList(refId);
            }
            final RelDataType rowType =
                    DataStreamTable$.MODULE$.getRowType(
                            relBuilder.getTypeFactory(),
                            dataStream,
                            resolvedSchema.getColumnNames().toArray(new String[0]),
                            fieldIndices,
                            scala.Option.empty());
            DataStreamTable<?> dataStreamTable =
                    new DataStreamTable<>(
                            relBuilder.getRelOptSchema(),
                            names,
                            rowType,
                            dataStream,
                            fieldIndices,
                            resolvedSchema.getColumnNames().toArray(new String[0]),
                            FlinkStatistic.UNKNOWN(),
                            scala.Option.empty());
            return LogicalTableScan.create(
                    relBuilder.getCluster(), dataStreamTable, Collections.emptyList());
        }

        private List<RexNode> convertToRexNodes(List<ResolvedExpression> expressions) {
            return expressions.stream()
                    .map(QueryOperationConverter.this::convertExprToRexNode)
                    .collect(toList());
        }

        private LogicalWindow toLogicalWindow(ResolvedGroupWindow window) {
            DataType windowType = window.getTimeAttribute().getOutputDataType();
            WindowReference windowReference =
                    new WindowReference(window.getAlias(), fromDataToLogicalType(windowType));
            switch (window.getType()) {
                case SLIDE:
                    return new SlidingGroupWindow(
                            windowReference,
                            window.getTimeAttribute(),
                            window.getSize()
                                    .orElseThrow(
                                            () -> new TableException("missed size parameters!")),
                            window.getSlide()
                                    .orElseThrow(
                                            () -> new TableException("missed slide parameters!")));
                case SESSION:
                    return new SessionGroupWindow(
                            windowReference,
                            window.getTimeAttribute(),
                            window.getGap()
                                    .orElseThrow(
                                            () -> new TableException("missed gap parameters!")));
                case TUMBLE:
                    return new TumblingGroupWindow(
                            windowReference,
                            window.getTimeAttribute(),
                            window.getSize()
                                    .orElseThrow(
                                            () -> new TableException("missed size parameters!")));
                default:
                    throw new TableException("Unknown window type");
            }
        }

        private JoinRelType convertJoinType(JoinType joinType) {
            switch (joinType) {
                case INNER:
                    return JoinRelType.INNER;
                case LEFT_OUTER:
                    return JoinRelType.LEFT;
                case RIGHT_OUTER:
                    return JoinRelType.RIGHT;
                case FULL_OUTER:
                    return JoinRelType.FULL;
                default:
                    throw new TableException("Unknown join type: " + joinType);
            }
        }
    }

    private class JoinExpressionVisitor extends ExpressionDefaultVisitor<RexNode> {

        private static final int numberOfJoinInputs = 2;

        @Override
        public RexNode visit(CallExpression callExpression) {
            final List<ResolvedExpression> newChildren =
                    callExpression.getChildren().stream()
                            .map(
                                    expr -> {
                                        RexNode convertedNode = expr.accept(this);
                                        return new RexNodeExpression(
                                                convertedNode,
                                                ((ResolvedExpression) expr).getOutputDataType(),
                                                null,
                                                null);
                                    })
                            .collect(Collectors.toList());

            final CallExpression newCall =
                    callExpression.replaceArgs(newChildren, callExpression.getOutputDataType());
            return convertExprToRexNode(newCall);
        }

        @Override
        public RexNode visit(FieldReferenceExpression fieldReference) {
            return relBuilder.field(
                    numberOfJoinInputs,
                    fieldReference.getInputIndex(),
                    fieldReference.getFieldIndex());
        }

        @Override
        protected RexNode defaultMethod(Expression expression) {
            return convertExprToRexNode(expression);
        }
    }

    private class AggregateVisitor extends ExpressionDefaultVisitor<AggCall> {

        @Override
        public AggCall visit(CallExpression unresolvedCall) {
            if (unresolvedCall.getFunctionDefinition() == AS) {
                String aggregateName =
                        extractValue(unresolvedCall.getChildren().get(1), String.class)
                                .orElseThrow(() -> new TableException("Unexpected name."));

                Expression aggregate = unresolvedCall.getChildren().get(0);
                if (isFunctionOfKind(aggregate, AGGREGATE)) {
                    return aggregate.accept(
                            new AggCallVisitor(
                                    relBuilder, expressionConverter, aggregateName, false));
                }
            }
            throw new TableException("Expected named aggregate. Got: " + unresolvedCall);
        }

        @Override
        protected AggCall defaultMethod(Expression expression) {
            throw new TableException("Unexpected expression: " + expression);
        }

        private class AggCallVisitor extends ExpressionDefaultVisitor<RelBuilder.AggCall> {

            private final RelBuilder relBuilder;
            private final SqlAggFunctionVisitor sqlAggFunctionVisitor;
            private final ExpressionConverter expressionConverter;
            private final String name;
            private final boolean isDistinct;

            public AggCallVisitor(
                    RelBuilder relBuilder,
                    ExpressionConverter expressionConverter,
                    String name,
                    boolean isDistinct) {
                this.relBuilder = relBuilder;
                this.sqlAggFunctionVisitor = new SqlAggFunctionVisitor(relBuilder);
                this.expressionConverter = expressionConverter;
                this.name = name;
                this.isDistinct = isDistinct;
            }

            @Override
            public RelBuilder.AggCall visit(CallExpression call) {
                FunctionDefinition def = call.getFunctionDefinition();
                if (BuiltInFunctionDefinitions.DISTINCT == def) {
                    Expression innerAgg = call.getChildren().get(0);
                    return innerAgg.accept(
                            new AggCallVisitor(relBuilder, expressionConverter, name, true));
                } else {
                    SqlAggFunction sqlAggFunction = call.accept(sqlAggFunctionVisitor);
                    return relBuilder.aggregateCall(
                            sqlAggFunction,
                            isDistinct,
                            false,
                            null,
                            name,
                            call.getChildren().stream()
                                    .map(expr -> expr.accept(expressionConverter))
                                    .collect(Collectors.toList()));
                }
            }

            @Override
            protected RelBuilder.AggCall defaultMethod(Expression expression) {
                throw new TableException("Unexpected expression: " + expression);
            }
        }
    }

    private class TableAggregateVisitor extends ExpressionDefaultVisitor<RelBuilder.AggCall> {
        @Override
        public AggCall visit(CallExpression call) {
            if (isFunctionOfKind(call, TABLE_AGGREGATE)) {
                return call.accept(new TableAggCallVisitor(relBuilder, expressionConverter));
            }
            return defaultMethod(call);
        }

        @Override
        protected AggCall defaultMethod(Expression expression) {
            throw new TableException("Expected table aggregate. Got: " + expression);
        }

        private class TableAggCallVisitor extends ExpressionDefaultVisitor<RelBuilder.AggCall> {

            private final RelBuilder relBuilder;
            private final SqlAggFunctionVisitor sqlAggFunctionVisitor;
            private final ExpressionConverter expressionConverter;

            public TableAggCallVisitor(
                    RelBuilder relBuilder, ExpressionConverter expressionConverter) {
                this.relBuilder = relBuilder;
                this.sqlAggFunctionVisitor = new SqlAggFunctionVisitor(relBuilder);
                this.expressionConverter = expressionConverter;
            }

            @Override
            public RelBuilder.AggCall visit(CallExpression call) {
                SqlAggFunction sqlAggFunction = call.accept(sqlAggFunctionVisitor);
                return relBuilder.aggregateCall(
                        sqlAggFunction,
                        false,
                        false,
                        null,
                        sqlAggFunction.toString(),
                        call.getChildren().stream()
                                .map(expr -> expr.accept(expressionConverter))
                                .collect(toList()));
            }

            @Override
            protected RelBuilder.AggCall defaultMethod(Expression expression) {
                throw new TableException("Expected table aggregate. Got: " + expression);
            }
        }
    }

    private RexNode convertExprToRexNode(Expression expr) {
        return expr.accept(expressionConverter);
    }
}
