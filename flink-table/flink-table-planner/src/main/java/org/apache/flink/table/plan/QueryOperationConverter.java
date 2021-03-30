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

package org.apache.flink.table.plan;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.CatalogReader;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.RexPlannerExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.WindowReference;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.utils.TableSqlFunction;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.DataSetQueryOperation;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JavaDataStreamQueryOperation;
import org.apache.flink.table.operations.JavaExternalQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.operations.ScalaDataStreamQueryOperation;
import org.apache.flink.table.operations.ScalaExternalQueryOperation;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.ValuesQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.operations.utils.QueryOperationDefaultVisitor;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.SessionGroupWindow;
import org.apache.flink.table.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataSetScan;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataStreamScan;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import scala.Option;
import scala.Some;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

/**
 * Converter from Flink's specific relational representation: {@link QueryOperation} to Calcite's
 * specific relational representation: {@link RelNode}.
 */
@Internal
public class QueryOperationConverter extends QueryOperationDefaultVisitor<RelNode> {

    private final FlinkRelBuilder relBuilder;
    private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
    private final ExpressionBridge<PlannerExpression> expressionBridge;
    private final AggregateVisitor aggregateVisitor = new AggregateVisitor();
    private final TableAggregateVisitor tableAggregateVisitor = new TableAggregateVisitor();
    private final JoinExpressionVisitor joinExpressionVisitor = new JoinExpressionVisitor();

    public QueryOperationConverter(
            FlinkRelBuilder relBuilder, ExpressionBridge<PlannerExpression> expressionBridge) {
        this.relBuilder = relBuilder;
        this.expressionBridge = expressionBridge;
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
            List<PlannerExpression> windowProperties =
                    windowAggregate.getWindowPropertiesExpressions().stream()
                            .map(expressionBridge::bridge)
                            .collect(toList());
            GroupKey groupKey = relBuilder.groupKey(groupings);
            LogicalWindow logicalWindow = toLogicalWindow(windowAggregate.getGroupWindow());
            return relBuilder
                    .windowAggregate(logicalWindow, groupKey, windowProperties, aggregations)
                    .build();
        }

        /** Get the {@link AggCall} correspond to the aggregate expression. */
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
            RexNode rexNode = convertToRexNode(filter.getCondition());
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
            FlinkTypeFactory typeFactory = relBuilder.getTypeFactory();
            if (calculatedTable.getFunctionDefinition() instanceof TableFunctionDefinition) {
                TableFunctionDefinition functionDefinition =
                        (TableFunctionDefinition) calculatedTable.getFunctionDefinition();

                String[] fieldNames =
                        calculatedTable.getResolvedSchema().getColumnNames().toArray(new String[0]);
                int[] fieldIndices = IntStream.range(0, fieldNames.length).toArray();

                TableFunction<?> tableFunction = functionDefinition.getTableFunction();
                TypeInformation<?> rowType = functionDefinition.getResultType();
                FlinkTableFunctionImpl<?> function =
                        new FlinkTableFunctionImpl<>(rowType, fieldIndices, fieldNames);

                final TableSqlFunction sqlFunction =
                        new TableSqlFunction(
                                tableFunction.functionIdentifier(),
                                tableFunction.toString(),
                                tableFunction,
                                rowType,
                                typeFactory,
                                function);

                List<RexNode> parameters = convertToRexNodes(calculatedTable.getArguments());
                return LogicalTableFunctionScan.create(
                        relBuilder.peek().getCluster(),
                        Collections.emptyList(),
                        relBuilder.call(sqlFunction, parameters),
                        function.getElementType(null),
                        function.getRowType(typeFactory, null),
                        null);
            }

            throw new ValidationException(
                    "The new type inference for functions is only supported in the Blink planner.");
        }

        @Override
        public RelNode visit(CatalogQueryOperation catalogTable) {
            ObjectIdentifier objectIdentifier = catalogTable.getTableIdentifier();
            return relBuilder
                    .scan(
                            objectIdentifier.getCatalogName(),
                            objectIdentifier.getDatabaseName(),
                            objectIdentifier.getObjectName())
                    .build();
        }

        @Override
        public RelNode visit(ValuesQueryOperation values) {
            RelDataType rowType =
                    relBuilder
                            .getTypeFactory()
                            .buildLogicalRowType(
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
            } else if (other instanceof JavaDataStreamQueryOperation) {
                JavaDataStreamQueryOperation<?> dataStreamQueryOperation =
                        (JavaDataStreamQueryOperation<?>) other;
                return convertToDataStreamScan(
                        dataStreamQueryOperation.getDataStream(),
                        dataStreamQueryOperation.getFieldIndices(),
                        TableSchema.fromResolvedSchema(
                                dataStreamQueryOperation.getResolvedSchema()));
            } else if (other instanceof DataSetQueryOperation) {
                return convertToDataSetScan((DataSetQueryOperation<?>) other);
            } else if (other instanceof ScalaDataStreamQueryOperation) {
                ScalaDataStreamQueryOperation<?> dataStreamQueryOperation =
                        (ScalaDataStreamQueryOperation<?>) other;
                return convertToDataStreamScan(
                        dataStreamQueryOperation.getDataStream(),
                        dataStreamQueryOperation.getFieldIndices(),
                        TableSchema.fromResolvedSchema(
                                dataStreamQueryOperation.getResolvedSchema()));
            }
            // best effort support of FLIP-136 in old planner
            else if (other instanceof JavaExternalQueryOperation) {
                JavaExternalQueryOperation<?> externalQueryOperation =
                        (JavaExternalQueryOperation<?>) other;
                return convertToDataStreamScan(
                        externalQueryOperation.getDataStream(),
                        IntStream.range(
                                        0,
                                        externalQueryOperation.getDataStream().getType().getArity())
                                .toArray(),
                        TableSchema.fromResolvedSchema(externalQueryOperation.getResolvedSchema()));
            } else if (other instanceof ScalaExternalQueryOperation) {
                ScalaExternalQueryOperation<?> externalQueryOperation =
                        (ScalaExternalQueryOperation<?>) other;
                return convertToDataStreamScan(
                        externalQueryOperation.getDataStream(),
                        IntStream.range(
                                        0,
                                        externalQueryOperation.getDataStream().getType().getArity())
                                .toArray(),
                        TableSchema.fromResolvedSchema(externalQueryOperation.getResolvedSchema()));
            }

            throw new TableException("Unknown table operation: " + other);
        }

        @Override
        public <U> RelNode visit(TableSourceQueryOperation<U> tableSourceTable) {
            final Table relTable =
                    new TableSourceTable<>(
                            TableSchema.fromResolvedSchema(tableSourceTable.getResolvedSchema()),
                            tableSourceTable.getTableSource(),
                            !tableSourceTable.isBatch(),
                            FlinkStatistic.UNKNOWN());

            CatalogReader catalogReader = (CatalogReader) relBuilder.getRelOptSchema();

            // TableSourceScan requires a unique name of a Table for computing a digest.
            // We are using the identity hash of the TableSource object.
            String refId =
                    "unregistered_" + System.identityHashCode(tableSourceTable.getTableSource());
            return new FlinkLogicalTableSourceScan(
                    relBuilder.getCluster(),
                    relBuilder.getCluster().traitSet().replace(FlinkConventions.LOGICAL()),
                    RelOptTableImpl.create(
                            catalogReader,
                            relTable.getRowType(relBuilder.getTypeFactory()),
                            relTable,
                            Schemas.path(
                                    catalogReader.getRootSchema(),
                                    Collections.singletonList(refId))),
                    TableSchema.fromResolvedSchema(tableSourceTable.getResolvedSchema()),
                    tableSourceTable.getTableSource(),
                    Option.empty());
        }

        private RelNode convertToDataStreamScan(
                DataStream<?> dataStream, int[] fieldIndices, TableSchema tableSchema) {
            RelDataType logicalRowType =
                    relBuilder.getTypeFactory().buildLogicalRowType(tableSchema);
            RowSchema rowSchema = new RowSchema(logicalRowType);

            return new FlinkLogicalDataStreamScan(
                    relBuilder.getCluster(),
                    relBuilder.getCluster().traitSet().replace(FlinkConventions.LOGICAL()),
                    relBuilder.getRelOptSchema(),
                    dataStream,
                    fieldIndices,
                    rowSchema);
        }

        private RelNode convertToDataSetScan(DataSetQueryOperation<?> tableOperation) {
            RelDataType logicalRowType =
                    relBuilder
                            .getTypeFactory()
                            .buildLogicalRowType(
                                    TableSchema.fromResolvedSchema(
                                            tableOperation.getResolvedSchema()));

            return new FlinkLogicalDataSetScan(
                    relBuilder.getCluster(),
                    relBuilder.getCluster().traitSet().replace(FlinkConventions.LOGICAL()),
                    relBuilder.getRelOptSchema(),
                    tableOperation.getDataSet(),
                    tableOperation.getFieldIndices(),
                    logicalRowType);
        }

        private RexNode convertToRexNode(Expression expression) {
            return expressionBridge.bridge(expression).toRexNode(relBuilder);
        }

        private List<RexNode> convertToRexNodes(List<ResolvedExpression> expressions) {
            return expressions.stream()
                    .map(expressionBridge::bridge)
                    .map(expr -> expr.toRexNode(relBuilder))
                    .collect(toList());
        }

        private LogicalWindow toLogicalWindow(ResolvedGroupWindow window) {
            TypeInformation<?> windowType =
                    fromDataTypeToLegacyInfo(window.getTimeAttribute().getOutputDataType());
            WindowReference windowReference =
                    new WindowReference(window.getAlias(), new Some<>(windowType));
            switch (window.getType()) {
                case SLIDE:
                    return new SlidingGroupWindow(
                            windowReference,
                            expressionBridge.bridge(window.getTimeAttribute()),
                            window.getSize().map(expressionBridge::bridge).get(),
                            window.getSlide().map(expressionBridge::bridge).get());
                case SESSION:
                    return new SessionGroupWindow(
                            windowReference,
                            expressionBridge.bridge(window.getTimeAttribute()),
                            window.getGap().map(expressionBridge::bridge).get());
                case TUMBLE:
                    return new TumblingGroupWindow(
                            windowReference,
                            expressionBridge.bridge(window.getTimeAttribute()),
                            window.getSize().map(expressionBridge::bridge).get());
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
        public RexNode visit(CallExpression unresolvedCall) {
            final Expression[] newChildren =
                    unresolvedCall.getChildren().stream()
                            .map(
                                    expr -> {
                                        RexNode convertedNode = expr.accept(this);
                                        return (Expression) new RexPlannerExpression(convertedNode);
                                    })
                            .toArray(Expression[]::new);

            UnresolvedCallExpression newCall =
                    unresolvedCall(unresolvedCall.getFunctionDefinition(), newChildren);
            return expressionBridge.bridge(newCall).toRexNode(relBuilder);
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
            return expressionBridge.bridge(expression).toRexNode(relBuilder);
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
                    return ((Aggregation) expressionBridge.bridge(aggregate))
                            .toAggCall(aggregateName, false, relBuilder);
                }
            }
            throw new TableException("Expected named aggregate. Got: " + unresolvedCall);
        }

        @Override
        protected AggCall defaultMethod(Expression expression) {
            throw new TableException("Unexpected expression: " + expression);
        }
    }

    private class TableAggregateVisitor extends AggregateVisitor {
        @Override
        public AggCall visit(CallExpression unresolvedCall) {
            if (isFunctionOfKind(unresolvedCall, TABLE_AGGREGATE)) {
                AggFunctionCall aggFunctionCall =
                        (AggFunctionCall) expressionBridge.bridge(unresolvedCall);
                return aggFunctionCall.toAggCall(aggFunctionCall.toString(), false, relBuilder);
            }
            throw new TableException("Expected table aggregate. Got: " + unresolvedCall);
        }
    }
}
