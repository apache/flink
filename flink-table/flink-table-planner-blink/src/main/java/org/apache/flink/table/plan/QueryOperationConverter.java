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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkCalciteCatalogReader;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.AggregateVisitors;
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.LookupCallResolver;
import org.apache.flink.table.expressions.ProctimeAttribute;
import org.apache.flink.table.expressions.RexNodeConverter;
import org.apache.flink.table.expressions.RexPlannerExpression;
import org.apache.flink.table.expressions.RowtimeAttribute;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.WindowEnd;
import org.apache.flink.table.expressions.WindowReference;
import org.apache.flink.table.expressions.WindowStart;
import org.apache.flink.table.expressions.catalog.FunctionDefinitionCatalog;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.utils.TableSqlFunction;
import org.apache.flink.table.operations.AggregateOperationFactory;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.PlannerQueryOperation;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationDefaultVisitor;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.SessionGroupWindow;
import org.apache.flink.table.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.plan.schema.BatchTableSourceTable;
import org.apache.flink.table.plan.schema.FlinkRelOptTable;
import org.apache.flink.table.plan.schema.FlinkTable;
import org.apache.flink.table.plan.schema.StreamTableSourceTable;
import org.apache.flink.table.plan.schema.TypedFlinkTableFunction;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import scala.Some;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataToLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Converter from Flink's specific relational representation: {@link QueryOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class QueryOperationConverter extends QueryOperationDefaultVisitor<RelNode> {

	private final FlinkRelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
	private final LookupCallResolver callResolver;
	private final RexNodeConverter rexNodeConverter;
	private final AggregateVisitor aggregateVisitor = new AggregateVisitor();
	private final JoinExpressionVisitor joinExpressionVisitor = new JoinExpressionVisitor();

	public QueryOperationConverter(FlinkRelBuilder relBuilder, FunctionDefinitionCatalog functionCatalog) {
		this.relBuilder = relBuilder;
		this.callResolver = new LookupCallResolver(functionCatalog);
		this.rexNodeConverter = new RexNodeConverter(relBuilder);
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

			return relBuilder.project(rexNodes, asList(projection.getTableSchema().getFieldNames()), true).build();
		}

		@Override
		public RelNode visit(AggregateQueryOperation aggregate) {
			List<AggCall> aggregations = aggregate.getAggregateExpressions()
					.stream()
					.map(this::getAggCall)
					.collect(toList());

			List<RexNode> groupings = convertToRexNodes(aggregate.getGroupingExpressions());
			GroupKey groupKey = relBuilder.groupKey(groupings);
			return relBuilder.aggregate(groupKey, aggregations).build();
		}

		@Override
		public RelNode visit(WindowAggregateQueryOperation windowAggregate) {
			List<AggCall> aggregations = windowAggregate.getAggregateExpressions()
					.stream()
					.map(this::getAggCall)
					.collect(toList());

			List<RexNode> groupings = convertToRexNodes(windowAggregate.getGroupingExpressions());
			LogicalWindow logicalWindow = toLogicalWindow(windowAggregate.getGroupWindow());
			WindowReference windowReference = logicalWindow.aliasAttribute();
			List<FlinkRelBuilder.NamedWindowProperty> windowProperties = windowAggregate
					.getWindowPropertiesExpressions()
					.stream()
					.map(expr -> convertToWindowProperty(expr.accept(callResolver), windowReference))
					.collect(toList());
			GroupKey groupKey = relBuilder.groupKey(groupings);
			return relBuilder.aggregate(logicalWindow, groupKey, windowProperties, aggregations).build();
		}

		private FlinkRelBuilder.NamedWindowProperty convertToWindowProperty(Expression expression,
				WindowReference windowReference) {
			Preconditions.checkArgument(expression instanceof CallExpression, "This should never happened");
			CallExpression aliasExpr = (CallExpression) expression;
			Preconditions.checkArgument(
					BuiltInFunctionDefinitions.AS.equals(aliasExpr.getFunctionDefinition()),
					"This should never happened");
			String name = ((ValueLiteralExpression) aliasExpr.getChildren().get(1)).getValueAs(String.class)
					.orElseThrow(
							() -> new TableException("Invalid literal."));
			Expression windowPropertyExpr = aliasExpr.getChildren().get(0);
			Preconditions.checkArgument(windowPropertyExpr instanceof CallExpression, "This should never happened");
			CallExpression windowPropertyCallExpr = (CallExpression) windowPropertyExpr;
			FunctionDefinition fd = windowPropertyCallExpr.getFunctionDefinition();
			if (BuiltInFunctionDefinitions.WINDOW_START.equals(fd)) {
				return new FlinkRelBuilder.NamedWindowProperty(name, new WindowStart(windowReference));
			} else if (BuiltInFunctionDefinitions.WINDOW_END.equals(fd)) {
				return new FlinkRelBuilder.NamedWindowProperty(name, new WindowEnd(windowReference));
			} else if (BuiltInFunctionDefinitions.PROCTIME.equals(fd)) {
				return new FlinkRelBuilder.NamedWindowProperty(name, new ProctimeAttribute(windowReference));
			} else if (BuiltInFunctionDefinitions.ROWTIME.equals(fd)) {
				return new FlinkRelBuilder.NamedWindowProperty(name, new RowtimeAttribute(windowReference));
			} else {
				throw new TableException("Invalid literal.");
			}
		}

		/**
		 * Get the {@link AggCall} correspond to the aggregate expression.
		 */
		private AggCall getAggCall(Expression aggregateExpression) {
			if (AggregateOperationFactory.isTableAggFunctionCall(aggregateExpression)) {
				throw new UnsupportedOperationException("TableAggFunction is not supported yet!");
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

			return relBuilder.join(
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
			return relBuilder.sortLimit(sort.getOffset(), sort.getFetch(), rexNodes)
					.build();
		}

		@Override
		public <U> RelNode visit(CalculatedQueryOperation<U> calculatedTable) {
			DataType resultType = fromLegacyInfoToDataType(calculatedTable.getResultType());
			TableFunction<?> tableFunction = calculatedTable.getTableFunction();
			String[] fieldNames = calculatedTable.getTableSchema().getFieldNames();

			FlinkTypeFactory typeFactory = relBuilder.getTypeFactory();
			TypedFlinkTableFunction function = new TypedFlinkTableFunction(
					tableFunction, fieldNames, resultType);
			TableSqlFunction sqlFunction = new TableSqlFunction(
					tableFunction.functionIdentifier(),
					tableFunction.toString(),
					tableFunction,
					resultType,
					typeFactory,
					function);

			List<RexNode> parameters = convertToRexNodes(calculatedTable.getParameters());

			return LogicalTableFunctionScan.create(
					relBuilder.peek().getCluster(),
					Collections.emptyList(),
					relBuilder.call(sqlFunction, parameters),
					function.getElementType(null),
					function.getRowType(typeFactory, null, null),
					null);
		}

		@Override
		public RelNode visit(CatalogQueryOperation catalogTable) {
			return relBuilder.scan(catalogTable.getTablePath()).build();
		}

		@Override
		public RelNode visit(QueryOperation other) {
			if (other instanceof PlannerQueryOperation) {
				return ((PlannerQueryOperation) other).getCalciteTree();
			}

			throw new TableException("Unknown table operation: " + other);
		}

		@Override
		public <U> RelNode visit(TableSourceQueryOperation<U> tableSourceTable) {
			FlinkTable relTable;
			if (tableSourceTable.isBatch()) {
				relTable = new BatchTableSourceTable<>((BatchTableSource<U>) tableSourceTable.getTableSource(),
						FlinkStatistic.UNKNOWN());
			} else {
				relTable = new StreamTableSourceTable<>((StreamTableSource<U>) tableSourceTable.getTableSource(),
						FlinkStatistic.UNKNOWN());
			}
			FlinkCalciteCatalogReader catalogReader = (FlinkCalciteCatalogReader) relBuilder.getRelOptSchema();
			// TableSourceScan requires a unique name of a Table for computing a digest.
			// We are using the identity hash of the TableSource object.
			String refId = "unregistered_" + System.identityHashCode(tableSourceTable.getTableSource());
			return new FlinkLogicalTableSourceScan(
					relBuilder.getCluster(),
					relBuilder.getCluster().traitSet().replace(FlinkConventions.LOGICAL()),
					FlinkRelOptTable.create(
							catalogReader,
							relTable.getRowType(relBuilder.getTypeFactory()),
							Pair.left(Schemas.path(catalogReader.getRootSchema(), Collections.singletonList(refId))),
							relTable)
			);
		}

		private List<RexNode> convertToRexNodes(List<Expression> expressions) {
			return expressions
					.stream()
					.map(QueryOperationConverter.this::convertExprToRexNode)
					.collect(toList());
		}

		private LogicalWindow toLogicalWindow(ResolvedGroupWindow window) {
			DataType windowType = window.getTimeAttribute().getOutputDataType();
			WindowReference windowReference = new WindowReference(window.getAlias(),
					new Some<>(fromDataToLogicalType(windowType)));
			switch (window.getType()) {
				case SLIDE:
					return new SlidingGroupWindow(
							windowReference,
							window.getTimeAttribute(),
							window.getSize().orElseThrow(() -> new TableException("missed size parameters!")),
							window.getSlide().orElseThrow(() -> new TableException("missed slide parameters!"))
					);
				case SESSION:
					return new SessionGroupWindow(
							windowReference,
							window.getTimeAttribute(),
							window.getGap().orElseThrow(() -> new TableException("missed gap parameters!"))
					);
				case TUMBLE:
					return new TumblingGroupWindow(
							windowReference,
							window.getTimeAttribute(),
							window.getSize().orElseThrow(() -> new TableException("missed size parameters!"))
					);
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
		public RexNode visitCall(CallExpression call) {
			List<Expression> newChildren = call.getChildren().stream().map(expr -> {
				RexNode convertedNode = expr.accept(this);
				return (Expression) new RexPlannerExpression(convertedNode);
			}).collect(toList());

			CallExpression newCall = new CallExpression(call.getFunctionDefinition(), newChildren);
			return convertExprToRexNode(newCall);
		}

		@Override
		public RexNode visitFieldReference(FieldReferenceExpression fieldReference) {
			return relBuilder.field(numberOfJoinInputs, fieldReference.getInputIndex(), fieldReference.getFieldIndex());
		}

		@Override
		protected RexNode defaultMethod(Expression expression) {
			return convertExprToRexNode(expression);
		}
	}

	private class AggregateVisitor extends ExpressionDefaultVisitor<AggCall> {

		@Override
		public AggCall visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == AS) {
				String aggregateName = extractValue(call.getChildren().get(1), String.class)
						.orElseThrow(() -> new TableException("Unexpected name."));

				Expression aggregate = call.getChildren().get(0);
				if (isFunctionOfType(aggregate, AGGREGATE_FUNCTION)) {
					return aggregate.accept(callResolver).accept(
							new AggregateVisitors.AggCallVisitor(relBuilder, rexNodeConverter, aggregateName, false));
				}
			}
			throw new TableException("Expected named aggregate. Got: " + call);
		}

		@Override
		protected AggCall defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}

	private RexNode convertExprToRexNode(Expression expr) {
		return expr.accept(callResolver).accept(rexNodeConverter);
	}
}
