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
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.resolver.LookupCallResolver;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.operations.AggregateQueryOperation;
import org.apache.flink.table.operations.CalculatedQueryOperation;
import org.apache.flink.table.operations.CatalogQueryOperation;
import org.apache.flink.table.operations.DistinctQueryOperation;
import org.apache.flink.table.operations.FilterQueryOperation;
import org.apache.flink.table.operations.JavaDataStreamQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation;
import org.apache.flink.table.operations.JoinQueryOperation.JoinType;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;
import org.apache.flink.table.operations.ScalaDataStreamQueryOperation;
import org.apache.flink.table.operations.SetQueryOperation;
import org.apache.flink.table.operations.SortQueryOperation;
import org.apache.flink.table.operations.TableSourceQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation;
import org.apache.flink.table.operations.WindowAggregateQueryOperation.ResolvedGroupWindow;
import org.apache.flink.table.operations.utils.QueryOperationDefaultVisitor;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.PlannerProctimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerRowtimeAttribute;
import org.apache.flink.table.planner.expressions.PlannerWindowEnd;
import org.apache.flink.table.planner.expressions.PlannerWindowReference;
import org.apache.flink.table.planner.expressions.PlannerWindowStart;
import org.apache.flink.table.planner.expressions.RexNodeConverter;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.expressions.SqlAggFunctionVisitor;
import org.apache.flink.table.planner.functions.utils.TableSqlFunction;
import org.apache.flink.table.planner.operations.DataStreamQueryOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.SessionGroupWindow;
import org.apache.flink.table.planner.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.planner.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.planner.plan.schema.DataStreamTable;
import org.apache.flink.table.planner.plan.schema.FlinkRelOptTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.schema.TypedFlinkTableFunction;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Some;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
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
	private final TableAggregateVisitor tableAggregateVisitor = new TableAggregateVisitor();
	private final JoinExpressionVisitor joinExpressionVisitor = new JoinExpressionVisitor();

	public QueryOperationConverter(FlinkRelBuilder relBuilder, FunctionLookup functionCatalog) {
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
			PlannerWindowReference windowReference = logicalWindow.aliasAttribute();
			List<FlinkRelBuilder.PlannerNamedWindowProperty> windowProperties = windowAggregate
					.getWindowPropertiesExpressions()
					.stream()
					.map(expr -> convertToWindowProperty(expr.accept(callResolver), windowReference))
					.collect(toList());
			GroupKey groupKey = relBuilder.groupKey(groupings);
			return relBuilder.windowAggregate(logicalWindow, groupKey, windowProperties, aggregations).build();
		}

		private FlinkRelBuilder.PlannerNamedWindowProperty convertToWindowProperty(Expression expression,
				PlannerWindowReference windowReference) {
			Preconditions.checkArgument(expression instanceof CallExpression, "This should never happened");
			CallExpression aliasExpr = (CallExpression) expression;
			Preconditions.checkArgument(
					BuiltInFunctionDefinitions.AS == aliasExpr.getFunctionDefinition(),
					"This should never happened");
			String name = ((ValueLiteralExpression) aliasExpr.getChildren().get(1)).getValueAs(String.class)
					.orElseThrow(
							() -> new TableException("Invalid literal."));
			Expression windowPropertyExpr = aliasExpr.getChildren().get(0);
			Preconditions.checkArgument(windowPropertyExpr instanceof CallExpression, "This should never happened");
			CallExpression windowPropertyCallExpr = (CallExpression) windowPropertyExpr;
			FunctionDefinition fd = windowPropertyCallExpr.getFunctionDefinition();
			if (BuiltInFunctionDefinitions.WINDOW_START == fd) {
				return new FlinkRelBuilder.PlannerNamedWindowProperty(name, new PlannerWindowStart(windowReference));
			} else if (BuiltInFunctionDefinitions.WINDOW_END == fd) {
				return new FlinkRelBuilder.PlannerNamedWindowProperty(name, new PlannerWindowEnd(windowReference));
			} else if (BuiltInFunctionDefinitions.PROCTIME == fd) {
				return new FlinkRelBuilder.PlannerNamedWindowProperty(name, new PlannerProctimeAttribute(windowReference));
			} else if (BuiltInFunctionDefinitions.ROWTIME == fd) {
				return new FlinkRelBuilder.PlannerNamedWindowProperty(name, new PlannerRowtimeAttribute(windowReference));
			} else {
				throw new TableException("Invalid literal.");
			}
		}

		/**
		 * Get the {@link AggCall} correspond to the aggregate or table aggregate expression.
		 */
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

			TypedFlinkTableFunction function = new TypedFlinkTableFunction(
					tableFunction, fieldNames, resultType);

			FlinkTypeFactory typeFactory = relBuilder.getTypeFactory();

			TableSqlFunction sqlFunction = new TableSqlFunction(
					tableFunction.functionIdentifier(),
					tableFunction.toString(),
					tableFunction,
					resultType,
					typeFactory,
					function,
					scala.Option.empty());

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
			} else if (other instanceof DataStreamQueryOperation) {
				return convertToDataStreamScan((DataStreamQueryOperation<?>) other);
			} else if (other instanceof JavaDataStreamQueryOperation) {
				JavaDataStreamQueryOperation<?> dataStreamQueryOperation = (JavaDataStreamQueryOperation<?>) other;
				return convertToDataStreamScan(
					dataStreamQueryOperation.getDataStream(),
					dataStreamQueryOperation.getFieldIndices(),
					dataStreamQueryOperation.getTableSchema());
			} else if (other instanceof ScalaDataStreamQueryOperation) {
				ScalaDataStreamQueryOperation dataStreamQueryOperation = (ScalaDataStreamQueryOperation<?>) other;
				return convertToDataStreamScan(
					dataStreamQueryOperation.getDataStream(),
					dataStreamQueryOperation.getFieldIndices(),
					dataStreamQueryOperation.getTableSchema());
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
				throw new TableException(String.format("%s is not supported.", tableSource.getClass().getSimpleName()));
			}

			FlinkStatistic statistic;
			List<String> names;
			if (tableSourceOperation instanceof RichTableSourceQueryOperation &&
				((RichTableSourceQueryOperation<U>) tableSourceOperation).getQualifiedName() != null) {
				statistic = ((RichTableSourceQueryOperation<U>) tableSourceOperation).getStatistic();
				names = ((RichTableSourceQueryOperation<U>) tableSourceOperation).getQualifiedName();
			} else {
				statistic = FlinkStatistic.UNKNOWN();
				// TableSourceScan requires a unique name of a Table for computing a digest.
				// We are using the identity hash of the TableSource object.
				String refId = "Unregistered_TableSource_" + System.identityHashCode(tableSource);
				names = Collections.singletonList(refId);
			}

			TableSourceTable<?> tableSourceTable = new TableSourceTable<>(tableSource, !isBatch, statistic);
			FlinkRelOptTable table = FlinkRelOptTable.create(
				relBuilder.getRelOptSchema(),
				tableSourceTable.getRowType(relBuilder.getTypeFactory()),
				names,
				tableSourceTable);
			return LogicalTableScan.create(relBuilder.getCluster(), table);
		}

		private RelNode convertToDataStreamScan(DataStreamQueryOperation<?> operation) {
			DataStreamTable<?> dataStreamTable = new DataStreamTable<>(
					operation.getDataStream(),
					operation.isProducesUpdates(),
					operation.isAccRetract(),
					operation.getFieldIndices(),
					operation.getTableSchema().getFieldNames(),
					operation.getStatistic(),
					scala.Option.apply(operation.getFieldNullables()));

			List<String> names;
			if (operation.getQualifiedName() != null) {
				names = operation.getQualifiedName();
			} else {
				String refId = String.format("Unregistered_DataStream_%s", operation.getDataStream().getId());
				names = Collections.singletonList(refId);
			}

			FlinkRelOptTable table = FlinkRelOptTable.create(
					relBuilder.getRelOptSchema(),
					dataStreamTable.getRowType(relBuilder.getTypeFactory()),
					names,
					dataStreamTable);
			return LogicalTableScan.create(relBuilder.getCluster(), table);
		}

		private RelNode convertToDataStreamScan(DataStream<?> dataStream, int[] fieldIndices, TableSchema tableSchema) {
			DataStreamTable<?> dataStreamTable = new DataStreamTable<>(
				dataStream,
				false,
				false,
				fieldIndices,
				tableSchema.getFieldNames(),
				FlinkStatistic.UNKNOWN(),
				scala.Option.empty());

			String refId = String.format("Unregistered_DataStream_%s", dataStream.getId());
			FlinkRelOptTable table = FlinkRelOptTable.create(
				relBuilder.getRelOptSchema(),
				dataStreamTable.getRowType(relBuilder.getTypeFactory()),
				Collections.singletonList(refId),
				dataStreamTable);
			return LogicalTableScan.create(relBuilder.getCluster(), table);
		}

		private List<RexNode> convertToRexNodes(List<ResolvedExpression> expressions) {
			return expressions
					.stream()
					.map(QueryOperationConverter.this::convertExprToRexNode)
					.collect(toList());
		}

		private LogicalWindow toLogicalWindow(ResolvedGroupWindow window) {
			DataType windowType = window.getTimeAttribute().getOutputDataType();
			PlannerWindowReference windowReference = new PlannerWindowReference(window.getAlias(),
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
		public RexNode visit(CallExpression callExpression) {
			final List<ResolvedExpression> newChildren = callExpression.getChildren().stream().map(expr -> {
				RexNode convertedNode = expr.accept(this);
				return new RexNodeExpression(convertedNode, ((ResolvedExpression) expr).getOutputDataType());
			}).collect(Collectors.toList());

			CallExpression newCall = new CallExpression(
					callExpression.getObjectIdentifier().get(), callExpression.getFunctionDefinition(), newChildren,
					callExpression.getOutputDataType());
			return convertExprToRexNode(newCall);
		}

		@Override
		public RexNode visit(FieldReferenceExpression fieldReference) {
			return relBuilder.field(numberOfJoinInputs, fieldReference.getInputIndex(), fieldReference.getFieldIndex());
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
				String aggregateName = extractValue(unresolvedCall.getChildren().get(1), String.class)
						.orElseThrow(() -> new TableException("Unexpected name."));

				Expression aggregate = unresolvedCall.getChildren().get(0);
				if (isFunctionOfKind(aggregate, AGGREGATE)) {
					return aggregate.accept(callResolver).accept(
							new AggCallVisitor(relBuilder, rexNodeConverter, aggregateName, false));
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
			private final RexNodeConverter rexNodeConverter;
			private final String name;
			private final boolean isDistinct;

			public AggCallVisitor(RelBuilder relBuilder, RexNodeConverter rexNodeConverter, String name,
					boolean isDistinct) {
				this.relBuilder = relBuilder;
				this.sqlAggFunctionVisitor = new SqlAggFunctionVisitor((FlinkTypeFactory) relBuilder.getTypeFactory());
				this.rexNodeConverter = rexNodeConverter;
				this.name = name;
				this.isDistinct = isDistinct;
			}

			@Override
			public RelBuilder.AggCall visit(CallExpression call) {
				FunctionDefinition def = call.getFunctionDefinition();
				if (BuiltInFunctionDefinitions.DISTINCT == def) {
					Expression innerAgg = call.getChildren().get(0);
					return innerAgg.accept(new AggCallVisitor(relBuilder, rexNodeConverter, name, true));
				} else {
					SqlAggFunction sqlAggFunction = call.accept(sqlAggFunctionVisitor);
					return relBuilder.aggregateCall(
						sqlAggFunction,
						isDistinct,
						false,
						null,
						name,
						call.getChildren().stream().map(expr -> expr.accept(rexNodeConverter))
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
				return call.accept(new TableAggCallVisitor(relBuilder, rexNodeConverter));
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
			private final RexNodeConverter rexNodeConverter;

			public TableAggCallVisitor(RelBuilder relBuilder, RexNodeConverter rexNodeConverter) {
				this.relBuilder = relBuilder;
				this.sqlAggFunctionVisitor = new SqlAggFunctionVisitor((FlinkTypeFactory) relBuilder.getTypeFactory());
				this.rexNodeConverter = rexNodeConverter;
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
					call.getChildren().stream().map(expr -> expr.accept(rexNodeConverter)).collect(toList()));
			}

			@Override
			protected RelBuilder.AggCall defaultMethod(Expression expression) {
				throw new TableException("Expected table aggregate. Got: " + expression);
			}
		}
	}

	private RexNode convertExprToRexNode(Expression expr) {
		return expr.accept(callResolver).accept(rexNodeConverter);
	}
}
