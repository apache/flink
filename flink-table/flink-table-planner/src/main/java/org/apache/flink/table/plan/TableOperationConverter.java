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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.expressions.RexPlannerExpression;
import org.apache.flink.table.expressions.WindowReference;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.utils.TableSqlFunction;
import org.apache.flink.table.operations.AggregateOperationFactory;
import org.apache.flink.table.operations.AggregateTableOperation;
import org.apache.flink.table.operations.CalculatedTableOperation;
import org.apache.flink.table.operations.CatalogTableOperation;
import org.apache.flink.table.operations.DistinctTableOperation;
import org.apache.flink.table.operations.FilterTableOperation;
import org.apache.flink.table.operations.JoinTableOperation;
import org.apache.flink.table.operations.JoinTableOperation.JoinType;
import org.apache.flink.table.operations.PlannerTableOperation;
import org.apache.flink.table.operations.ProjectTableOperation;
import org.apache.flink.table.operations.SetTableOperation;
import org.apache.flink.table.operations.SortTableOperation;
import org.apache.flink.table.operations.TableOperation;
import org.apache.flink.table.operations.TableOperationDefaultVisitor;
import org.apache.flink.table.operations.TableOperationVisitor;
import org.apache.flink.table.operations.WindowAggregateTableOperation;
import org.apache.flink.table.operations.WindowAggregateTableOperation.ResolvedGroupWindow;
import org.apache.flink.table.plan.logical.LogicalWindow;
import org.apache.flink.table.plan.logical.SessionGroupWindow;
import org.apache.flink.table.plan.logical.SlidingGroupWindow;
import org.apache.flink.table.plan.logical.TumblingGroupWindow;
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.GroupKey;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import scala.Some;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.ExpressionUtils.extractValue;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;

/**
 * Converter from Flink's specific relational representation: {@link TableOperation} to Calcite's specific relational
 * representation: {@link RelNode}.
 */
@Internal
public class TableOperationConverter extends TableOperationDefaultVisitor<RelNode> {

	/**
	 * Supplier for {@link TableOperationConverter} that can wrap given {@link RelBuilder}.
	 */
	@Internal
	public static class ToRelConverterSupplier {
		private final ExpressionBridge<PlannerExpression> expressionBridge;

		public ToRelConverterSupplier(ExpressionBridge<PlannerExpression> expressionBridge) {
			this.expressionBridge = expressionBridge;
		}

		public TableOperationConverter get(RelBuilder relBuilder) {
			return new TableOperationConverter(relBuilder, expressionBridge);
		}
	}

	private final RelBuilder relBuilder;
	private final SingleRelVisitor singleRelVisitor = new SingleRelVisitor();
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final AggregateVisitor aggregateVisitor = new AggregateVisitor();
	private final TableAggregateVisitor tableAggregateVisitor = new TableAggregateVisitor();
	private final JoinExpressionVisitor joinExpressionVisitor = new JoinExpressionVisitor();

	public TableOperationConverter(
			RelBuilder relBuilder,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		this.relBuilder = relBuilder;
		this.expressionBridge = expressionBridge;
	}

	@Override
	public RelNode defaultMethod(TableOperation other) {
		other.getChildren().forEach(child -> relBuilder.push(child.accept(this)));
		return other.accept(singleRelVisitor);
	}

	private class SingleRelVisitor implements TableOperationVisitor<RelNode> {

		@Override
		public RelNode visitProject(ProjectTableOperation projection) {
			List<RexNode> rexNodes = convertToRexNodes(projection.getProjectList());

			return relBuilder.project(rexNodes, asList(projection.getTableSchema().getFieldNames()), true).build();
		}

		@Override
		public RelNode visitAggregate(AggregateTableOperation aggregate) {
			boolean isTableAggregate = aggregate.getAggregateExpressions().size() == 1 &&
				AggregateOperationFactory.isTableAggFunctionCall(aggregate.getAggregateExpressions().get(0));

			List<AggCall> aggregations = aggregate.getAggregateExpressions()
				.stream()
				.map(expr -> {
					if (isTableAggregate) {
						return expr.accept(tableAggregateVisitor);
					} else {
						return expr.accept(aggregateVisitor);
					}
				}).collect(toList());

			List<RexNode> groupings = convertToRexNodes(aggregate.getGroupingExpressions());
			GroupKey groupKey = relBuilder.groupKey(groupings);

			if (isTableAggregate) {
				return ((FlinkRelBuilder) relBuilder).tableAggregate(groupKey, aggregations).build();
			} else {
				return relBuilder.aggregate(groupKey, aggregations).build();
			}
		}

		@Override
		public RelNode visitWindowAggregate(WindowAggregateTableOperation windowAggregate) {
			FlinkRelBuilder flinkRelBuilder = (FlinkRelBuilder) relBuilder;
			List<AggCall> aggregations = windowAggregate.getAggregateExpressions()
				.stream()
				.map(expr -> expr.accept(aggregateVisitor))
				.collect(toList());

			List<RexNode> groupings = convertToRexNodes(windowAggregate.getGroupingExpressions());
			List<PlannerExpression> windowProperties = windowAggregate.getWindowPropertiesExpressions()
				.stream()
				.map(expressionBridge::bridge)
				.collect(toList());
			GroupKey groupKey = relBuilder.groupKey(groupings);
			LogicalWindow logicalWindow = toLogicalWindow(windowAggregate.getGroupWindow());
			return flinkRelBuilder.aggregate(logicalWindow, groupKey, windowProperties, aggregations).build();
		}

		@Override
		public RelNode visitJoin(JoinTableOperation join) {
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
		public RelNode visitSetOperation(SetTableOperation setOperation) {
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
		public RelNode visitFilter(FilterTableOperation filter) {
			RexNode rexNode = convertToRexNode(filter.getCondition());
			return relBuilder.filter(rexNode).build();
		}

		@Override
		public RelNode visitDistinct(DistinctTableOperation distinct) {
			return relBuilder.distinct().build();
		}

		@Override
		public RelNode visitSort(SortTableOperation sort) {
			List<RexNode> rexNodes = convertToRexNodes(sort.getOrder());
			return relBuilder.sortLimit(sort.getOffset(), sort.getFetch(), rexNodes)
				.build();
		}

		@Override
		public <U> RelNode visitCalculatedTable(CalculatedTableOperation<U> calculatedTable) {
			String[] fieldNames = calculatedTable.getTableSchema().getFieldNames();
			int[] fieldIndices = IntStream.range(0, fieldNames.length).toArray();
			TypeInformation<U> resultType = calculatedTable.getResultType();

			FlinkTableFunctionImpl function = new FlinkTableFunctionImpl<>(
				resultType,
				fieldIndices,
				fieldNames);
			TableFunction<?> tableFunction = calculatedTable.getTableFunction();

			FlinkTypeFactory typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
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
				function.getRowType(typeFactory, null),
				null);
		}

		@Override
		public RelNode visitCatalogTable(CatalogTableOperation catalogTable) {
			return relBuilder.scan(catalogTable.getTablePath()).build();
		}

		@Override
		public RelNode visitOther(TableOperation other) {
			if (other instanceof PlannerTableOperation) {
				return ((PlannerTableOperation) other).getCalciteTree();
			}

			throw new TableException("Unknown table operation: " + other);
		}

		private RexNode convertToRexNode(Expression expression) {
			return expressionBridge.bridge(expression).toRexNode(relBuilder);
		}

		private List<RexNode> convertToRexNodes(List<Expression> expressions) {
			return expressions
				.stream()
				.map(expressionBridge::bridge)
				.map(expr -> expr.toRexNode(relBuilder))
				.collect(toList());
		}

		private LogicalWindow toLogicalWindow(ResolvedGroupWindow window) {
			TypeInformation<?> windowType = window.getTimeAttribute().getResultType();
			WindowReference windowReference = new WindowReference(window.getAlias(), new Some<>(windowType));
			switch (window.getType()) {
				case SLIDE:
					return new SlidingGroupWindow(
						windowReference,
						expressionBridge.bridge(window.getTimeAttribute()),
						window.getSize().map(expressionBridge::bridge).get(),
						window.getSlide().map(expressionBridge::bridge).get()
					);
				case SESSION:
					return new SessionGroupWindow(
						windowReference,
						expressionBridge.bridge(window.getTimeAttribute()),
						window.getGap().map(expressionBridge::bridge).get()
					);
				case TUMBLE:
					return new TumblingGroupWindow(
						windowReference,
						expressionBridge.bridge(window.getTimeAttribute()),
						window.getSize().map(expressionBridge::bridge).get()
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
			return expressionBridge.bridge(newCall).toRexNode(relBuilder);
		}

		@Override
		public RexNode visitFieldReference(FieldReferenceExpression fieldReference) {
			return relBuilder.field(numberOfJoinInputs, fieldReference.getInputIndex(), fieldReference.getFieldIndex());
		}

		@Override
		protected RexNode defaultMethod(Expression expression) {
			return expressionBridge.bridge(expression).toRexNode(relBuilder);
		}
	}

	private class AggregateVisitor extends ExpressionDefaultVisitor<AggCall> {

		@Override
		public AggCall visitCall(CallExpression call) {
			if (call.getFunctionDefinition() == AS) {
				String aggregateName = extractValue(call.getChildren().get(1), Types.STRING)
					.orElseThrow(() -> new TableException("Unexpected name"));

				Expression aggregate = call.getChildren().get(0);
				if (isFunctionOfType(aggregate, AGGREGATE_FUNCTION)) {
					return ((Aggregation) expressionBridge.bridge(aggregate))
						.toAggCall(aggregateName, false, relBuilder);
				}
			}
			throw new TableException("Expected named aggregate. Got: " + call);
		}

		@Override
		protected AggCall defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}

	private class TableAggregateVisitor extends AggregateVisitor {
		@Override
		public AggCall visitCall(CallExpression call) {
			if (isFunctionOfType(call, AGGREGATE_FUNCTION)) {
				AggFunctionCall aggFunctionCall = (AggFunctionCall) expressionBridge.bridge(call);
				return aggFunctionCall.toAggCall(aggFunctionCall.toString(), false, relBuilder);
			}
			throw new TableException("Expected table aggregate. Got: " + call);
		}
	}
}
