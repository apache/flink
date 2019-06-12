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

package org.apache.flink.table.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.functions.utils.AggSqlFunction;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.AGGREGATE_FUNCTION;
import static org.apache.flink.table.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * The class contains all kinds of visitors to visit Aggregate.
 */
public class AggregateVisitors {

	private static final Map<FunctionDefinition, SqlAggFunction> AGG_DEF_SQL_OPERATOR_MAPPING = new HashMap<>();

	static {
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.AVG, FlinkSqlOperatorTable.AVG);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.COUNT, FlinkSqlOperatorTable.COUNT);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MAX, FlinkSqlOperatorTable.MAX);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.MIN, FlinkSqlOperatorTable.MIN);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SUM, FlinkSqlOperatorTable.SUM);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.SUM0, FlinkSqlOperatorTable.SUM0);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.STDDEV_POP, FlinkSqlOperatorTable.STDDEV_POP);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.STDDEV_SAMP, FlinkSqlOperatorTable.STDDEV_SAMP);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.VAR_POP, FlinkSqlOperatorTable.VAR_POP);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.VAR_SAMP, FlinkSqlOperatorTable.VAR_SAMP);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(BuiltInFunctionDefinitions.COLLECT, FlinkSqlOperatorTable.COLLECT);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.DENSE_RANK, FlinkSqlOperatorTable.DENSE_RANK);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.FIRST_VALUE, FlinkSqlOperatorTable.FIRST_VALUE);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.STDDEV, FlinkSqlOperatorTable.STDDEV);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.LEAD, FlinkSqlOperatorTable.LEAD);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.LAG, FlinkSqlOperatorTable.LAG);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.LAST_VALUE, FlinkSqlOperatorTable.LAST_VALUE);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.RANK, FlinkSqlOperatorTable.RANK);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.ROW_NUMBER, FlinkSqlOperatorTable.ROW_NUMBER);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.SINGLE_VALUE, FlinkSqlOperatorTable.SINGLE_VALUE);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.CONCAT_AGG, FlinkSqlOperatorTable.CONCAT_AGG);
		AGG_DEF_SQL_OPERATOR_MAPPING.put(InternalFunctionDefinitions.VARIANCE, FlinkSqlOperatorTable.VARIANCE);
	}

	static class AggFunctionVisitor extends ExpressionDefaultVisitor<SqlAggFunction> {
		private final FlinkTypeFactory typeFactory;

		AggFunctionVisitor(FlinkTypeFactory typeFactory) {
			this.typeFactory = typeFactory;
		}

		@Override
		public SqlAggFunction visitCall(CallExpression call) {
			Preconditions.checkArgument(isFunctionOfType(call, AGGREGATE_FUNCTION));
			FunctionDefinition def = call.getFunctionDefinition();
			if (AGG_DEF_SQL_OPERATOR_MAPPING.containsKey(def)) {
				return AGG_DEF_SQL_OPERATOR_MAPPING.get(def);
			}
			if (BuiltInFunctionDefinitions.DISTINCT.equals(def)) {
				Expression innerAgg = call.getChildren().get(0);
				return innerAgg.accept(this);
			}
			if (def instanceof AggregateFunctionDefinition) {
				AggregateFunctionDefinition aggDef = (AggregateFunctionDefinition) def;
				UserDefinedAggregateFunction userDefinedAggregateFunc = aggDef.getAggregateFunction();
				if (userDefinedAggregateFunc instanceof AggregateFunction) {
					AggregateFunction aggFunc = (AggregateFunction) userDefinedAggregateFunc;
					return new AggSqlFunction(
							aggFunc.functionIdentifier(),
							aggFunc.toString(),
							aggFunc,
							fromLegacyInfoToDataType(aggDef.getResultTypeInfo()),
							fromLegacyInfoToDataType(aggDef.getAccumulatorTypeInfo()),
							typeFactory,
							aggFunc.requiresOver());
				}
			}
			throw new TableException("Unexpected AggregateFunction " + def.getName());
		}

		@Override
		protected SqlAggFunction defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}

	static class AggFunctionReturnTypeVisitor extends ExpressionDefaultVisitor<RelDataType> {

		private final FlinkTypeFactory typeFactory;
		private final RexNodeConverter rexNodeConverter;

		AggFunctionReturnTypeVisitor(FlinkTypeFactory typeFactory, RexNodeConverter rexNodeConverter) {
			this.typeFactory = typeFactory;
			this.rexNodeConverter = rexNodeConverter;
		}

		@Override
		public RelDataType visitCall(CallExpression call) {
			Preconditions.checkArgument(isFunctionOfType(call, AGGREGATE_FUNCTION));
			FunctionDefinition def = call.getFunctionDefinition();
			if (def instanceof AggregateFunctionDefinition) {
				AggregateFunctionDefinition aggDef = (AggregateFunctionDefinition) def;
				return typeFactory.createFieldTypeFromLogicalType(
						fromTypeInfoToLogicalType(aggDef.getResultTypeInfo()));
			} else if (BuiltInFunctionDefinitions.DISTINCT.equals(def) ||
					InternalFunctionDefinitions.LEAD.equals(def) ||
					InternalFunctionDefinitions.LAG.equals(def) ||
					InternalFunctionDefinitions.SINGLE_VALUE.equals(def)) {
				Expression child = call.getChildren().get(0);
				return child.accept(this);
			} else if (BuiltInFunctionDefinitions.AVG.equals(def) ||
					BuiltInFunctionDefinitions.STDDEV_POP.equals(def) ||
					BuiltInFunctionDefinitions.STDDEV_SAMP.equals(def) ||
					InternalFunctionDefinitions.STDDEV.equals(def) ||
					BuiltInFunctionDefinitions.VAR_POP.equals(def) ||
					BuiltInFunctionDefinitions.VAR_SAMP.equals(def) ||
					InternalFunctionDefinitions.VARIANCE.equals(def)) {
				RexNode inputRexNode = call.getChildren().get(0).accept(rexNodeConverter);
				return typeFactory.getTypeSystem().deriveAvgAggType(typeFactory, inputRexNode.getType());
			} else if (BuiltInFunctionDefinitions.COUNT.equals(def) ||
					InternalFunctionDefinitions.DENSE_RANK.equals(def) ||
					InternalFunctionDefinitions.RANK.equals(def) ||
					InternalFunctionDefinitions.ROW_NUMBER.equals(def)) {
				return typeFactory.createSqlType(SqlTypeName.BIGINT);
			} else if (InternalFunctionDefinitions.CONCAT_AGG.equals(def)) {
				return typeFactory.createSqlType(SqlTypeName.VARCHAR);
			} else if (BuiltInFunctionDefinitions.MAX.equals(def) ||
					BuiltInFunctionDefinitions.MIN.equals(def) ||
					InternalFunctionDefinitions.FIRST_VALUE.equals(def) ||
					InternalFunctionDefinitions.LAST_VALUE.equals(def)) {
				RexNode inputRexNode = call.getChildren().get(0).accept(rexNodeConverter);
				return inputRexNode.getType();
			} else if (BuiltInFunctionDefinitions.SUM0.equals(def)) {
				RexNode inputRexNode = call.getChildren().get(0).accept(rexNodeConverter);
				return typeFactory.getTypeSystem().deriveSumType(typeFactory, inputRexNode.getType());
			} else if (BuiltInFunctionDefinitions.SUM.equals(def)) {
				RexNode inputRexNode = call.getChildren().get(0).accept(rexNodeConverter);
				// Sum return type is always nullable
				return typeFactory.createTypeWithNullability(
						typeFactory.getTypeSystem().deriveSumType(typeFactory, inputRexNode.getType()),
						true);
			} else if (BuiltInFunctionDefinitions.COLLECT.equals(def)) {
				RexNode inputRexNode = call.getChildren().get(0).accept(rexNodeConverter);
				MultisetType multisetType = new MultisetType(toLogicalType(inputRexNode.getType()));
				return typeFactory.createFieldTypeFromLogicalType(multisetType);
			} else {
				throw new TableException("Unexpected AggregateFunction " + def.getName());
			}
		}

		@Override
		protected RelDataType defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}

	/**
	 * The class to get AggCall of Aggregate Expression.
	 */
	public static class AggCallVisitor extends ExpressionDefaultVisitor<RelBuilder.AggCall> {

		private final RelBuilder relBuilder;
		private final FlinkTypeFactory typeFactory;
		private final RexNodeConverter rexNodeConverter;
		private final String name;
		private final boolean isDistinct;

		public AggCallVisitor(RelBuilder relBuilder, RexNodeConverter rexNodeConverter, String name,
				boolean isDistinct) {
			this.relBuilder = relBuilder;
			this.typeFactory = (FlinkTypeFactory) relBuilder.getTypeFactory();
			this.rexNodeConverter = rexNodeConverter;
			this.name = name;
			this.isDistinct = isDistinct;
		}

		@Override
		public RelBuilder.AggCall visitCall(CallExpression call) {
			Preconditions.checkArgument(isFunctionOfType(call, AGGREGATE_FUNCTION));
			FunctionDefinition def = call.getFunctionDefinition();

			if (def instanceof AggregateFunctionDefinition) {
				AggregateFunctionDefinition aggDef = (AggregateFunctionDefinition) def;
				UserDefinedAggregateFunction userDefinedAggregateFunc = aggDef.getAggregateFunction();
				if (userDefinedAggregateFunc instanceof AggregateFunction) {
					AggregateFunction aggFunc = (AggregateFunction) userDefinedAggregateFunc;
					SqlAggFunction sqlAggFunction = new AggSqlFunction(
							aggFunc.functionIdentifier(),
							aggFunc.toString(),
							aggFunc,
							fromLegacyInfoToDataType(aggDef.getResultTypeInfo()),
							fromLegacyInfoToDataType(aggDef.getAccumulatorTypeInfo()),
							typeFactory,
							aggFunc.requiresOver());
					return relBuilder.aggregateCall(
							sqlAggFunction,
							isDistinct,
							false,
							null,
							name,
							call.getChildren().stream().map(expr -> expr.accept(rexNodeConverter))
									.collect(Collectors.toList())
					);
				} else {
					throw new UnsupportedOperationException(def.getName());
				}
			}
			if (BuiltInFunctionDefinitions.DISTINCT.equals(def)) {
				Expression innerAgg = call.getChildren().get(0);
				return innerAgg.accept(new AggCallVisitor(relBuilder, rexNodeConverter, name, true));
			}
			Preconditions.checkArgument(AGG_DEF_SQL_OPERATOR_MAPPING.containsKey(def));

			return relBuilder.aggregateCall(
					AGG_DEF_SQL_OPERATOR_MAPPING.get(def),
					isDistinct,
					false,
					null,
					name,
					call.getChildren().stream().map(expr -> expr.accept(rexNodeConverter))
							.collect(Collectors.toList()));

		}

		@Override
		protected RelBuilder.AggCall defaultMethod(Expression expression) {
			throw new TableException("Unexpected expression: " + expression);
		}
	}
}
