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
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.functions.utils.AggSqlFunction;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
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
	}

	static class AggFunctionVisitor extends ExpressionDefaultVisitor<SqlAggFunction> {
		private final FlinkTypeFactory typeFactory;

		AggFunctionVisitor(FlinkTypeFactory typeFactory) {
			this.typeFactory = typeFactory;
		}

		@Override
		public SqlAggFunction visit(CallExpression call) {
			Preconditions.checkArgument(isFunctionOfKind(call, AGGREGATE));
			FunctionDefinition def = call.getFunctionDefinition();
			if (AGG_DEF_SQL_OPERATOR_MAPPING.containsKey(def)) {
				return AGG_DEF_SQL_OPERATOR_MAPPING.get(def);
			}
			if (BuiltInFunctionDefinitions.DISTINCT.equals(def)) {
				Expression innerAgg = call.getChildren().get(0);
				return innerAgg.accept(this);
			}
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
			} else {
				throw new UnsupportedOperationException("TableAggregateFunction is not supported yet!");
			}
		}

		@Override
		protected SqlAggFunction defaultMethod(Expression expression) {
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
		public RelBuilder.AggCall visit(CallExpression call) {
			Preconditions.checkArgument(isFunctionOfKind(call, AGGREGATE));
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
					throw new UnsupportedOperationException("TableAggregateFunction is not supported yet!");
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
