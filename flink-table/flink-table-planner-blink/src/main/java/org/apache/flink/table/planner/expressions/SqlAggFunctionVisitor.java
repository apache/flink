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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionDefaultVisitor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionRequirement;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;

import org.apache.calcite.sql.SqlAggFunction;

import java.util.IdentityHashMap;
import java.util.Map;

import static org.apache.flink.table.expressions.utils.ApiExpressionUtils.isFunctionOfKind;
import static org.apache.flink.table.functions.FunctionKind.AGGREGATE;
import static org.apache.flink.table.functions.FunctionKind.TABLE_AGGREGATE;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * The class to get {@link SqlAggFunctionVisitor} of CallExpression.
 */
public class SqlAggFunctionVisitor extends ExpressionDefaultVisitor<SqlAggFunction> {

	private static final Map<FunctionDefinition, SqlAggFunction> AGG_DEF_SQL_OPERATOR_MAPPING = new IdentityHashMap<>();

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

	private final FlinkTypeFactory typeFactory;

	public SqlAggFunctionVisitor(FlinkTypeFactory typeFactory) {
		this.typeFactory = typeFactory;
	}

	@Override
	public SqlAggFunction visit(CallExpression call) {
		if (!isFunctionOfKind(call, AGGREGATE) && !isFunctionOfKind(call, TABLE_AGGREGATE)) {
			defaultMethod(call);
		}

		FunctionDefinition def = call.getFunctionDefinition();
		if (AGG_DEF_SQL_OPERATOR_MAPPING.containsKey(def)) {
			return AGG_DEF_SQL_OPERATOR_MAPPING.get(def);
		}
		if (BuiltInFunctionDefinitions.DISTINCT == def) {
			Expression innerAgg = call.getChildren().get(0);
			return innerAgg.accept(this);
		}

		if (isFunctionOfKind(call, AGGREGATE)) {
			AggregateFunctionDefinition aggDef = (AggregateFunctionDefinition) def;
			AggregateFunction aggFunc = aggDef.getAggregateFunction();
			return new AggSqlFunction(
				aggFunc.functionIdentifier(),
				aggFunc.toString(),
				aggFunc,
				fromLegacyInfoToDataType(aggDef.getResultTypeInfo()),
				fromLegacyInfoToDataType(aggDef.getAccumulatorTypeInfo()),
				typeFactory,
				aggFunc.getRequirements().contains(FunctionRequirement.OVER_WINDOW_ONLY),
				scala.Option.empty());
		} else {
			TableAggregateFunctionDefinition aggDef = (TableAggregateFunctionDefinition) def;
			TableAggregateFunction aggFunc = aggDef.getTableAggregateFunction();
			return new AggSqlFunction(
				aggFunc.functionIdentifier(),
				aggFunc.toString(),
				aggFunc,
				fromLegacyInfoToDataType(aggDef.getResultTypeInfo()),
				fromLegacyInfoToDataType(aggDef.getAccumulatorTypeInfo()),
				typeFactory,
				false,
				scala.Option.empty());
		}
	}

	@Override
	protected SqlAggFunction defaultMethod(Expression expression) {
		throw new TableException("Unexpected expression: " + expression);
	}
}
