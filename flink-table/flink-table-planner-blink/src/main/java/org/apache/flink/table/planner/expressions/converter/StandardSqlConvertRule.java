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

package org.apache.flink.table.planner.expressions.converter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.lang.reflect.Field;
import java.util.Optional;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;

/**
 * Use standard sql to convert {@link CallExpression} to RexNode.
 */
public class StandardSqlConvertRule implements CallExpressionConvertRule {

	@Override
	public Optional<RexNode> convert(CallExpression call, ConvertContext context) {
		FunctionDefinition def = call.getFunctionDefinition();
		Optional<String> standardSql = def instanceof BuiltInFunctionDefinition ?
				((BuiltInFunctionDefinition) def).getStandardSql() :
				Optional.empty();
		return standardSql.map(s -> {
			try {
				Field field = SqlStdOperatorTable.class.getField(s);
				RexNode rexNode = context.getRelBuilder().call(
						(SqlOperator) field.get(SqlStdOperatorTable.class),
						toRexNodes(context, call.getChildren()));
				LogicalType flinkType = call.getOutputDataType().getLogicalType();
				LogicalType calciteType = FlinkTypeFactory.toLogicalType(rexNode.getType());
				if (!flinkType.equals(calciteType)) {
					throw new TableException(String.format(
							"Function definition output type(%s) should be the same as calcite output type(%s)",
							flinkType,
							calciteType));
				}
				return rexNode;
			} catch (NoSuchFieldException | IllegalAccessException e) {
				throw new TableException("Wrong standardSql: " + s, e);
			}
		});
	}
}
