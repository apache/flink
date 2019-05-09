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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableEnvImpl$;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionDefaultVisitor;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.FunctionDefinition;
import org.apache.flink.table.expressions.TableFunctionDefinition;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AS;
import static org.apache.flink.table.expressions.ExpressionUtils.isFunctionOfType;
import static org.apache.flink.table.expressions.FunctionDefinition.Type.TABLE_FUNCTION;

/**
 * Utility class for creating a valid {@link CalculatedTableOperation} operation.
 */
@Internal
public class CalculatedTableFactory {

	private FunctionTableCallVisitor calculatedTableCreator = new FunctionTableCallVisitor();

	/**
	 * Creates a valid {@link CalculatedTableOperation} operation.
	 *
	 * @param callExpr call to table function as expression
	 * @return valid calculated table
	 */
	public TableOperation create(Expression callExpr) {
		return callExpr.accept(calculatedTableCreator);
	}

	private class FunctionTableCallVisitor extends ApiExpressionDefaultVisitor<CalculatedTableOperation> {

		@Override
		public CalculatedTableOperation visitCall(CallExpression call) {
			FunctionDefinition definition = call.getFunctionDefinition();
			if (definition.equals(AS)) {
				return unwrapFromAlias(call);
			} else if (definition instanceof TableFunctionDefinition) {
				return createFunctionCall(
					(TableFunctionDefinition) definition,
					Collections.emptyList(),
					call.getChildren());
			} else {
				return defaultMethod(call);
			}
		}

		private CalculatedTableOperation unwrapFromAlias(CallExpression call) {
			List<Expression> children = call.getChildren();
			List<String> aliases = children.subList(1, children.size())
				.stream()
				.map(alias -> ExpressionUtils.extractValue(alias, Types.STRING)
					.orElseThrow(() -> new ValidationException("Unexpected alias: " + alias)))
				.collect(toList());

			if (!isFunctionOfType(children.get(0), TABLE_FUNCTION)) {
				throw fail();
			}

			CallExpression tableCall = (CallExpression) children.get(0);
			TableFunctionDefinition tableFunctionDefinition =
				(TableFunctionDefinition) tableCall.getFunctionDefinition();
			return createFunctionCall(tableFunctionDefinition, aliases, tableCall.getChildren());
		}

		private CalculatedTableOperation createFunctionCall(
				TableFunctionDefinition tableFunctionDefinition,
				List<String> aliases,
				List<Expression> parameters) {
			TypeInformation resultType = tableFunctionDefinition.getResultType();

			int callArity = resultType.getTotalFields();
			int aliasesSize = aliases.size();

			String[] fieldNames;
			if (aliasesSize == 0) {
				fieldNames = TableEnvImpl$.MODULE$.getFieldNames(resultType);
			} else if (aliasesSize != callArity) {
				throw new ValidationException(String.format(
					"List of column aliases must have same degree as table; " +
						"the returned table of function '%s' has " +
						"%d columns, whereas alias list has %d columns",
					tableFunctionDefinition.getName(),
					callArity,
					aliasesSize));
			} else {
				fieldNames = aliases.toArray(new String[aliasesSize]);
			}

			TypeInformation<?>[] fieldTypes = TableEnvImpl$.MODULE$.getFieldTypes(resultType);

			return new CalculatedTableOperation(
				tableFunctionDefinition.getTableFunction(),
				parameters,
				tableFunctionDefinition.getResultType(),
				new TableSchema(fieldNames, fieldTypes));
		}

		@Override
		protected CalculatedTableOperation defaultMethod(Expression expression) {
			throw fail();
		}

		private ValidationException fail() {
			return new ValidationException(
				"A lateral join only accepts a string expression which defines a table function " +
					"call that might be followed by some alias.");
		}
	}
}
