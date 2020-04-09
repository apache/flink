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

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A call expression converter rule that converts calls to user defined functions.
 */
public class FunctionDefinitionConvertRule implements CallExpressionConvertRule {
	@Override
	public Optional<RexNode> convert(
			CallExpression call,
			ConvertContext context) {
		TypeInference typeInference = call.getFunctionDefinition().getTypeInference(context.getDataTypeFactory());

		if (typeInference.getOutputTypeStrategy() == TypeStrategies.MISSING) {
			return Optional.empty();
		}

		switch (call.getFunctionDefinition().getKind()) {
			case SCALAR:
			case TABLE:
				List<RexNode> args = call.getChildren().stream().map(context::toRexNode).collect(Collectors.toList());

				final BridgingSqlFunction sqlFunction = BridgingSqlFunction.of(
					context.getDataTypeFactory(),
					context.getTypeFactory(),
					SqlKind.OTHER_FUNCTION,
					call.getFunctionIdentifier().orElse(null),
					call.getFunctionDefinition(),
					typeInference);

				return Optional.of(context.getRelBuilder().call(sqlFunction, args));
			default:
				return Optional.empty();
		}
	}
}
