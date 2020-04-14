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

package org.apache.flink.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.FunctionLookup;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInferenceUtil;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A test implementation for a {@link FunctionLookup}. It mocks away a few features of a
 * {@link org.apache.flink.table.catalog.FunctionCatalog}. This class supports only a subset
 * of builtin functions because those functions still depend on planner expressions for argument
 * validation and type inference. Supported builtin functions are:
 *
 * <ul>
 *     <li>BuiltinFunctionDefinitions.EQUALS</li>
 *     <li>BuiltinFunctionDefinitions.IS_NULL</li>
 * </ul>
 *
 * <p>Pseudo functions that are executed during expression resolution e.g.:
 * <ul>
 *      <li>BuiltinFunctionDefinitions.WITH_COLUMNS</li>
 *      <li>BuiltinFunctionDefinitions.WITHOUT_COLUMNS</li>
 *      <li>BuiltinFunctionDefinitions.RANGE_TO</li>
 *      <li>BuiltinFunctionDefinitions.FLATTEN</li>
 * </ul>
 *
 * <p>Built-in functions that use the Flink's type inference stack:
 * <ul>
 *     <li>BuiltinFunctionDefinitions.ROW</li>
 * </ul>
 *
 * <p>This class supports only a simplified identifier parsing logic. It does not support escaping.
 * It just naively splits on dots. The proper logic comes with a planner implementation which is not
 * available in the API module.
 */
public final class FunctionLookupMock implements FunctionLookup {

	private final Map<FunctionIdentifier, FunctionDefinition> functions;

	public FunctionLookupMock(Map<FunctionIdentifier, FunctionDefinition> functions) {
		this.functions = functions;
	}

	@Override
	public Optional<Result> lookupFunction(String stringIdentifier) {
		// this is a simplified version for the test
		return lookupFunction(UnresolvedIdentifier.of(stringIdentifier.split("\\.")));
	}

	@Override
	public Optional<Result> lookupFunction(UnresolvedIdentifier identifier) {
		final FunctionIdentifier functionIdentifier;
		if (identifier.getCatalogName().isPresent() && identifier.getDatabaseName().isPresent()) {
			functionIdentifier = FunctionIdentifier.of(
				ObjectIdentifier.of(
					identifier.getCatalogName().get(),
					identifier.getDatabaseName().get(),
					identifier.getObjectName()));
		} else {
			functionIdentifier = FunctionIdentifier.of(identifier.getObjectName());
		}

		return Optional.ofNullable(functions.get(functionIdentifier))
			.map(func -> new Result(functionIdentifier, func));
	}

	@Override
	public Result lookupBuiltInFunction(BuiltInFunctionDefinition definition) {
		return new Result(
			FunctionIdentifier.of(definition.getName()),
			definition
		);
	}

	@Override
	public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
		return (unresolvedCall, resolvedArgs) -> {
			FunctionDefinition functionDefinition = unresolvedCall.getFunctionDefinition();
			List<DataType> argumentTypes = resolvedArgs.stream()
				.map(ResolvedExpression::getOutputDataType)
				.collect(Collectors.toList());
			if (functionDefinition.equals(BuiltInFunctionDefinitions.EQUALS)) {
				return new TypeInferenceUtil.Result(
					argumentTypes,
					null,
					DataTypes.BOOLEAN()
				);
			} else if (functionDefinition.equals(BuiltInFunctionDefinitions.IS_NULL)) {
				return new TypeInferenceUtil.Result(
					argumentTypes,
					null,
					DataTypes.BOOLEAN()
				);
			} else if (functionDefinition instanceof ScalarFunctionDefinition) {
				return new TypeInferenceUtil.Result(
					argumentTypes,
					null,
					// We do not support a full legacy type inference here. We support only a static result
					// type
					TypeConversions.fromLegacyInfoToDataType(((ScalarFunctionDefinition) functionDefinition)
						.getScalarFunction()
						.getResultType(null)));
			}

			throw new IllegalArgumentException(
				"Unsupported builtin function in the test: " + unresolvedCall);
		};
	}
}
