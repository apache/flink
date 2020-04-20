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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.io.IOException;
import java.util.List;

import scala.Some;

import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeGetResultType;
import static org.apache.flink.table.planner.functions.utils.HiveFunctionUtils.invokeSetArgs;
import static org.apache.flink.table.runtime.types.ClassLogicalTypeConverter.getDefaultExternalClassForType;

/**
 * Hive {@link ScalarSqlFunction}.
 * Override getFunction to clone function and invoke {@code HiveScalarFunction#setArgumentTypesAndConstants}.
 * Override SqlReturnTypeInference to invoke {@code HiveScalarFunction#getHiveResultType} instead of
 * {@code HiveScalarFunction#getResultType(Class[])}.
 *
 * @deprecated TODO hack code, its logical should be integrated to ScalarSqlFunction
 */
@Deprecated
public class HiveScalarSqlFunction extends ScalarSqlFunction {

	private final ScalarFunction function;

	public HiveScalarSqlFunction(
			FunctionIdentifier identifier, ScalarFunction function, FlinkTypeFactory typeFactory) {
		super(identifier, identifier.toString(), function,
				typeFactory, new Some<>(createReturnTypeInference(function, typeFactory)));
		this.function = function;
	}

	@Override
	public ScalarFunction makeFunction(Object[] constantArguments, LogicalType[] argTypes) {
		ScalarFunction clone;
		try {
			clone = InstantiationUtil.clone(function);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return (ScalarFunction) invokeSetArgs(clone, constantArguments, argTypes);
	}

	private static SqlReturnTypeInference createReturnTypeInference(
			ScalarFunction function, FlinkTypeFactory typeFactory) {
		return opBinding -> {
			List<RelDataType> sqlTypes = opBinding.collectOperandTypes();
			LogicalType[] parameters = UserDefinedFunctionUtils.getOperandTypeArray(opBinding);

			Object[] constantArguments = new Object[sqlTypes.size()];
			for (int i = 0; i < sqlTypes.size(); i++) {
				if (!opBinding.isOperandNull(i, false) && opBinding.isOperandLiteral(i, false)) {
					constantArguments[i] = opBinding.getOperandLiteralValue(
							i, getDefaultExternalClassForType(parameters[i]));
				}
			}
			return invokeGetResultType(function, constantArguments, parameters, typeFactory);
		};
	}
}
