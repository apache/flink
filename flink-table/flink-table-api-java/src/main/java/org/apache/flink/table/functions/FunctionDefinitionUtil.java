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

package org.apache.flink.table.functions;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.functions.python.utils.PythonFunctionUtils;

/**
 * A util to instantiate {@link FunctionDefinition} in the default way.
 */
public class FunctionDefinitionUtil {

	public static FunctionDefinition createFunctionDefinition(String name, String className) {
		return createJavaFunctionDefinition(name, className);
	}

	public static FunctionDefinition createFunctionDefinition(
			String name,
			String className,
			FunctionLanguage functionLanguage,
			ReadableConfig config) {
		if (functionLanguage == FunctionLanguage.PYTHON) {
			return createFunctionDefinitionInternal(
				name,
				(UserDefinedFunction) PythonFunctionUtils.getPythonFunction(className, config));
		} else {
			return createJavaFunctionDefinition(name, className);
		}
	}

	private static FunctionDefinition createJavaFunctionDefinition(
			String name,
			String className) {
		Object func;
		try {
			func = Thread.currentThread().getContextClassLoader().loadClass(className).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw new IllegalStateException(
				String.format("Failed instantiating '%s'", className), e);
		}

		return createFunctionDefinitionInternal(name, (UserDefinedFunction) func);
	}

	private static FunctionDefinition createFunctionDefinitionInternal(String name, UserDefinedFunction udf) {
		if (udf instanceof ScalarFunction || udf instanceof TableFunction) {
			// table and scalar function use the new type inference
			// once the other functions have been updated, this entire class will not be necessary
			// anymore and can be replaced with UserDefinedFunctionHelper.instantiateFunction
			return udf;
		} else if (udf instanceof AggregateFunction) {
			AggregateFunction a = (AggregateFunction) udf;

			return new AggregateFunctionDefinition(
				name,
				a,
				UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(a),
				UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(a)
			);
		} else if (udf instanceof TableAggregateFunction) {
			TableAggregateFunction a = (TableAggregateFunction) udf;

			return new TableAggregateFunctionDefinition(
				name,
				a,
				UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(a),
				UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(a)
			);
		} else {
			throw new UnsupportedOperationException(
				String.format("Function %s should be of ScalarFunction, TableFunction, AggregateFunction, or "
					+ "TableAggregateFunction", udf.getClass().getName())
			);
		}
	}
}
