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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;

/**
 * Utility methods for validation and extraction of types during function registration in a
 * {@link org.apache.flink.table.catalog.FunctionCatalog}.
 */
@Internal
public class UserDefinedFunctionHelper {

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getReturnTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<T> getReturnTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction,
			TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = aggregateFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				0);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction) {
		return getAccumulatorTypeOfAggregateFunction(aggregateFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param aggregateFunction The AggregateFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 *
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T, ACC> TypeInformation<ACC> getAccumulatorTypeOfAggregateFunction(
			UserDefinedAggregateFunction<T, ACC> aggregateFunction,
			TypeInformation<ACC> scalaType) {

		TypeInformation<ACC> userProvidedType = aggregateFunction.getAccumulatorType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				aggregateFunction,
				UserDefinedAggregateFunction.class,
				aggregateFunction.getClass(),
				1);
		}
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(TableFunction<T> tableFunction) {
		return getReturnTypeOfTableFunction(tableFunction, null);
	}

	/**
	 * Tries to infer the TypeInformation of an AggregateFunction's accumulator type.
	 *
	 * @param tableFunction The TableFunction for which the accumulator type is inferred.
	 * @param scalaType The implicitly inferred type of the accumulator type.
	 * @return The inferred accumulator type of the AggregateFunction.
	 */
	public static <T> TypeInformation<T> getReturnTypeOfTableFunction(
			TableFunction<T> tableFunction,
			TypeInformation<T> scalaType) {

		TypeInformation<T> userProvidedType = tableFunction.getResultType();
		if (userProvidedType != null) {
			return userProvidedType;
		} else if (scalaType != null) {
			return scalaType;
		} else {
			return TypeExtractor.createTypeInfo(
				tableFunction,
				TableFunction.class,
				tableFunction.getClass(),
				0);
		}
	}

	/**
	 * Prepares a {@link UserDefinedFunction} for usage in the API.
	 */
	public static void prepareFunction(TableConfig config, UserDefinedFunction function) {
		if (function instanceof TableFunction) {
			UserDefinedFunctionHelper.validateNotSingleton(function.getClass());
		}
		UserDefinedFunctionHelper.validateInstantiation(function.getClass());
		UserDefinedFunctionHelper.cleanFunction(config, function);
	}

	/**
	 * Checks if a user-defined function can be easily instantiated.
	 */
	private static void validateInstantiation(Class<?> clazz) {
		if (!InstantiationUtil.isPublic(clazz)) {
			throw new ValidationException(String.format("Function class %s is not public.", clazz.getCanonicalName()));
		} else if (!InstantiationUtil.isProperClass(clazz)) {
			throw new ValidationException(String.format(
				"Function class %s is no proper class," +
					" it is either abstract, an interface, or a primitive type.", clazz.getCanonicalName()));
		}
	}

	/**
	 * Check whether this is a Scala object. Using Scala objects can lead to concurrency issues,
	 * e.g., due to a shared collector.
	 */
	private static void validateNotSingleton(Class<?> clazz) {
		// TODO it is not a good way to check singleton. Maybe improve it further.
		if (Arrays.stream(clazz.getFields()).anyMatch(f -> f.getName().equals("MODULE$"))) {
			throw new ValidationException(String.format(
				"Function implemented by class %s is a Scala object. This is forbidden because of concurrency" +
					" problems when using them.", clazz.getCanonicalName()));
		}
	}

	/**
	 * Modifies a function instance by removing any reference to outer classes. This enables
	 * non-static inner function classes.
	 */
	private static void cleanFunction(TableConfig config, UserDefinedFunction function) {
		final ClosureCleanerLevel level = config.getConfiguration().get(PipelineOptions.CLOSURE_CLEANER_LEVEL);
		try {
			ClosureCleaner.clean(function, level, true);
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Function class '%s' is not serializable. Make sure that the class is self-contained " +
						"(i.e. no references to outer classes) and all inner fields are serializable as well.",
					function.getClass()),
				t);
		}
	}

	private UserDefinedFunctionHelper() {
	}
}
