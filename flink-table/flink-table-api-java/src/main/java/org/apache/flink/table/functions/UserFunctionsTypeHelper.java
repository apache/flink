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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;

/**
 * Utility methods for validation and extraction of types during function registration in a
 * {@link org.apache.flink.table.catalog.FunctionCatalog}.
 */
@Internal
public class UserFunctionsTypeHelper {

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
	 * Checks if a user-defined function can be easily instantiated.
	 */
	public static void validateInstantiation(Class<?> clazz) {
		if (!InstantiationUtil.isPublic(clazz)) {
			throw new ValidationException(String.format("Function class %s is not public.", clazz.getCanonicalName()));
		} else if (!InstantiationUtil.isProperClass(clazz)) {
			throw new ValidationException(String.format(
				"Function class %s is no proper class," +
					" it is either abstract, an interface, or a primitive type.", clazz.getCanonicalName()));
		} else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
			throw new ValidationException(String.format(
				"The class %s is an inner class, but not statically accessible.", clazz.getCanonicalName()));
		}
	}

	/**
	 * Check whether this is a Scala object. It is forbidden to use {@link TableFunction} implemented
	 * by a Scala object, since concurrent risks.
	 */
	public static void validateNotSingleton(Class<?> clazz) {
		// TODO it is not a good way to check singleton. Maybe improve it further.
		if (Arrays.stream(clazz.getFields()).anyMatch(f -> f.getName().equals("MODULE$"))) {
			throw new ValidationException(String.format(
				"TableFunction implemented by class %s is a Scala object. This is forbidden because of concurrency" +
					" problems when using them.", clazz.getCanonicalName()));
		}
	}

	private UserFunctionsTypeHelper() {
	}
}
