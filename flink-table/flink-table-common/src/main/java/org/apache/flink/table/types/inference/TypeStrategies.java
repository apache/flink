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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MappingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.UseArgumentTypeStrategy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
@Internal
public final class TypeStrategies {

	/**
	 * Placeholder for a missing type strategy.
	 */
	public static final TypeStrategy MISSING = new MissingTypeStrategy();

	/**
	 * Type strategy that returns a fixed {@link DataType}.
	 */
	public static TypeStrategy explicit(DataType dataType) {
		return new ExplicitTypeStrategy(dataType);
	}

	/**
	 * Type strategy that returns the n-th input argument.
	 */
	public static TypeStrategy argument(int pos) {
		return new UseArgumentTypeStrategy(pos);
	}

	/**
	 * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input strategy
	 * infers identical types.
	 */
	public static TypeStrategy mapping(Map<InputTypeStrategy, TypeStrategy> mappings) {
		return new MappingTypeStrategy(mappings);
	}

	// --------------------------------------------------------------------------------------------
	// Specific type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Type strategy that returns a {@link DataTypes#ROW()} with fields types equal to input types.
	 */
	public static final TypeStrategy ROW = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		DataTypes.Field[] fields = IntStream.range(0, argumentDataTypes.size())
			.mapToObj(idx -> DataTypes.FIELD("f" + idx, argumentDataTypes.get(idx)))
			.toArray(DataTypes.Field[]::new);

		return Optional.of(DataTypes.ROW(fields).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#MAP(DataType, DataType)} with a key type equal to type
	 * of the first argument and a value type equal to the type of second argument.
	 */
	public static final TypeStrategy MAP = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 2) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.MAP(argumentDataTypes.get(0), argumentDataTypes.get(1)).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#ARRAY(DataType)} with element type equal to the type of
	 * the first argument.
	 */
	public static final TypeStrategy ARRAY = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 1) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.ARRAY(argumentDataTypes.get(0)).notNull());
	};

	// --------------------------------------------------------------------------------------------

	private TypeStrategies() {
		// no instantiation
	}
}
