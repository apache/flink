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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MappingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;

import java.util.Map;

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
	 * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input strategy
	 * infers identical types.
	 */
	public static TypeStrategy mapping(Map<InputTypeStrategy, TypeStrategy> mappings) {
		return new MappingTypeStrategy(mappings);
	}

	// --------------------------------------------------------------------------------------------

	private TypeStrategies() {
		// no instantiation
	}
}
