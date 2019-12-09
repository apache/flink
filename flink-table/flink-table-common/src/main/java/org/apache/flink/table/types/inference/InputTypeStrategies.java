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
import org.apache.flink.table.types.inference.strategies.BridgingInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.BridgingInputTypeStrategy.BridgingSignature;
import org.apache.flink.table.types.inference.strategies.ExplicitInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.NopInputTypeStrategy;
import org.apache.flink.table.types.inference.strategies.OutputTypeInputTypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.List;

/**
 * Strategies for inferring missing or incomplete input argument data types.
 *
 * @see InputTypeStrategy
 */
@Internal
public final class InputTypeStrategies {

	/**
	 * Input strategy that does nothing.
	 */
	public static final InputTypeStrategy NOP = new NopInputTypeStrategy();

	/**
	 * Input type strategy that supplies the function's output {@link DataType} for each unknown
	 * argument if available.
	 */
	public static final OutputTypeInputTypeStrategy OUTPUT_TYPE = new OutputTypeInputTypeStrategy();

	/**
	 * Input type strategy that supplies a fixed {@link DataType} for each argument.
	 */
	public static ExplicitInputTypeStrategy explicit(DataType... dataTypes) {
		return new ExplicitInputTypeStrategy(Arrays.asList(dataTypes));
	}

	/**
	 * Special input type strategy for enriching data types with an expected conversion class. This
	 * is in particular useful when a data type has been created out of a {@link LogicalType} but
	 * runtime hints are still missing.
	 */
	public static BridgingInputTypeStrategy bridging(List<BridgingSignature> bridgingSignatures) {
		return new BridgingInputTypeStrategy(bridgingSignatures);
	}

	// --------------------------------------------------------------------------------------------

	private InputTypeStrategies() {
		// no instantiation
	}
}
