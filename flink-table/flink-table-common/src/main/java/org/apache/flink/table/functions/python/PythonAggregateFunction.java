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

package org.apache.flink.table.functions.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * The wrapper of user defined python aggregate function.
 */
@Internal
public class PythonAggregateFunction extends AggregateFunction implements PythonFunction {

	private static final long serialVersionUID = 1L;

	private final String name;
	private final byte[] serializedAggregateFunction;
	private final DataType[] inputTypes;
	private final DataType resultType;
	private final DataType accumulatorType;
	private final PythonFunctionKind pythonFunctionKind;
	private final boolean deterministic;
	private final PythonEnv pythonEnv;

	public PythonAggregateFunction(
		String name,
		byte[] serializedAggregateFunction,
		DataType[] inputTypes,
		DataType resultType,
		DataType accumulatorType,
		PythonFunctionKind pythonFunctionKind,
		boolean deterministic,
		PythonEnv pythonEnv) {
		this.name = name;
		this.serializedAggregateFunction = serializedAggregateFunction;
		this.inputTypes = inputTypes;
		this.resultType = resultType;
		this.accumulatorType = accumulatorType;
		this.pythonFunctionKind = pythonFunctionKind;
		this.deterministic = deterministic;
		this.pythonEnv = pythonEnv;
	}

	public void accumulate(Object accumulator, Object... args) {
		throw new UnsupportedOperationException(
			"This method is a placeholder and should not be called.");
	}

	@Override
	public Object getValue(Object accumulator) {
		return null;
	}

	@Override
	public Object createAccumulator() {
		return null;
	}

	@Override
	public byte[] getSerializedPythonFunction() {
		return serializedAggregateFunction;
	}

	@Override
	public PythonEnv getPythonEnv() {
		return pythonEnv;
	}

	@Override
	public PythonFunctionKind getPythonFunctionKind() {
		return pythonFunctionKind;
	}

	@Override
	public boolean isDeterministic() {
		return deterministic;
	}

	@Override
	public TypeInformation getResultType() {
		return TypeConversions.fromDataTypeToLegacyInfo(resultType);
	}

	@Override
	public TypeInformation getAccumulatorType() {
		return TypeConversions.fromDataTypeToLegacyInfo(accumulatorType);
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		TypeInference.Builder builder = TypeInference.newBuilder();
		if (inputTypes != null) {
			builder.typedArguments(inputTypes);
		}
		return builder
			.outputTypeStrategy(TypeStrategies.explicit(resultType))
			.accumulatorTypeStrategy(TypeStrategies.explicit(accumulatorType))
			.build();
	}

	@Override
	public String toString() {
		return name;
	}
}
