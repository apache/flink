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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.planner.plan.utils.AggFunctionFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

import static org.apache.flink.table.types.inference.TypeStrategies.explicit;

/**
 * Base class for fully resolved and strongly typed {@link AggregateFunction}s provided by {@link AggFunctionFactory}.
 *
 * <p>We override {@link #getTypeInference(DataTypeFactory)} in case the internal function is used
 * externally for testing.
 */
@Internal
public abstract class InternalAggregateFunction<T, ACC> extends AggregateFunction<T, ACC> {

	public abstract DataType[] getInputDataTypes();

	public abstract DataType getAccumulatorDataType();

	public abstract DataType getOutputDataType();

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInference.newBuilder()
			.typedArguments(getInputDataTypes())
			.accumulatorTypeStrategy(explicit(getAccumulatorDataType()))
			.outputTypeStrategy(explicit(getOutputDataType()))
			.build();
	}
}
