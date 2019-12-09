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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Base class for user-defined aggregates and table aggregates.
 */
@PublicEvolving
public abstract class UserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {

	/**
	 * Creates and initializes the accumulator for this {@link UserDefinedAggregateFunction}. The
	 * accumulator is used to keep the aggregated values which are needed to compute an aggregation
	 * result.
	 *
	 * @return the accumulator with the initial value
	 */
	public abstract ACC createAccumulator();

	/**
	 * Returns the {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s result.
	 *
	 * @return The {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s result or
	 *         <code>null</code> if the result type should be automatically inferred.
	 */
	public TypeInformation<T> getResultType() {
		return null;
	}

	/**
	 * Returns the {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s accumulator.
	 *
	 * @return The {@link TypeInformation} of the {@link UserDefinedAggregateFunction}'s accumulator
	 *         or <code>null</code> if the accumulator type should be automatically inferred.
	 */
	public TypeInformation<ACC> getAccumulatorType() {
		return null;
	}
}
