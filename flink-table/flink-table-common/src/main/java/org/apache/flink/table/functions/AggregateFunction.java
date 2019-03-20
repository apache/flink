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
 * Base class for user-defined aggregates.
 *
 * <p>The behavior of an {@link AggregateFunction} can be defined by implementing a series of custom
 * methods. An {@link AggregateFunction} needs at least three methods:
 *  - <code>createAccumulator</code>,
 *  - <code>accumulate</code>, and
 *  - <code>getValue</code>.
 *
 * <p>There are a few other methods that can be optional to have:
 *  - <code>retract</code>,
 *  - <code>merge</code>, and
 *  - <code>resetAccumulator</code>.
 *
 * <p>All these methods must be declared publicly, not static, and named exactly as the names
 * mentioned above. The methods {@link #createAccumulator()} and {@link #getValue} are defined in
 * the {@link AggregateFunction} functions, while other methods are explained below.
 *
 * <pre>
 * {@code
 * Processes the input values and update the provided accumulator instance. The method
 * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
 * requires at least one accumulate() method.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void accumulate(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Retracts the input values from the accumulator instance. The current design assumes the
 * inputs are the values that have been previously accumulated. The method retract can be
 * overloaded with different custom types and arguments. This function must be implemented for
 * data stream bounded OVER aggregates.
 *
 * param: accumulator           the accumulator which contains the current aggregated results
 * param: [user defined inputs] the input value (usually obtained from a new arrived data).
 *
 * public void retract(ACC accumulator, [user defined inputs])
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Merges a group of accumulator instances into one accumulator instance. This function must be
 * implemented for data stream session window grouping aggregates and data set grouping aggregates.
 *
 * param: accumulator the accumulator which will keep the merged aggregate results. It should
 *                    be noted that the accumulator may contain the previous aggregated
 *                    results. Therefore user should not replace or clean this instance in the
 *                    custom merge method.
 * param: its         an java.lang.Iterable pointed to a group of accumulators that will be
 *                    merged.
 *
 * public void merge(ACC accumulator, java.lang.Iterable<ACC> iterable)
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Resets the accumulator for this AggregateFunction. This function must be implemented for
 * data set grouping aggregates.
 *
 * param: accumulator the accumulator which needs to be reset
 *
 * public void resetAccumulator(ACC accumulator)
 * }
 * </pre>
 *
 * @param <T>   the type of the aggregation result
 * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
 *              aggregated values which are needed to compute an aggregation result.
 *              AggregateFunction represents its state using accumulator, thereby the state of the
 *              AggregateFunction must be put into the accumulator.
 */
@PublicEvolving
public abstract class AggregateFunction<T, ACC> extends UserDefinedFunction {

	/**
	 * Creates and initializes the accumulator for this {@link AggregateFunction}. The accumulator
	 * is used to keep the aggregated values which are needed to compute an aggregation result.
	 *
	 * @return the accumulator with the initial value
	 */
	public abstract ACC createAccumulator();

	/**
	 * Called every time when an aggregation result should be materialized.
	 * The returned value could be either an early and incomplete result
	 * (periodically emitted as data arrive) or the final result of the
	 * aggregation.
	 *
	 * @param accumulator the accumulator which contains the current
	 *                    aggregated results
	 * @return the aggregation result
	 */
	public abstract T getValue(ACC accumulator);

	/**
	 * Returns <code>true</code> if this {@link AggregateFunction} can only be applied in an
	 * OVER window.
	 *
	 * @return <code>true</code> if the {@link AggregateFunction} requires an OVER window,
	 *         <code>false</code> otherwise.
	 */
	public boolean requiresOver() {
		return false;
	}

	/**
	 * Returns the {@link TypeInformation} of the {@link AggregateFunction}'s result.
	 *
	 * @return The {@link TypeInformation} of the {@link AggregateFunction}'s result or
	 *         <code>null</code> if the result type should be automatically inferred.
	 */
	public TypeInformation<T> getResultType() {
		return null;
	}

	/**
	 * Returns the {@link TypeInformation} of the {@link AggregateFunction}'s accumulator.
	 *
	 * @return The {@link TypeInformation} of the {@link AggregateFunction}'s accumulator or
	 *         <code>null</code> if the accumulator type should be automatically inferred.
	 */
	public TypeInformation<ACC> getAccumulatorType() {
		return null;
	}
}
