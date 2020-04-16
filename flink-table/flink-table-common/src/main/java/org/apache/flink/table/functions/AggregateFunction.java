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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class for user-defined aggregates.
 *
 * <p>The behavior of an {@link AggregateFunction} can be defined by implementing a series of custom
 * methods. An {@link AggregateFunction} needs at least three methods:
 * <ul>
 *     <li>createAccumulator</li>
 *     <li>accumulate</li>
 *     <li>getValue</li>
 * </ul>
 *
 * <p>There are a few other methods that can be optional to have:
 * <ul>
 *     <li>retract</li>
 *     <li>merge</li>
 *     <li>resetAccumulator</li>
 * </ul>
 *
 * <p>All these methods must be declared publicly, not static, and named exactly as the names
 * mentioned above. The method {@link #createAccumulator()} is defined in the
 * {@link UserDefinedAggregateFunction} function, and method {@link #getValue} is defined in
 * the {@link AggregateFunction} while other methods are explained below.
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
 * <p>If this aggregate function can only be applied in an OVER window, this can be declared using the
 * requirement {@link FunctionRequirement#OVER_WINDOW_ONLY} in {@link #getRequirements()}.
 *
 * @param <T>   the type of the aggregation result
 * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
 *              aggregated values which are needed to compute an aggregation result.
 *              AggregateFunction represents its state using accumulator, thereby the state of the
 *              AggregateFunction must be put into the accumulator.
 */
@PublicEvolving
public abstract class AggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {

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
	 *
	 * @deprecated Use {@link #getRequirements()} instead.
	 */
	@Deprecated
	public boolean requiresOver() {
		return false;
	}

	@Override
	public final FunctionKind getKind() {
		return FunctionKind.AGGREGATE;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		throw new TableException("Aggregate functions are not updated to the new type system yet.");
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		final HashSet<FunctionRequirement> requirements = new HashSet<>();
		if (requiresOver()) {
			requirements.add(FunctionRequirement.OVER_WINDOW_ONLY);
		}
		return Collections.unmodifiableSet(requirements);
	}
}
