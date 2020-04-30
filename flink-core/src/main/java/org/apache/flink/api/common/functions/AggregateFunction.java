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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * The {@code AggregateFunction} is a flexible aggregation function, characterized by the
 * following features:
 *
 * <ul>
 *     <li>The aggregates may use different types for input values, intermediate aggregates,
 *         and result type, to support a wide range of aggregation types.</li>
 *
 *     <li>Support for distributive aggregations: Different intermediate aggregates can be
 *         merged together, to allow for pre-aggregation/final-aggregation optimizations.</li>
 * </ul>
 *
 * <p>The {@code AggregateFunction}'s intermediate aggregate (in-progress aggregation state)
 * is called the <i>accumulator</i>. Values are added to the accumulator, and final aggregates are
 * obtained by finalizing the accumulator state. This supports aggregation functions where the
 * intermediate state needs to be different than the aggregated values and the final result type,
 * such as for example <i>average</i> (which typically keeps a count and sum).
 * Merging intermediate aggregates (partial aggregates) means merging the accumulators.
 *
 * <p>The AggregationFunction itself is stateless. To allow a single AggregationFunction
 * instance to maintain multiple aggregates (such as one aggregate per key), the
 * AggregationFunction creates a new accumulator whenever a new aggregation is started.
 *
 * <p>Aggregation functions must be {@link Serializable} because they are sent around
 * between distributed processes during distributed execution.
 *
 * <h1>Example: Average and Weighted Average</h1>
 *
 * <pre>{@code
 * // the accumulator, which holds the state of the in-flight aggregate
 * public class AverageAccumulator {
 *     long count;
 *     long sum;
 * }
 *
 * // implementation of an aggregation function for an 'average'
 * public class Average implements AggregateFunction<Integer, AverageAccumulator, Double> {
 *
 *     public AverageAccumulator createAccumulator() {
 *         return new AverageAccumulator();
 *     }
 *
 *     public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
 *         a.count += b.count;
 *         a.sum += b.sum;
 *         return a;
 *     }
 *
 *     public AverageAccumulator add(Integer value, AverageAccumulator acc) {
 *         acc.sum += value;
 *         acc.count++;
 *         return acc;
 *     }
 *
 *     public Double getResult(AverageAccumulator acc) {
 *         return acc.sum / (double) acc.count;
 *     }
 * }
 *
 * // implementation of a weighted average
 * // this reuses the same accumulator type as the aggregate function for 'average'
 * public class WeightedAverage implements AggregateFunction<Datum, AverageAccumulator, Double> {
 *
 *     public AverageAccumulator createAccumulator() {
 *         return new AverageAccumulator();
 *     }
 *
 *     public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
 *         a.count += b.count;
 *         a.sum += b.sum;
 *         return a;
 *     }
 *
 *     public AverageAccumulator add(Datum value, AverageAccumulator acc) {
 *         acc.count += value.getWeight();
 *         acc.sum += value.getValue();
 *         return acc;
 *     }
 *
 *     public Double getResult(AverageAccumulator acc) {
 *         return acc.sum / (double) acc.count;
 *     }
 * }
 * }</pre>
 *
 * @param <IN>  The type of the values that are aggregated (input values)
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> The type of the aggregated result
 */
@PublicEvolving
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {

	/**
	 * Creates a new accumulator, starting a new aggregate.
	 *
	 * <p>The new accumulator is typically meaningless unless a value is added
	 * via {@link #add(Object, Object)}.
	 *
	 * <p>The accumulator is the state of a running aggregation. When a program has multiple
	 * aggregates in progress (such as per key and window), the state (per key and window)
	 * is the size of the accumulator.
	 *
	 * @return A new accumulator, corresponding to an empty aggregate.
	 */
	ACC createAccumulator();

	/**
	 * Adds the given input value to the given accumulator, returning the
	 * new accumulator value.
	 *
	 * <p>For efficiency, the input accumulator may be modified and returned.
	 *
	 * @param value The value to add
	 * @param accumulator The accumulator to add the value to
	 *
	 * @return The accumulator with the updated state
	 */
	ACC add(IN value, ACC accumulator);

	/**
	 * Gets the result of the aggregation from the accumulator.
	 *
	 * @param accumulator The accumulator of the aggregation
	 * @return The final aggregation result.
	 */
	OUT getResult(ACC accumulator);

	/**
	 * Merges two accumulators, returning an accumulator with the merged state.
	 *
	 * <p>This function may reuse any of the given accumulators as the target for the merge
	 * and return that. The assumption is that the given accumulators will not be used any
	 * more after having been passed to this function.
	 *
	 * @param a An accumulator to merge
	 * @param b Another accumulator to merge
	 *
	 * @return The accumulator with the merged state
	 */
	ACC merge(ACC a, ACC b);
}
