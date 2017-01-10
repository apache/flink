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

import java.io.Serializable;

/**
 * 
 * <p>Aggregation functions must be {@link Serializable} because they are sent around
 * between distributed processes during distributed execution.
 * 
 * <p>An example how to use this interface is below:
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
 *     public void add(Integer value, AverageAccumulator acc) {
 *         acc.sum += value;
 *         acc.count++;
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
 *     public void add(Datum value, AverageAccumulator acc) {
 *         acc.count += value.getWeight();
 *         acc.sum += value.getValue();
 *     }
 *
 *     public Double getResult(AverageAccumulator acc) {
 *         return acc.sum / (double) acc.count;
 *     }
 * }
 * }</pre>
 */
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {

	ACC createAccumulator();

	void add(IN value, ACC accumulator);

	OUT getResult(ACC accumulator);

	ACC merge(ACC a, ACC b);
}
