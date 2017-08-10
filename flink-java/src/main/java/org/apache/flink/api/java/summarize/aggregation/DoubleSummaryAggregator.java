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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.api.java.summarize.aggregation.CompensatedSum.ZERO;

/**
 * Aggregator that can handle Double types.
 */
@Internal
public class DoubleSummaryAggregator extends NumericSummaryAggregator<Double> {

	// Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

	/**
	 * Aggregator for min operation.
	 */
	public static class MinDoubleAggregator implements Aggregator<Double, Double> {

		private double min = Double.POSITIVE_INFINITY;

		@Override
		public void aggregate(Double value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			min = Math.min(min, ((MinDoubleAggregator) other).min);
		}

		@Override
		public Double result() {
			return min;
		}
	}

	/**
	 * Aggregator for max operation.
	 */
	public static class MaxDoubleAggregator implements Aggregator<Double, Double> {

		private double max = Double.NEGATIVE_INFINITY;

		@Override
		public void aggregate(Double value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			max = Math.max(max, ((MaxDoubleAggregator) other).max);
		}

		@Override
		public Double result() {
			return max;
		}
	}

	/**
	 * Aggregator for sum operation.
	 */
	public static class SumDoubleAggregator implements Aggregator<Double, Double> {

		private CompensatedSum sum = ZERO;

		@Override
		public void aggregate(Double value) {
			sum = sum.add(value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			sum = sum.add(((SumDoubleAggregator) other).sum);
		}

		@Override
		public Double result() {
			return sum.value();
		}
	}

	@Override
	protected Aggregator<Double, Double> initMin() {
		return new MinDoubleAggregator();
	}

	@Override
	protected Aggregator<Double, Double> initMax() {
		return new MaxDoubleAggregator();
	}

	@Override
	protected Aggregator<Double, Double> initSum() {
		return new SumDoubleAggregator();
	}

	@Override
	protected boolean isNan(Double number) {
		return number.isNaN();
	}

	@Override
	protected boolean isInfinite(Double number) {
		return number.isInfinite();
	}

}
