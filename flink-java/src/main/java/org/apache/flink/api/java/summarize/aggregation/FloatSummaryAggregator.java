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
 * Aggregator that can handle Float types.
 */
@Internal
public class FloatSummaryAggregator extends NumericSummaryAggregator<Float> {

	private static final long serialVersionUID = 1L;

	// Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

	/**
	 * Aggregator for min operation.
	 */
	public static class MinFloatAggregator implements Aggregator<Float, Float> {

		private float min = Float.POSITIVE_INFINITY;

		@Override
		public void aggregate(Float value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Float, Float> other) {
			min = Math.min(min, ((MinFloatAggregator) other).min);
		}

		@Override
		public Float result() {
			return min;
		}
	}

	/**
	 * Aggregator for max operation.
	 */
	public static class MaxFloatAggregator implements Aggregator<Float, Float> {

		private float max = Float.NEGATIVE_INFINITY;

		@Override
		public void aggregate(Float value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Float, Float> other) {
			max = Math.max(max, ((MaxFloatAggregator) other).max);
		}

		@Override
		public Float result() {
			return max;
		}
	}

	/**
	 * Aggregator for sum operation.
	 */
	public static class SumFloatAggregator implements Aggregator<Float, Float> {

		private CompensatedSum sum = ZERO;

		@Override
		public void aggregate(Float value) {
			sum = sum.add(value);
		}

		@Override
		public void combine(Aggregator<Float, Float> other) {
			sum = sum.add(((SumFloatAggregator) other).sum);
		}

		@Override
		public Float result() {
			// overflow will go to infinity
			return (float) sum.value();
		}
	}

	@Override
	protected Aggregator<Float, Float> initMin() {
		return new MinFloatAggregator();
	}

	@Override
	protected Aggregator<Float, Float> initMax() {
		return new MaxFloatAggregator();
	}

	@Override
	protected Aggregator<Float, Float> initSum() {
		return new SumFloatAggregator();
	}

	@Override
	protected boolean isNan(Float number) {
		return number.isNaN();
	}

	@Override
	protected boolean isInfinite(Float number) {
		return number.isInfinite();
	}
}
