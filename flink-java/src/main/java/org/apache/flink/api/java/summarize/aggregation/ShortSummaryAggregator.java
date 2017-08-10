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

/**
 * Aggregator that can handle Short types.
 */
@Internal
public class ShortSummaryAggregator extends NumericSummaryAggregator<Short> {

	private static final long serialVersionUID = 1L;

	/**
	 * Like Math.min() except for shorts.
	 */
	public static Short min(Short a, Short b) {
		return a <= b ? a : b;
	}

	/**
	 * Like Math.max() except for shorts.
	 */
	public static Short max(Short a, Short b) {
		return a >= b ? a : b;
	}

	// Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

	/**
	 * Aggregator for min operation.
	 */
	public static class MinShortAggregator implements Aggregator<Short, Short> {

		private short min = Short.MAX_VALUE;

		@Override
		public void aggregate(Short value) {
			min = min(min, value);
		}

		@Override
		public void combine(Aggregator<Short, Short> other) {
			min = min(min, ((MinShortAggregator) other).min);
		}

		@Override
		public Short result() {
			return min;
		}
	}

	/**
	 * Aggregator for max operation.
	 */
	public static class MaxShortAggregator implements Aggregator<Short, Short> {

		private short max = Short.MIN_VALUE;

		@Override
		public void aggregate(Short value) {
			max = max(max, value);
		}

		@Override
		public void combine(Aggregator<Short, Short> other) {
			max = max(max, ((MaxShortAggregator) other).max);
		}

		@Override
		public Short result() {
			return max;
		}
	}

	/**
	 * Aggregator for sum operation.
	 */
	public static class SumShortAggregator implements Aggregator<Short, Short> {

		private short sum = 0;

		@Override
		public void aggregate(Short value) {
			sum += value;
		}

		@Override
		public void combine(Aggregator<Short, Short> other) {
			sum += ((SumShortAggregator) other).sum;
		}

		@Override
		public Short result() {
			return sum;
		}
	}

	@Override
	protected Aggregator<Short, Short> initMin() {
		return new MinShortAggregator();
	}

	@Override
	protected Aggregator<Short, Short> initMax() {
		return new MaxShortAggregator();
	}

	@Override
	protected Aggregator<Short, Short> initSum() {
		return new SumShortAggregator();
	}

	@Override
	protected boolean isNan(Short number) {
		// NaN never applies here because only types like Float and Double have NaN
		return false;
	}

	@Override
	protected boolean isInfinite(Short number) {
		// Infinity never applies here because only types like Float and Double have Infinity
		return false;
	}
}
