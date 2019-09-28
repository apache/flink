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

package org.apache.flink.runtime.rest.handler.job.metrics;

/**
 * An interface for accumulating double values.
 */
interface DoubleAccumulator {

	/**
	 * Adds the given value to this accumulator.
	 *
	 * @param value value to add
	 */
	void add(double value);

	/**
	 * Returns the current value of this accumulator.
	 *
	 * @return current value of this accumulator
	 */
	double getValue();

	/**
	 * Returns the name of this accumulator type. This name is used as a suffix for exposed metrics.
	 *
	 * @return name of this accumulator type
	 */
	String getName();

	/**
	 * A factory for {@link DoubleAccumulator}s. This allows us to regenerate a new set of accumulators for each metrics
	 * without re-evaluating the "agg" query parameter or re-using existing accumulators.
	 *
	 * @param <A> DoubleAccumulator subclass
	 */
	interface DoubleAccumulatorFactory<A extends DoubleAccumulator> {
		/**
		 * Creates a new accumulator with the given initial value.
		 *
		 * @param init initial value
		 * @return new accumulator with the given initial value
		 */
		A get(double init);
	}

	/**
	 * Factory for {@link DoubleMaximum}.
	 */
	final class DoubleMaximumFactory implements DoubleAccumulatorFactory<DoubleMaximum> {
		private static final DoubleMaximumFactory INSTANCE = new DoubleMaximumFactory();

		private DoubleMaximumFactory(){
		}

		@Override
		public DoubleMaximum get(double init) {
			return new DoubleMaximum(init);
		}

		public static DoubleMaximumFactory get() {
			return INSTANCE;
		}
	}

	/**
	 * Factory for {@link DoubleMinimum}.
	 */
	final class DoubleMinimumFactory implements DoubleAccumulatorFactory<DoubleMinimum> {
		private static final DoubleMinimumFactory INSTANCE = new DoubleMinimumFactory();

		private DoubleMinimumFactory(){
		}

		@Override
		public DoubleMinimum get(double init) {
			return new DoubleMinimum(init);
		}

		public static DoubleMinimumFactory get() {
			return INSTANCE;
		}
	}

	/**
	 * Factory for {@link DoubleSum}.
	 */
	final class DoubleSumFactory implements DoubleAccumulatorFactory<DoubleSum> {
		private static final DoubleSumFactory INSTANCE = new DoubleSumFactory();

		private DoubleSumFactory(){
		}

		@Override
		public DoubleSum get(double init) {
			return new DoubleSum(init);
		}

		public static DoubleSumFactory get() {
			return INSTANCE;
		}
	}

	/**
	 * Factory for {@link DoubleAverage}.
	 */
	final class DoubleAverageFactory implements DoubleAccumulatorFactory<DoubleAverage> {
		private static final DoubleAverageFactory INSTANCE = new DoubleAverageFactory();

		private DoubleAverageFactory(){
		}

		@Override
		public DoubleAverage get(double init) {
			return new DoubleAverage(init);
		}

		public static DoubleAverageFactory get() {
			return INSTANCE;
		}
	}

	/**
	 * {@link DoubleAccumulator} that returns the maximum value.
	 */
	final class DoubleMaximum implements DoubleAccumulator {

		public static final String NAME = "max";

		private double value;

		private DoubleMaximum(double init) {
			value = init;
		}

		@Override
		public void add(double value) {
			this.value = Math.max(this.value, value);
		}

		@Override
		public double getValue() {
			return value;
		}

		@Override
		public String getName() {
			return NAME;
		}
	}

	/**
	 * {@link DoubleAccumulator} that returns the minimum value.
	 */
	final class DoubleMinimum implements DoubleAccumulator {

		public static final String NAME = "min";

		private double value;

		private DoubleMinimum(double init) {
			value = init;
		}

		@Override
		public void add(double value) {
			this.value = Math.min(this.value, value);
		}

		@Override
		public double getValue() {
			return value;
		}

		@Override
		public String getName() {
			return NAME;
		}
	}

	/**
	 * {@link DoubleAccumulator} that returns the sum of all values.
	 */
	final class DoubleSum implements DoubleAccumulator {

		public static final String NAME = "sum";

		private double value;

		private DoubleSum(double init) {
			value = init;
		}

		@Override
		public void add(double value) {
			this.value += value;
		}

		@Override
		public double getValue() {
			return value;
		}

		@Override
		public String getName() {
			return NAME;
		}
	}

	/**
	 * {@link DoubleAccumulator} that returns the average over all values.
	 */
	final class DoubleAverage implements DoubleAccumulator {

		public static final String NAME = "avg";

		private double sum;
		private int count;

		private DoubleAverage(double init) {
			sum = init;
			count = 1;
		}

		@Override
		public void add(double value) {
			this.sum += value;
			this.count++;
		}

		@Override
		public double getValue() {
			return sum / count;
		}

		@Override
		public String getName() {
			return NAME;
		}
	}
}
