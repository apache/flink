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
import org.apache.flink.api.java.summarize.BooleanColumnSummary;
import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.apache.flink.api.java.summarize.StringColumnSummary;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

/**
 * This is a generic Aggregator for Value types like StringValue, DoubleValue, etc.
 * This class makes it easy to re-use the implementation of another aggregator.
 *
 * @param <VT> the "Value Type" to aggregate, e.g. DoubleValue, StringValue
 * @param <PT> the "Primitive Type" that "Value Type" can be naturally converted to, e.g. DoubleValue converts to Double
 * @param <R> the result type of the aggregation, e.g. NumericColumnSummary&lt;Double&gt;
 * @param <A> the underlying primitive Aggregator that does the actual work, e.g. DoubleSummaryAggregator
 */
@Internal
public abstract class ValueSummaryAggregator<VT extends Value, PT, R, A extends Aggregator<PT, R>> implements Aggregator<VT, R> {

	private A aggregator = initPrimitiveAggregator();

	@Override
	public void aggregate(VT value) {
		if (value != null) {
			aggregator.aggregate(getValue(value));
		}
		else {
			aggregator.aggregate(null);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void combine(Aggregator<VT, R> otherSameType) {
		ValueSummaryAggregator<VT, PT, R, A> other = (ValueSummaryAggregator<VT, PT, R, A>) otherSameType;
		aggregator.combine(other.aggregator);
	}

	@Override
	public R result() {
		return aggregator.result();
	}

	/**
	 * Initialize an aggregator that can be used for the underlying primitive in the Value type.
	 *
	 * <p>E.g. DoubleValues can easily be converted to Double and could use an underlying Aggregator&lt;Double,?&gt;
	 */
	protected abstract A initPrimitiveAggregator();

	/**
	 * Get the value out of a value type.
	 */
	protected abstract PT getValue(VT value);

	// -----------------------------------------------------------------------------
	// Implementations below
	// -----------------------------------------------------------------------------

	/**
	 * A {@link ValueSummaryAggregator} for {@link Short}.
	 */
	public static class ShortValueSummaryAggregator extends ValueSummaryAggregator<ShortValue, Short, NumericColumnSummary<Short>, ShortSummaryAggregator> {

		@Override
		protected ShortSummaryAggregator initPrimitiveAggregator() {
			return new ShortSummaryAggregator();
		}

		@Override
		protected Short getValue(ShortValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link Integer}.
	 */
	public static class IntegerValueSummaryAggregator extends ValueSummaryAggregator<IntValue, Integer, NumericColumnSummary<Integer>, IntegerSummaryAggregator> {

		@Override
		protected IntegerSummaryAggregator initPrimitiveAggregator() {
			return new IntegerSummaryAggregator();
		}

		@Override
		protected Integer getValue(IntValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link Long}.
	 */
	public static class LongValueSummaryAggregator extends ValueSummaryAggregator<LongValue, Long, NumericColumnSummary<Long>, LongSummaryAggregator> {

		@Override
		protected LongSummaryAggregator initPrimitiveAggregator() {
			return new LongSummaryAggregator();
		}

		@Override
		protected Long getValue(LongValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link Float}.
	 */
	public static class FloatValueSummaryAggregator extends ValueSummaryAggregator<FloatValue, Float, NumericColumnSummary<Float>, FloatSummaryAggregator> {

		@Override
		protected FloatSummaryAggregator initPrimitiveAggregator() {
			return new FloatSummaryAggregator();
		}

		@Override
		protected Float getValue(FloatValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link Double}.
	 */
	public static class DoubleValueSummaryAggregator extends ValueSummaryAggregator<DoubleValue, Double, NumericColumnSummary<Double>, DoubleSummaryAggregator> {

		@Override
		protected DoubleSummaryAggregator initPrimitiveAggregator() {
			return new DoubleSummaryAggregator();
		}

		@Override
		protected Double getValue(DoubleValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link Boolean}.
	 */
	public static class BooleanValueSummaryAggregator extends ValueSummaryAggregator<BooleanValue, Boolean, BooleanColumnSummary, BooleanSummaryAggregator> {

		@Override
		protected BooleanSummaryAggregator initPrimitiveAggregator() {
			return new BooleanSummaryAggregator();
		}

		@Override
		protected Boolean getValue(BooleanValue value) {
			return value.getValue();
		}
	}

	/**
	 * A {@link ValueSummaryAggregator} for {@link String}.
	 */
	public static class StringValueSummaryAggregator extends ValueSummaryAggregator<StringValue, String, StringColumnSummary, StringSummaryAggregator> {

		@Override
		protected StringSummaryAggregator initPrimitiveAggregator() {
			return new StringSummaryAggregator();
		}

		@Override
		protected String getValue(StringValue value) {
			return value.getValue();
		}
	}
}
