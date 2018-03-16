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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

/**
 * Factory for creating Summary Aggregators.
 */
@Internal
public class SummaryAggregatorFactory {

	@SuppressWarnings("unchecked")
	public static <R extends Tuple> TupleSummaryAggregator<R> create(TupleTypeInfoBase<?> inType) {
		Aggregator[] columnAggregators = new Aggregator[inType.getArity()];
		for (int field = 0; field < inType.getArity(); field++) {
			Class clazz = inType.getTypeAt(field).getTypeClass();
			columnAggregators[field] = SummaryAggregatorFactory.create(clazz);
		}
		return new TupleSummaryAggregator<>(columnAggregators);
	}

	/**
	 * Create a SummaryAggregator for the supplied type.
	 * @param <T> the type to aggregate
	 * @param <R> the result type of the aggregation
	 */
	@SuppressWarnings("unchecked")
	public static <T, R> Aggregator<T, R> create(Class<T> type) {
		if (type == Long.class) {
			return (Aggregator<T, R>) new LongSummaryAggregator();
		}
		else if (type == LongValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.LongValueSummaryAggregator();
		}
		else if (type == Integer.class) {
			return (Aggregator<T, R>) new IntegerSummaryAggregator();
		}
		else if (type == IntValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.IntegerValueSummaryAggregator();
		}
		else if (type == Double.class) {
			return (Aggregator<T, R>) new DoubleSummaryAggregator();
		}
		else if (type == DoubleValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.DoubleValueSummaryAggregator();
		}
		else if (type == Float.class) {
			return (Aggregator<T, R>) new FloatSummaryAggregator();
		}
		else if (type == FloatValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.FloatValueSummaryAggregator();
		}
		else if (type == Short.class) {
			return (Aggregator<T, R>) new ShortSummaryAggregator();
		}
		else if (type == ShortValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.ShortValueSummaryAggregator();
		}
		else if (type == Boolean.class) {
			return (Aggregator<T, R>) new BooleanSummaryAggregator();
		}
		else if (type == BooleanValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.BooleanValueSummaryAggregator();
		}
		else if (type == String.class) {
			return (Aggregator<T, R>) new StringSummaryAggregator();
		}
		else if (type == StringValue.class) {
			return (Aggregator<T, R>) new ValueSummaryAggregator.StringValueSummaryAggregator();
		}
		else {
			// rather than error for unsupported types do something very generic
			return (Aggregator<T, R>) new ObjectSummaryAggregator();
		}
	}

}
