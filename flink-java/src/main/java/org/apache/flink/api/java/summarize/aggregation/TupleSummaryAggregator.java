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

/**
 * Aggregate tuples using an array of aggregators, one for each "column" or position within the Tuple.
 */
@Internal
public class TupleSummaryAggregator<R extends Tuple> implements Aggregator<Tuple, R> {

	private static final long serialVersionUID = 1L;

	private final Aggregator[] columnAggregators;

	public TupleSummaryAggregator(Aggregator[] columnAggregators) {
		this.columnAggregators = columnAggregators;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void aggregate(Tuple value) {
		for (int i = 0; i < columnAggregators.length; i++) {
			columnAggregators[i].aggregate(value.getField(i));
		}

	}

	@Override
	@SuppressWarnings("unchecked")
	public void combine(Aggregator<Tuple, R> other) {
		TupleSummaryAggregator tupleSummaryAggregator = (TupleSummaryAggregator) other;
		for (int i = 0; i < columnAggregators.length; i++) {
			columnAggregators[i].combine(tupleSummaryAggregator.columnAggregators[i]);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public R result() {
		try {
			Class tupleClass = Tuple.getTupleClass(columnAggregators.length);
			R tuple = (R) tupleClass.newInstance();
			for (int i = 0; i < columnAggregators.length; i++) {
				tuple.setField(columnAggregators[i].result(), i);
			}
			return tuple;
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException("Unexpected error instantiating Tuple class for aggregation results", e);

		}
	}
}
