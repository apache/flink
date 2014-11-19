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

package org.apache.flink.api.java.aggregation;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Specify multiple aggregations on a {@link Tuple}.
 *
 * <p>This builder is created by calling one of the aggregations functions
 * {@code min}, {@code max}, etc. on either a {@link DataSet} or an
 * {@link UnsortedGrouping}. Additional aggregations can then be specified
 * by calling the respective method on this builder. Once all aggregations
 * are specified, call {@code aggregate} to construct the
 * {@link AggregationOperator} that wraps the aggregations.
 * 
 * @param <T> The tuple input type.
 *
 * @author Viktor Rosenfeld <viktor.rosenfeld@tu-berlin.de>
 * 
 * @see Aggregations
 * @see AggregationOperator
 * @see DataSet
 * @see UnsortedGrouping
 */
public class AggregationBuilder<T> {
	
	private AggregationOperatorFactory aggregationOperatorFactory = AggregationOperatorFactory.getInstance();
	private List<AggregationFunction<?, ?>> functions;
	private DataSet<T> input;
	private UnsortedGrouping<T> grouping;
	
	public AggregationBuilder(DataSet<T> input) {
		this();
		this.input = input;
	}
	
	public AggregationBuilder(UnsortedGrouping<T> grouping) {
		this();
		this.grouping = grouping;
	}
	
	private AggregationBuilder() {
		this.functions = new ArrayList<AggregationFunction<?,?>>();
	}
	
	public AggregationBuilder<T> min(int field) {
		functions.add(Aggregations.min(field));
		return this;
	}
	
	public AggregationBuilder<T> max(int field) {
		functions.add(Aggregations.max(field));
		return this;
	}
	
	public AggregationBuilder<T> count() {
		functions.add(Aggregations.count());
		return this;
	}
	
	public AggregationBuilder<T> sum(int field) {
		functions.add(Aggregations.sum(field));
		return this;
	}
	
	public AggregationBuilder<T> average(int field) {
		functions.add(Aggregations.average(field));
		return this;
	}
	
	public AggregationBuilder<T> key(int field) {
		functions.add(Aggregations.key(field));
		return this;
	}
	
	public <R extends Tuple> AggregationOperator<T, R> aggregate() {
		AggregationOperator<T, R> op = null;
		AggregationFunction<?,?>[] functions = new AggregationFunction<?, ?>[this.functions.size()];
		this.functions.toArray(functions);
		if (input != null && grouping == null) {
			op = aggregationOperatorFactory.aggregate(input, functions);
		} else if (grouping != null && input == null) {
			op = aggregationOperatorFactory.aggregate(grouping, functions);
		} else {
			throw new IllegalStateException("Trying to aggregate DataSet and UnsortedGrouping at the same time.");
		}
		return op;
	}

	public AggregationOperatorFactory getAggregationOperatorFactory() {
		return aggregationOperatorFactory;
	}

	public void setAggregationOperatorFactory(AggregationOperatorFactory aggregationOperatorFactory) {
		this.aggregationOperatorFactory = aggregationOperatorFactory;
	}

}
