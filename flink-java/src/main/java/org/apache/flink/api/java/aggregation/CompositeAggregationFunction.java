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

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * A composite aggregation function delegates the computation of intermediate
 * results to other aggregation functions.
 * 
 * <p>Each composite must implement 2 methods:
 * 
 * <dl>
 *   <dt>getIntermediates
 *   	<dd>Retrieve a list of functions that compute intermediate results.
 *   <dt>computeComposite
 *   	<dd>Compute the result of the composite function.
 * </dl>
 * 
 * <p>Because the composite function itself is not used in the computation
 * of intermediate results, the methods {@code initializeIntermediate} and
 * {@code reduce} should never be called. 
 * 
 * @param <T> The input type.
 * @param <R> The output type.
 */
public abstract class CompositeAggregationFunction<T, R> extends AggregationFunction<T, R> {
	private static final long serialVersionUID = 517617558180264806L;

	public CompositeAggregationFunction(String name, int fieldPosition) {
		super(name, fieldPosition);
	}
	
	/**
	 * Return aggregation functions that compute intermediate results.
	 */
	public abstract List<AggregationFunction<?, ?>> getIntermediates(List<AggregationFunction<?, ?>> existingIntermediates);
	
	/**
	 * Return the result of the composite aggregation function.
	 */
	public abstract R computeComposite(Tuple tuple);
	
	@Override
	public R initializeIntermediate(T value) {
		throw new IllegalStateException("A composite aggregation function should not be initialized; initialize the intermediates instead.");
	}
	
	@Override
	public R reduce(R value1, R value2) {
		throw new IllegalStateException("A composite aggregation function should not be reduced; reduce the intermediates instead.");
	}
	
}
