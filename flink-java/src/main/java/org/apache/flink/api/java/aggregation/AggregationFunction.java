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

import java.io.Serializable;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.operators.GroupReduceOperator;

/**
 * An aggregation function that aggregates a list of elements of the same
 * type to a single value.
 * 
 * <p>The result of an aggregation function can be of the same or of a
 * different type as the aggregated elements. 
 * 
 * <p>Each aggregation function must implement the following three API
 * functions:
 * <ul>
 *   <li>{@link AggregationFunction.initialize}
 *   <li>{@link AggregationFunction.aggregate}
 *   <li>{@link AggregationFunctino.getAggregate}
 * </ul>
 * 
 * <b>Note: An aggregation function may be reused. It is therefore
 * necessary that state initialization happens in 
 * {@link AggregationFunction.initialize} and not in the constructor.</b> 
 * 
 * <p>Each aggregation function specifies the index of the tuple field
 * that it aggregates. 
 * 
 * <p>Each aggregation function has a name that is visible in the name
 * of the {@link GroupReduceOperator} operation that computes the aggregation.
 * 
 * @param <T> The type of the elements that are aggregated (input type).
 * @param <R> The type of the aggregation result (output type).
 *
 * @see AggregationFunction.ResultTypeBehavior
 */
public abstract class AggregationFunction<T, R> implements Serializable {
	private static final long serialVersionUID = 9082279166205627942L;

	private String name;

	/**
	 * Create a named AggregationFunction.
	 * @param name The AggregationFunctions's name.
	 */
	public AggregationFunction(String name) {
		this.name = name;
	}

	/**
	 * Specifies whether an AggregationFunction returns the same type
	 * as its input element or another, fixed type.
	 *
	 * <ul>
	 *   <li>{@code FIXED}: The result type is fixed,
	 *   e.g., {@code count()} returns {@code Long}.
	 *   <li>{@code INPUT}: The result type is the same
	 *   as the input type, e.g., {@code min()}.
	 * </ul>
	 */
	public static enum ResultTypeBehavior {
		INPUT,
		FIXED
	}

	/**
	 * Specifies whether an AggregationFunction returns the same type
	 * as its input element or another, fixed type.
	 * 
	 * @return {@link AggregationFunction.FIXED} if the result type is
	 * fixed and {@link AggregationFunction.INPUT} if the result type is
	 * the same as the output type.
	 */
	public abstract ResultTypeBehavior getResultTypeBehavior();
	
	/**
	 * Return the AggregationFunction's result type.
	 */
	public abstract BasicTypeInfo<R> getResultType();

	/**
	 * Specify the AggregationFunction's input type.
	 * 
	 * <p>This method is called once for each aggregation function when
	 * the result type of the of the transformed DataSet returned by
	 * {@link DataSet.aggregate} or {@link UnsortedGrouping.aggregate}
	 * is computed. The type of the aggregated field is passed in
	 * {@code inputType}. This method can be used to differ the internal
	 * implementation of the aggregation function based on the input type.
	 * 
	 * @param inputType The type of the elements that are aggregated.
	 */
	public abstract void setInputType(BasicTypeInfo<T> inputType);
	
	/**
	 * Return the index of the aggregated tuple field.
	 */
	public abstract int getFieldPosition();
	
	/**
	 * Initialize the aggregation function.
	 * <b>Note: An aggregation function may be reused. It is therefore
	 * necessary that state initialization happens in 
	 * {@link AggregationFunction.initialize} and not in the constructor.</b> 
	 */
	public abstract void initialize();
	
	/**
	 * Aggregate a single {@code value}.
	 */
	public abstract void aggregate(T value);
	
	/**
	 * Return the aggregation result.
	 */
	public abstract R getAggregate();

	@Override
	public String toString() {
		return name + "()";
	}

	protected String getName() {
		return name;
	}

}
