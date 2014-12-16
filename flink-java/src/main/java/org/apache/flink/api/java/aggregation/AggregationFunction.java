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
import org.apache.flink.api.java.operators.AggregationOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;

/**
 * An aggregation function that aggregates a list of elements of the same
 * type to a single value.
 * 
 * <p>The result of an aggregation function can be of the same or of a
 * different type as the aggregated elements. 
 * 
 * <p>An aggregation function may be composed of other functions that 
 * compute an intermediate result (see also 
 * {@link CompositeAggregationFunction}).  
 *
 * <p>Internally, an aggregation is implemented as a Reduce operation.
 * However, because a field can be aggregated using different aggregation
 * functions, and because an aggregation may be decomposed into multiple
 * intermediates internally, the field on which an aggregation function
 * is computed is not necessarily the input field.
 * 
 * <p>Each aggregation function therefore specifies 3 indices:
 * 
 * <dl>
 * 	 <dt>inputPosition <dd>The field of the input tuple that is aggregated.
 *   <dt>intermediatePosition <dd>The field of the intermediate tuple passed
 *                                to {@code reduce} to compute the aggregate.
 *   <dt>outputPosition <dd>The field of the output tuple holding the result.
 * </dl>
 *
 * <p>Each aggregation function must implement the following methods:
 *
 * <dl>
 *   <dt>{@code initializeIntermediate} 
 *   	<dd>Map the value of the input field to a value which is used to
 *   	compute the aggregate.
 *   <dt>{@code reduce}
 *   	<dd>Recursively aggregate to values to compute the result.
 * </dl>
 * 
 * <p>An aggregation function may overwrite
 * {@code setInputType} to modify its implementation
 * depending on the aggregated type (see also {@link SumAggregationFunction}).
 * 
 * <p>Each aggregation function has a name that is visible in the name
 * of the {@link ReduceOperator} and {@link MapOperator} operations that
 * compute the aggregation. Only the final {@link MapOperator} name contains
 * the names of the aggregation function specified in user code; the
 * intermediate {@link MapOperator} and the {@link ReduceOperator} show the
 * names of the intermediate aggregation functions.
 * 
 * @param <T> The type of the elements that are aggregated (input type).
 * @param <R> The type of the aggregation result (output type).
 *
 * @see AggregationFunction.ResultTypeBehavior
 * @see AggregationOperator
 */
public abstract class AggregationFunction<T, R> implements Serializable {
	private static final long serialVersionUID = 9082279166205627942L;

	private int inputPosition;
	private int outputPosition;
	private int intermediatePosition;
	private String name;

	/**
	 * Create a named AggregationFunction.
	 * @param name The AggregationFunctions's name.
	 */
	public AggregationFunction(String name, int inputPosition) {
		this.name = name;
		this.inputPosition = inputPosition;
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
	 * {@code inputType}. This method can be used to differentiate the
	 * internal implementation of the aggregation function based on the
	 * input type.
	 * 
	 * @param inputType The type of the elements that are aggregated.
	 */
	public void setInputType(BasicTypeInfo<T> inputType) {
		// do nothing
	}
	
	/**
	 * Return the initial value of the intermediate tuple that is used
	 * to compute the aggregation.
	 */
	public abstract R initializeIntermediate(T value);

	/**
	 * Recursively compute the aggregate of two values.
	 */
	public abstract R reduce(R value1, R value2);

	@Override
	public String toString() {
		return getName() + "(" + inputPosition + ")";
	}

	///// Getter / Setter
	
	public String getName() {
		return name;
	}

	public int getOutputPosition() {
		return outputPosition;
	}

	public void setOutputPosition(int outputPosition) {
		this.outputPosition = outputPosition;
	}

	public int getIntermediatePosition() {
		return intermediatePosition;
	}

	public void setIntermediatePosition(int intermediatePosition) {
		this.intermediatePosition = intermediatePosition;
	}

	public int getInputPosition() {
		return inputPosition;
	}

	public void setInputPosition(int inputPosition) {
		this.inputPosition = inputPosition;
	}

}
