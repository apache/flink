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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FirstReducer;
import org.apache.flink.api.java.functions.SelectByMaxFunction;
import org.apache.flink.api.java.functions.SelectByMinFunction;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class UnsortedGrouping<T> extends Grouping<T> {

	public UnsortedGrouping(DataSet<T> set, Keys<T> keys) {
		super(set, keys);
	}

	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Applies an Aggregate transformation on a grouped {@link org.apache.flink.api.java.tuple.Tuple} {@link DataSet}.<br/>
	 * <b>Note: Only Tuple DataSets can be aggregated.</b>
	 * The transformation applies a built-in {@link Aggregations Aggregation} on a specified field 
	 *   of a Tuple group. Additional aggregation functions can be added to the resulting 
	 *   {@link AggregateOperator} by calling {@link AggregateOperator#and(Aggregations, int)}.
	 * 
	 * @param agg The built-in aggregation function that is computed.
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the aggregated DataSet. 
	 * 
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Aggregations
	 * @see AggregateOperator
	 * @see DataSet
	 */
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}

	/**
	 * Syntactic sugar for aggregate (SUM, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the summed DataSet.
	 *
	 * @see org.apache.flink.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> sum (int field) {
		return this.aggregate (Aggregations.SUM, field);
	}

	/**
	 * Syntactic sugar for aggregate (MAX, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the max'ed DataSet.
	 *
	 * @see org.apache.flink.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> max (int field) {
		return this.aggregate (Aggregations.MAX, field);
	}

	/**
	 * Syntactic sugar for aggregate (MIN, field)
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the min'ed DataSet.
	 *
	 * @see org.apache.flink.api.java.operators.AggregateOperator
	 */
	public AggregateOperator<T> min (int field) {
		return this.aggregate (Aggregations.MIN, field);
	}
	
	/**
	 * Applies a Reduce transformation on a grouped {@link DataSet}.<br/>
	 * For each group, the transformation consecutively calls a {@link org.apache.flink.api.common.functions.RichReduceFunction}
	 *   until only a single element for each group remains. 
	 * A ReduceFunction combines two elements into one new element of the same type.
	 * 
	 * @param reducer The ReduceFunction that is applied on each group of the DataSet.
	 * @return A ReduceOperator that represents the reduced DataSet.
	 * 
	 * @see org.apache.flink.api.common.functions.RichReduceFunction
	 * @see ReduceOperator
	 * @see DataSet
	 */
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		if (reducer == null) {
			throw new NullPointerException("Reduce function must not be null.");
		}
		return new ReduceOperator<T>(this, reducer);
	}
	
	/**
	 * Applies a GroupReduce transformation on a grouped {@link DataSet}.<br/>
	 * The transformation calls a {@link org.apache.flink.api.common.functions.RichGroupReduceFunction} for each group of the DataSet.
	 * A GroupReduceFunction can iterate over all elements of a group and emit any
	 *   number of output elements including none.
	 * 
	 * @param reducer The GroupReduceFunction that is applied on each group of the DataSet.
	 * @return A GroupReduceOperator that represents the reduced DataSet.
	 * 
	 * @see org.apache.flink.api.common.functions.RichGroupReduceFunction
	 * @see GroupReduceOperator
	 * @see DataSet
	 */
	public <R> GroupReduceOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		if (reducer == null) {
			throw new NullPointerException("GroupReduce function must not be null.");
		}
		TypeInformation<R> resultType = TypeExtractor.getGroupReduceReturnTypes(reducer, this.getDataSet().getType());

		return new GroupReduceOperator<T, R>(this, resultType, reducer);
	}
	
	/**
	 * Returns a new set containing the first n elements in this grouped {@link DataSet}.<br/>
	 * @param n The desired number of elements for each group.
	 * @return A ReduceGroupOperator that represents the DataSet containing the elements.
	*/
	public GroupReduceOperator<T, T> first(int n) {
		if(n < 1) {
			throw new InvalidProgramException("Parameter n of first(n) must be at least 1.");
		}
		
		return reduceGroup(new FirstReducer<T>(n));
	}

	/**
	 * Applies a special case of a reduce transformation (minBy) on a grouped {@link DataSet}.<br/>
	 * The transformation consecutively calls a {@link ReduceFunction} 
	 * until only a single element remains which is the result of the transformation.
	 * A ReduceFunction combines two elements into one new element of the same type.
	 *  
	 * @param fields Keys taken into account for finding the minimum.
	 * @return A {@link ReduceOperator} representing the minimum.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ReduceOperator<T> minBy(int... fields)  {
		
		// Check for using a tuple
		if(!this.dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Method minBy(int) only works on tuples.");
		}
			
		return new ReduceOperator<T>(this, new SelectByMinFunction(
				(TupleTypeInfo) this.dataSet.getType(), fields));
	}
	
	/**
	 * Applies a special case of a reduce transformation (maxBy) on a grouped {@link DataSet}.<br/>
	 * The transformation consecutively calls a {@link ReduceFunction} 
	 * until only a single element remains which is the result of the transformation.
	 * A ReduceFunction combines two elements into one new element of the same type.
	 *  
	 * @param fields Keys taken into account for finding the minimum.
	 * @return A {@link ReduceOperator} representing the minimum.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ReduceOperator<T> maxBy(int... fields)  {
		
		// Check for using a tuple
		if(!this.dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Method maxBy(int) only works on tuples.");
		}
			
		return new ReduceOperator<T>(this, new SelectByMaxFunction(
				(TupleTypeInfo) this.dataSet.getType(), fields));
	}
	// --------------------------------------------------------------------------------------------
	//  Group Operations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} elements within a group on the specified field in the specified {@link Order}.</br>
	 * <b>Note: Only groups of Tuple elements can be sorted.</b><br/>
	 * Groups can be sorted by multiple fields by chaining {@link #sortGroup(int, Order)} calls.
	 * 
	 * @param field The Tuple field on which the group is sorted.
	 * @param order The Order in which the specified Tuple field is sorted.
	 * @return A SortedGrouping with specified order of group element.
	 * 
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Order
	 */
	public SortedGrouping<T> sortGroup(int field, Order order) {
		return new SortedGrouping<T>(this.dataSet, this.keys, field, order);
	}
	
}
