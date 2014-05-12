/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;

/**
 * Grouping is an intermediate step for a transformation on a grouped DataSet.<br/>
 * The following transformation can be applied on Grouping:
 * <ul>
 * 	<li>{@link Grouping#reduce(ReduceFunction)},</li>
 * <li>{@link Grouping#reduceGroup(GroupReduceFunction)}, and</li>
 * <li>{@link Grouping#aggregate(Aggregations, int)}.</li>
 * </ul>
 *
 * @param <T> The type of the elements of the grouped DataSet.
 * 
 * @see DataSet
 */
public class Grouping<T> {
	
	protected final DataSet<T> dataSet;
	
	protected final Keys<T> keys;

	public Grouping(DataSet<T> set, Keys<T> keys) {
		if (set == null || keys == null) {
			throw new NullPointerException();
		}
		
		if (keys.isEmpty()) {
			throw new InvalidProgramException("The grouping keys must not be empty.");
		}

		this.dataSet = set;
		this.keys = keys;
	}
	
	
	protected DataSet<T> getDataSet() {
		return this.dataSet;
	}
	
	protected Keys<T> getKeys() {
		return this.keys;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Applies an Aggregate transformation on a grouped {@link Tuple} {@link DataSet}.<br/>
	 * <b>Note: Only Tuple DataSets can be aggregated.</b>
	 * The transformation applies a built-in {@link Aggregations Aggregation} on a specified field 
	 *   of a Tuple group. Additional aggregation functions can be added to the resulting 
	 *   {@link AggregateOperator} by calling {@link AggregateOperator#and(Aggregations, int)}.
	 * 
	 * @param agg The built-in aggregation function that is computed.
	 * @param field The index of the Tuple field on which the aggregation function is applied.
	 * @return An AggregateOperator that represents the aggregated DataSet. 
	 * 
	 * @see Tuple
	 * @see Aggregations
	 * @see AggregateOperator
	 * @see DataSet
	 */
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
	/**
	 * Applies a Reduce transformation on a grouped {@link DataSet}.<br/>
	 * For each group, the transformation consecutively calls a {@link ReduceFunction} 
	 *   until only a single element for each group remains. 
	 * A ReduceFunction combines two elements into one new element of the same type.
	 * 
	 * @param reducer The ReduceFunction that is applied on each group of the DataSet.
	 * @return A ReduceOperator that represents the reduced DataSet.
	 * 
	 * @see ReduceFunction
	 * @see ReduceOperator
	 * @see DataSet
	 */
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	/**
	 * Applies a GroupReduce transformation on a grouped {@link DataSet}.<br/>
	 * The transformation calls a {@link GroupReduceFunction} for each group of the DataSet.
	 * A GroupReduceFunction can iterate over all elements of a group and emit any
	 *   number of output elements including none.
	 * 
	 * @param reducer The GroupReduceFunction that is applied on each group of the DataSet.
	 * @return A GroupReduceOperator that represents the reduced DataSet.
	 * 
	 * @see GroupReduceFunction
	 * @see GroupReduceOperator
	 * @see DataSet
	 */
	public <R> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}

	// --------------------------------------------------------------------------------------------
	//  Group Operations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sorts {@link Tuple} elements within a group on the specified field in the specified {@link Order}.</br>
	 * <b>Note: Only groups of Tuple elements can be sorted.</b><br/>
	 * Groups can be sorted by multiple fields by chaining {@link #sortGroup(int, Order)} calls.
	 * 
	 * @param field The Tuple field on which the group is sorted.
	 * @param order The Order in which the specified Tuple field is sorted.
	 * @return A SortedGrouping with specified order of group element.
	 * 
	 * @see Tuple
	 * @see Order
	 */
	public SortedGrouping<T> sortGroup(int field, Order order) {
		
		int pos;
		int[] groupSortKeyPositions;
		Order[] groupSortOrders ;
		
		if (!dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
		}
		if (field >= dataSet.getType().getArity()) {
			throw new IllegalArgumentException("Order key out of tuple bounds.");
		}
		
		groupSortKeyPositions = new int[1];
		groupSortOrders = new Order[1];
		pos = 0;
		
		groupSortKeyPositions[pos] = field;
		groupSortOrders[pos] = order;
		return new SortedGrouping<T>(this.dataSet, this.keys, groupSortKeyPositions, groupSortOrders);
	}
	
//	public <K extends Comparable<K>> Grouping<T> sortGroup(KeySelector<T, K> keyExtractor, Order order) {
//		throw new UnsupportedOperationException("Group sorting not supported for KeyExtractor functions.");
//	}

}
