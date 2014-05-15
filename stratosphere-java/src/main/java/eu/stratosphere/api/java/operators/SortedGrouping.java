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

import java.util.Arrays;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.GroupReduceFunction;

/**
 * SortedGrouping is an intermediate step for a transformation on a grouped and sorted DataSet.<br/>
 * The following transformation can be applied on sorted groups:
 * <ul>
 * 	<li>{@link Grouping#reduce(ReduceFunction)},</li>
 * </ul>
 * 
 * @param <T> The type of the elements of the sorted and grouped DataSet.
 */
public class SortedGrouping<T> extends Grouping<T> {
	
	private int[] groupSortKeyPositions;
	private Order[] groupSortOrders ;
	
	public SortedGrouping(DataSet<T> set, Keys<T> keys, int field, Order order) {
		super(set, keys);
		
		if (!dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
		}
		if (field >= dataSet.getType().getArity()) {
			throw new IllegalArgumentException("Order key out of tuple bounds.");
		}
		
		this.groupSortKeyPositions = new int[]{field};
		this.groupSortOrders = new Order[]{order};
		
	}
	
	protected int[] getGroupSortKeyPositions() {
		return this.groupSortKeyPositions;
	}
	
	protected Order[] getGroupSortOrders() {
		return this.groupSortOrders;
	}

	/**
	 * Applies a GroupReduce transformation on a grouped and sorted {@link DataSet}.<br/>
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
		
		if (!dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
		}
		if (field >= dataSet.getType().getArity()) {
			throw new IllegalArgumentException("Order key out of tuple bounds.");
		}
		
		int newLength = this.groupSortKeyPositions.length + 1;
		this.groupSortKeyPositions = Arrays.copyOf(this.groupSortKeyPositions, newLength);
		this.groupSortOrders = Arrays.copyOf(this.groupSortOrders, newLength);
		pos = newLength - 1;
		
		this.groupSortKeyPositions[pos] = field;
		this.groupSortOrders[pos] = order;
		return this;
	}
}
