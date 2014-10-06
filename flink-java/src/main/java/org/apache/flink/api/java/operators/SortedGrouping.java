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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.functions.FirstReducer;

import java.util.Arrays;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.Keys.ExpressionKeys;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import com.google.common.base.Preconditions;


/**
 * SortedGrouping is an intermediate step for a transformation on a grouped and sorted DataSet.<br/>
 * The following transformation can be applied on sorted groups:
 * <ul>
 * 	<li>{@link SortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)},</li>
 * </ul>
 * 
 * @param <T> The type of the elements of the sorted and grouped DataSet.
 */
public class SortedGrouping<T> extends Grouping<T> {
	
	private int[] groupSortKeyPositions;
	private Order[] groupSortOrders ;
	
	/*
	 * int sorting keys for tuples
	 */
	public SortedGrouping(DataSet<T> set, Keys<T> keys, int field, Order order) {
		super(set, keys);
		
		if (!dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
		}
		if (field >= dataSet.getType().getArity()) {
			throw new IllegalArgumentException("Order key out of tuple bounds.");
		}
		// use int-based expression key to properly resolve nested tuples for grouping
		ExpressionKeys<T> ek = new ExpressionKeys<T>(new int[]{field}, dataSet.getType());
		this.groupSortKeyPositions = ek.computeLogicalKeyPositions();
		this.groupSortOrders = new Order[groupSortKeyPositions.length];
		Arrays.fill(this.groupSortOrders, order);
	}
	
	/*
	 * String sorting for Pojos and tuples
	 */
	public SortedGrouping(DataSet<T> set, Keys<T> keys, String field, Order order) {
		super(set, keys);
		
		if (!(dataSet.getType() instanceof CompositeType)) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for composite data types (pojo / tuple / case class)");
		}
		// resolve String-field to int using the expression keys
		ExpressionKeys<T> ek = new ExpressionKeys<T>(new String[]{field}, dataSet.getType());
		this.groupSortKeyPositions = ek.computeLogicalKeyPositions();
		this.groupSortOrders = new Order[groupSortKeyPositions.length];
		Arrays.fill(this.groupSortOrders, order); // if field == "*"
	}
	
	protected int[] getGroupSortKeyPositions() {
		return this.groupSortKeyPositions;
	}
	
	protected Order[] getGroupSortOrders() {
		return this.groupSortOrders;
	}

	/**
	 * Applies a GroupReduce transformation on a grouped and sorted {@link DataSet}.<br/>
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
		TypeInformation<R> resultType = TypeExtractor.getGroupReduceReturnTypes(reducer,
				this.getDataSet().getType());
		return new GroupReduceOperator<T, R>(this, resultType, reducer);
	}

	
	/**
	 * Returns a new set containing the first n elements in this grouped and sorted {@link DataSet}.<br/>
	 * @param n The desired number of elements for each group.
	 * @return A ReduceGroupOperator that represents the DataSet containing the elements.
	*/
	public GroupReduceOperator<T, T> first(int n) {
		if(n < 1) {
			throw new InvalidProgramException("Parameter n of first(n) must be at least 1.");
		}
		
		return reduceGroup(new FirstReducer<T>(n));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Group Operations
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} elements within a group on the specified field in the specified {@link Order}.</br>
	 * <b>Note: Only groups of Tuple or Pojo elements can be sorted.</b><br/>
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
		
		if (!dataSet.getType().isTupleType()) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for tuple data types");
		}
		if (field >= dataSet.getType().getArity()) {
			throw new IllegalArgumentException("Order key out of tuple bounds.");
		}
		ExpressionKeys<T> ek = new ExpressionKeys<T>(new int[]{field}, dataSet.getType());
		addSortGroupInternal(ek, order);
		return this;
	}
	
	private void addSortGroupInternal(ExpressionKeys<T> ek, Order order) {
		Preconditions.checkArgument(order != null, "Order can not be null");
		int[] additionalKeyPositions = ek.computeLogicalKeyPositions();
		
		int newLength = this.groupSortKeyPositions.length + additionalKeyPositions.length;
		this.groupSortKeyPositions = Arrays.copyOf(this.groupSortKeyPositions, newLength);
		this.groupSortOrders = Arrays.copyOf(this.groupSortOrders, newLength);
		int pos = newLength - additionalKeyPositions.length;
		int off = newLength - additionalKeyPositions.length;
		for(;pos < newLength; pos++) {
			this.groupSortKeyPositions[pos] = additionalKeyPositions[pos - off];
			this.groupSortOrders[pos] = order; // use the same order
		}
	}
	
	/**
	 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} or POJO elements within a group on the specified field in the specified {@link Order}.</br>
	 * <b>Note: Only groups of Tuple or Pojo elements can be sorted.</b><br/>
	 * Groups can be sorted by multiple fields by chaining {@link #sortGroup(String, Order)} calls.
	 * 
	 * @param field The Tuple or Pojo field on which the group is sorted.
	 * @param order The Order in which the specified field is sorted.
	 * @return A SortedGrouping with specified order of group element.
	 * 
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Order
	 */
	public SortedGrouping<T> sortGroup(String field, Order order) {
		
		if (! (dataSet.getType() instanceof CompositeType)) {
			throw new InvalidProgramException("Specifying order keys via field positions is only valid for composite data types (pojo / tuple / case class)");
		}
		ExpressionKeys<T> ek = new ExpressionKeys<T>(new String[]{field}, dataSet.getType());
		addSortGroupInternal(ek, order);
		return this;
	}
	
}
