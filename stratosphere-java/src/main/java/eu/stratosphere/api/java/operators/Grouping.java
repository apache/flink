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
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.ReduceFunction;

public class Grouping<T> {
	
	private final DataSet<T> dataSet;
	
	private final Keys<T> keys;
	
	private int[] groupSortKeyPositions = null;
	private Order[] groupSortOrders = null;

	public Grouping(DataSet<T> set, Keys<T> keys) {
		if (set == null || keys == null)
			throw new NullPointerException();
		
		if (keys.isEmpty()) {
			throw new InvalidProgramException("The grouping keys must not be empty.");
		}

		this.dataSet = set;
		this.keys = keys;
	}
	
	
	public DataSet<T> getDataSet() {
		return this.dataSet;
	}
	
	public Keys<T> getKeys() {
		return this.keys;
	}
	
	public int[] getGroupSortKeyPositions() {
		return this.groupSortKeyPositions;
	}
	
	public Order[] getGroupSortOrders() {
		return this.groupSortOrders;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	public <K extends Comparable<K>> Grouping<T> sortGroup(KeySelector<T, K> keyExtractor, Order order) {
		// TODO
		throw new UnsupportedOperationException("Group sorting not supported for KeyExtractor functions.");
	}
	
	public Grouping<T> sortGroup(String fieldExpression, Order order) {
		// TODO
		throw new UnsupportedOperationException("Group sorting not supported for FieldExpression keys.");
	}
	
	public Grouping<T> sortGroup(int field, Order order) {
		
		int pos;
		
		if(this.groupSortKeyPositions == null) {
			this.groupSortKeyPositions = new int[1];
			this.groupSortOrders = new Order[1];
			pos = 0;
		} else {
			int newLength = this.groupSortKeyPositions.length + 1;
			this.groupSortKeyPositions = Arrays.copyOf(this.groupSortKeyPositions, newLength);
			this.groupSortOrders = Arrays.copyOf(this.groupSortOrders, newLength);
			pos = newLength - 1;
		}
		
		this.groupSortKeyPositions[pos] = field;
		this.groupSortOrders[pos] = order;
		return this;
	}
	
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		if(this.groupSortKeyPositions != null) {
			// TODO
			throw new UnsupportedOperationException("Sorted groups not supported for Aggregation operation at the moment.");
		}
		return new AggregateOperator<T>(this, agg, field);
	}
	
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	public <R> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	
}
