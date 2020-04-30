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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.ExpressionKeys;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.FirstReducer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * SortedGrouping is an intermediate step for a transformation on a grouped and sorted DataSet.
 *
 * <p>The following transformation can be applied on sorted groups:
 * <ul>
 * 	<li>{@link SortedGrouping#reduceGroup(org.apache.flink.api.common.functions.GroupReduceFunction)},</li>
 * </ul>
 *
 * @param <T> The type of the elements of the sorted and grouped DataSet.
 */
@Public
public class SortedGrouping<T> extends Grouping<T> {

	private int[] groupSortKeyPositions;
	private Order[] groupSortOrders;
	private Keys.SelectorFunctionKeys<T, ?> groupSortSelectorFunctionKey = null;

	/*
	 * int sorting keys for tuples
	 */
	public SortedGrouping(DataSet<T> set, Keys<T> keys, int field, Order order) {
		super(set, keys);

		if (!Keys.ExpressionKeys.isSortKey(field, inputDataSet.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		// use int-based expression key to properly resolve nested tuples for grouping
		ExpressionKeys<T> ek = new ExpressionKeys<>(field, inputDataSet.getType());

		this.groupSortKeyPositions = ek.computeLogicalKeyPositions();
		this.groupSortOrders = new Order[groupSortKeyPositions.length];
		Arrays.fill(this.groupSortOrders, order);
	}

	/*
	 * String sorting for Pojos and tuples
	 */
	public SortedGrouping(DataSet<T> set, Keys<T> keys, String field, Order order) {
		super(set, keys);

		if (!Keys.ExpressionKeys.isSortKey(field, inputDataSet.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		// resolve String-field to int using the expression keys
		ExpressionKeys<T> ek = new ExpressionKeys<>(field, inputDataSet.getType());

		this.groupSortKeyPositions = ek.computeLogicalKeyPositions();
		this.groupSortOrders = new Order[groupSortKeyPositions.length];
		Arrays.fill(this.groupSortOrders, order); // if field == "*"
	}

	/*
	 * KeySelector sorting for any data type
	 */
	public <K> SortedGrouping(DataSet<T> set, Keys<T> keys, Keys.SelectorFunctionKeys<T, K> keySelector, Order order) {
		super(set, keys);

		if (!(this.keys instanceof Keys.SelectorFunctionKeys)) {
			throw new InvalidProgramException("Sorting on KeySelector keys only works with KeySelector grouping.");
		}
		TypeInformation<?> sortKeyType = keySelector.getKeyType();
		if (!sortKeyType.isSortKeyType()) {
			throw new InvalidProgramException("Key type " + sortKeyType + " is not sortable.");
		}

		this.groupSortKeyPositions = keySelector.computeLogicalKeyPositions();
		for (int i = 0; i < groupSortKeyPositions.length; i++) {
			groupSortKeyPositions[i] += this.keys.getNumberOfKeyFields();
		}

		this.groupSortSelectorFunctionKey = keySelector;
		this.groupSortOrders = new Order[groupSortKeyPositions.length];
		Arrays.fill(this.groupSortOrders, order);
	}

	// --------------------------------------------------------------------------------------------

	protected int[] getGroupSortKeyPositions() {
		return this.groupSortKeyPositions;
	}

	protected Order[] getGroupSortOrders() {
		return this.groupSortOrders;
	}

	protected Ordering getGroupOrdering() {

		Ordering o = new Ordering();
		for (int i = 0; i < this.groupSortKeyPositions.length; i++) {
			o.appendOrdering(this.groupSortKeyPositions[i], null, this.groupSortOrders[i]);
		}

		return o;
	}

	/**
	 * Uses a custom partitioner for the grouping.
	 *
	 * @param partitioner The custom partitioner.
	 * @return The grouping object itself, to allow for method chaining.
	 */
	public SortedGrouping<T> withPartitioner(Partitioner<?> partitioner) {
		Preconditions.checkNotNull(partitioner);

		getKeys().validateCustomPartitioner(partitioner, null);

		this.customPartitioner = partitioner;
		return this;
	}

	protected Keys.SelectorFunctionKeys<T, ?> getSortSelectionFunctionKey() {
		return this.groupSortSelectorFunctionKey;
	}

	/**
	 * Applies a GroupReduce transformation on a grouped and sorted {@link DataSet}.
	 *
	 * <p>The transformation calls a {@link org.apache.flink.api.common.functions.RichGroupReduceFunction} for each group of the DataSet.
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
				inputDataSet.getType(), Utils.getCallLocationName(), true);
		return new GroupReduceOperator<>(this, resultType, inputDataSet.clean(reducer), Utils.getCallLocationName());
	}

	/**
	 * Applies a GroupCombineFunction on a grouped {@link DataSet}.
	 * A CombineFunction is similar to a GroupReduceFunction but does not perform a full data exchange. Instead, the
	 * CombineFunction calls the combine method once per partition for combining a group of results. This
	 * operator is suitable for combining values into an intermediate format before doing a proper groupReduce where
	 * the data is shuffled across the node for further reduction. The GroupReduce operator can also be supplied with
	 * a combiner by implementing the RichGroupReduce function. The combine method of the RichGroupReduce function
	 * demands input and output type to be the same. The CombineFunction, on the other side, can have an arbitrary
	 * output type.
	 * @param combiner The GroupCombineFunction that is applied on the DataSet.
	 * @return A GroupCombineOperator which represents the combined DataSet.
	 */
	public <R> GroupCombineOperator<T, R> combineGroup(GroupCombineFunction<T, R> combiner) {
		if (combiner == null) {
			throw new NullPointerException("GroupCombine function must not be null.");
		}
		TypeInformation<R> resultType = TypeExtractor.getGroupCombineReturnTypes(combiner,
				this.getInputDataSet().getType(), Utils.getCallLocationName(), true);

		return new GroupCombineOperator<>(this, resultType, inputDataSet.clean(combiner), Utils.getCallLocationName());
	}

	/**
	 * Returns a new set containing the first n elements in this grouped and sorted {@link DataSet}.
	 * @param n The desired number of elements for each group.
	 * @return A GroupReduceOperator that represents the DataSet containing the elements.
	*/
	public GroupReduceOperator<T, T> first(int n) {
		if (n < 1) {
			throw new InvalidProgramException("Parameter n of first(n) must be at least 1.");
		}

		return reduceGroup(new FirstReducer<T>(n));
	}

	// --------------------------------------------------------------------------------------------
	//  Group Operations
	// --------------------------------------------------------------------------------------------

	/**
	 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} elements within a group on the specified field in the specified {@link Order}.
	 *
	 * <p><b>Note: Only groups of Tuple or Pojo elements can be sorted.</b>
	 *
	 * <p>Groups can be sorted by multiple fields by chaining {@link #sortGroup(int, Order)} calls.
	 *
	 * @param field The Tuple field on which the group is sorted.
	 * @param order The Order in which the specified Tuple field is sorted.
	 * @return A SortedGrouping with specified order of group element.
	 *
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Order
	 */
	public SortedGrouping<T> sortGroup(int field, Order order) {
		if (groupSortSelectorFunctionKey != null) {
			throw new InvalidProgramException("Chaining sortGroup with KeySelector sorting is not supported");
		}
		if (!Keys.ExpressionKeys.isSortKey(field, inputDataSet.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		ExpressionKeys<T> ek = new ExpressionKeys<>(field, inputDataSet.getType());

		addSortGroupInternal(ek, order);
		return this;
	}

	/**
	 * Sorts {@link org.apache.flink.api.java.tuple.Tuple} or POJO elements within a group on the specified field in the specified {@link Order}.
	 *
	 * <p><b>Note: Only groups of Tuple or Pojo elements can be sorted.</b>
	 *
	 * <p>Groups can be sorted by multiple fields by chaining {@link #sortGroup(String, Order)} calls.
	 *
	 * @param field The Tuple or Pojo field on which the group is sorted.
	 * @param order The Order in which the specified field is sorted.
	 * @return A SortedGrouping with specified order of group element.
	 *
	 * @see org.apache.flink.api.java.tuple.Tuple
	 * @see Order
	 */
	public SortedGrouping<T> sortGroup(String field, Order order) {
		if (groupSortSelectorFunctionKey != null) {
			throw new InvalidProgramException("Chaining sortGroup with KeySelector sorting is not supported");
		}
		if (!Keys.ExpressionKeys.isSortKey(field, inputDataSet.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		ExpressionKeys<T> ek = new ExpressionKeys<>(field, inputDataSet.getType());

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
		for (; pos < newLength; pos++) {
			this.groupSortKeyPositions[pos] = additionalKeyPositions[pos - off];
			this.groupSortOrders[pos] = order; // use the same order
		}
	}
}
