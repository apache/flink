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
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.SortPartitionOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

/**
 * This operator represents a DataSet with locally sorted partitions.
 *
 * @param <T> The type of the DataSet with locally sorted partitions.
 */
public class SortPartitionOperator<T> extends SingleInputOperator<T, T, SortPartitionOperator<T>> {

	private int[] sortKeyPositions;

	private Order[] sortOrders;

	private final String sortLocationName;

	// for key selector
	private boolean useKeySelector;
	private Order sortOrder;
	private Keys<T> sortKey;


	public SortPartitionOperator(DataSet<T> dataSet, int sortField, Order sortOrder, String sortLocationName) {
		super(dataSet, dataSet.getType());
		this.sortLocationName = sortLocationName;
		this.useKeySelector = false;

		int[] flatOrderKeys = getFlatFields(sortField);
		this.appendSorting(flatOrderKeys, sortOrder);
	}

	public SortPartitionOperator(DataSet<T> dataSet, String sortField, Order sortOrder, String sortLocationName) {
		super(dataSet, dataSet.getType());
		this.sortLocationName = sortLocationName;
		this.useKeySelector = false;

		int[] flatOrderKeys = getFlatFields(sortField);
		this.appendSorting(flatOrderKeys, sortOrder);
	}

	public SortPartitionOperator(DataSet<T> dataSet, Keys<T> sortKey, Order sortOrder, String sortLocationName) {
		super(dataSet, dataSet.getType());
		this.sortLocationName = sortLocationName;
		this.useKeySelector = true;

		if (sortKey instanceof Keys.SelectorFunctionKeys && !((Keys.SelectorFunctionKeys) sortKey).getKeyType().isSortKeyType()) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		this.sortKey = sortKey;
		this.sortOrder = sortOrder;
	}

	/**
	 * Returns whether using key selector or not.
     */
	public boolean useKeySelector() {
		return useKeySelector;
	}

	/**
	 * Appends an additional sort order with the specified field in the specified order to the
	 * local partition sorting of the DataSet.
	 *
	 * @param field The field index of the additional sort order of the local partition sorting.
	 * @param order The order of the additional sort order of the local partition sorting.
	 * @return The DataSet with sorted local partitions.
	 */
	public SortPartitionOperator<T> sortPartition(int field, Order order) {
		if (useKeySelector) {
			throw new InvalidProgramException("Expression keys cannot be appended after selector function keys");
		}

		int[] flatOrderKeys = getFlatFields(field);
		this.appendSorting(flatOrderKeys, order);
		return this;
	}

	/**
	 * Appends an additional sort order with the specified field in the specified order to the
	 * local partition sorting of the DataSet.
	 *
	 * @param field The field expression referring to the field of the additional sort order of
	 *              the local partition sorting.
	 * @param order The order of the additional sort order of the local partition sorting.
	 * @return The DataSet with sorted local partitions.
	 */
	public SortPartitionOperator<T> sortPartition(String field, Order order) {
		if (useKeySelector) {
			throw new InvalidProgramException("Expression keys cannot be appended after selector function keys");
		}

		int[] flatOrderKeys = getFlatFields(field);
		this.appendSorting(flatOrderKeys, order);
		return this;
	}

	/**
	 * Appends an additional sort order with the specified field in the specified order to the
	 * local partition sorting of the DataSet.
	 *
	 * @param keyExtractor The KeySelector function which extracts the key value of the additional
	 *                     sort order of the local partition sorting.
	 * @param order        The order of the additional sort order of the local partition sorting.
	 * @return The DataSet with sorted local partitions.
	 */
	public <K> SortPartitionOperator<T> sortPartition(KeySelector<T, K> keyExtractor, Order order) {
		throw new InvalidProgramException("KeySelector cannot be chained.");
	}

	// --------------------------------------------------------------------------------------------
	//  Key Extraction
	// --------------------------------------------------------------------------------------------

	private int[] getFlatFields(int field) {

		if (!Keys.ExpressionKeys.isSortKey(field, super.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(field, super.getType());
		return ek.computeLogicalKeyPositions();
	}

	private int[] getFlatFields(String fields) {

		if (!Keys.ExpressionKeys.isSortKey(fields, super.getType())) {
			throw new InvalidProgramException("Selected sort key is not a sortable type");
		}

		Keys.ExpressionKeys<T> ek = new Keys.ExpressionKeys<>(fields, super.getType());
		return ek.computeLogicalKeyPositions();
	}

	private void appendSorting(int[] flatOrderFields, Order order) {

		if(this.sortKeyPositions == null) {
			// set sorting info
			this.sortKeyPositions = flatOrderFields;
			this.sortOrders = new Order[flatOrderFields.length];
			Arrays.fill(this.sortOrders, order);
		} else {
			// append sorting info to exising info
			int oldLength = this.sortKeyPositions.length;
			int newLength = oldLength + flatOrderFields.length;
			this.sortKeyPositions = Arrays.copyOf(this.sortKeyPositions, newLength);
			this.sortOrders = Arrays.copyOf(this.sortOrders, newLength);

			for(int i=0; i<flatOrderFields.length; i++) {
				this.sortKeyPositions[oldLength+i] = flatOrderFields[i];
				this.sortOrders[oldLength+i] = order;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translation
	// --------------------------------------------------------------------------------------------

	protected org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateToDataFlow(Operator<T> input) {

		String name = "Sort at " + sortLocationName;

		if (useKeySelector) {
			return translateToDataFlowWithKeyExtractor(input, (Keys.SelectorFunctionKeys<T, ?>) sortKey, name);
		}

		Ordering partitionOrdering = new Ordering();
		for (int i = 0; i < this.sortKeyPositions.length; i++) {
			partitionOrdering.appendOrdering(this.sortKeyPositions[i], null, this.sortOrders[i]);
		}

		// distinguish between partition types
		UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<>(getType(), getType());
		SortPartitionOperatorBase<T> noop = new  SortPartitionOperatorBase<>(operatorInfo, partitionOrdering, name);
		noop.setInput(input);
		if(this.getParallelism() < 0) {
			// use parallelism of input if not explicitly specified
			noop.setParallelism(input.getParallelism());
		} else {
			// use explicitly specified parallelism
			noop.setParallelism(this.getParallelism());
		}

		return noop;

	}

	private <K> org.apache.flink.api.common.operators.SingleInputOperator<?, T, ?> translateToDataFlowWithKeyExtractor(
		Operator<T> input, Keys.SelectorFunctionKeys<T, K> keys, String name) {
		TypeInformation<Tuple2<K, T>> typeInfoWithKey = KeyFunctions.createTypeWithKey(keys);
		Keys.ExpressionKeys<Tuple2<K, T>> newKey = new Keys.ExpressionKeys<>(0, typeInfoWithKey);
		Operator<Tuple2<K, T>> keyedInput = KeyFunctions.appendKeyExtractor(input, keys);
		appendSorting(newKey.computeLogicalKeyPositions(), sortOrder);

		Ordering partitionOrdering = new Ordering();
		for (int i = 0; i < this.sortKeyPositions.length; i++) {
			partitionOrdering.appendOrdering(this.sortKeyPositions[i], null, this.sortOrders[i]);
		}

		// distinguish between partition types
		UnaryOperatorInformation<Tuple2<K, T>, Tuple2<K, T>> operatorInfo = new UnaryOperatorInformation<>(typeInfoWithKey, typeInfoWithKey);
		SortPartitionOperatorBase<Tuple2<K, T>> noop = new SortPartitionOperatorBase<>(operatorInfo, partitionOrdering, name);
		noop.setInput(keyedInput);
		if (this.getParallelism() < 0) {
			// use parallelism of input if not explicitly specified
			noop.setParallelism(input.getParallelism());
		} else {
			// use explicitly specified parallelism
			noop.setParallelism(this.getParallelism());
		}

		return KeyFunctions.appendKeyRemover(noop, keys);
	}

}
