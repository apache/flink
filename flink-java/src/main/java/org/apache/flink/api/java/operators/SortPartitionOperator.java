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
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.SortPartitionOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;

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


	public SortPartitionOperator(DataSet<T> dataSet, int sortField, Order sortOrder, String sortLocationName) {
		super(dataSet, dataSet.getType());
		this.sortLocationName = sortLocationName;

		int[] flatOrderKeys = getFlatFields(sortField);
		this.appendSorting(flatOrderKeys, sortOrder);
	}

	public SortPartitionOperator(DataSet<T> dataSet, String sortField, Order sortOrder, String sortLocationName) {
		super(dataSet, dataSet.getType());
		this.sortLocationName = sortLocationName;

		int[] flatOrderKeys = getFlatFields(sortField);
		this.appendSorting(flatOrderKeys, sortOrder);
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

		int[] flatOrderKeys = getFlatFields(field);
		this.appendSorting(flatOrderKeys, order);
		return this;
	}

	/**
	 * Appends an additional sort order with the specified field in the specified order to the
	 * local partition sorting of the DataSet.
	 *
	 * @param field The field expression referring to the field of the additional sort order of
	 *                 the local partition sorting.
	 * @param order The order  of the additional sort order of the local partition sorting.
	 * @return The DataSet with sorted local partitions.
	 */
	public SortPartitionOperator<T> sortPartition(String field, Order order) {
		int[] flatOrderKeys = getFlatFields(field);
		this.appendSorting(flatOrderKeys, order);
		return this;
	}

	// --------------------------------------------------------------------------------------------
	//  Key Extraction
	// --------------------------------------------------------------------------------------------

	private int[] getFlatFields(int field) {

		if(!(super.getType() instanceof TupleTypeInfoBase<?>)) {
			throw new InvalidProgramException("Field positions can only be specified on Tuple or " +
					"Case Class types.");
		}
		else {
			// check selected field is sortable type
			TypeInformation<?> sortKeyType = ((TupleTypeInfoBase<?>) super.getType()).getTypeAt(field);
			if (!sortKeyType.isSortKeyType()) {
				throw new InvalidProgramException("Selected sort key is not a sortable type " + sortKeyType);
			}
		}

		Keys.ExpressionKeys<T> ek;
		try {
			ek = new Keys.ExpressionKeys<T>(new int[]{field}, super.getType());
		} catch(IllegalArgumentException iae) {
			throw new InvalidProgramException("Invalid specification of field position.", iae);
		}
		return ek.computeLogicalKeyPositions();
	}

	private int[] getFlatFields(String fields) {

		if(super.getType() instanceof CompositeType) {

			// check selected field is sortable type
			TypeInformation<?> sortKeyType = ((CompositeType<?>) super.getType()).getTypeAt(fields);
			if (!sortKeyType.isSortKeyType()) {
				throw new InvalidProgramException("Selected sort key is not a sortable type " + sortKeyType);
			}

			// compute flat field positions for (nested) sorting fields
			Keys.ExpressionKeys<T> ek;
			try {
				ek = new Keys.ExpressionKeys<T>(new String[]{fields}, super.getType());
			} catch(IllegalArgumentException iae) {
				throw new InvalidProgramException("Invalid specification of field expression.", iae);
			}
			return ek.computeLogicalKeyPositions();
		} else {

			fields = fields.trim();
			if (!(fields.equals("*") || fields.equals("_"))) {
				throw new InvalidProgramException("Output sorting of non-composite types can only be defined on the full type. " +
						"Use a field wildcard for that (\"*\" or \"_\")");
			} else {

				// check if selected field is sortable type
				if (!super.getType().isSortKeyType()) {
					throw new InvalidProgramException("Selected sort key cannot be sorted: " + super.getType());
				}

				return new int[]{0};
			}
		}
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

		Ordering partitionOrdering = new Ordering();
		for (int i = 0; i < this.sortKeyPositions.length; i++) {
			partitionOrdering.appendOrdering(this.sortKeyPositions[i], null, this.sortOrders[i]);
		}

		// distinguish between partition types
		UnaryOperatorInformation<T, T> operatorInfo = new UnaryOperatorInformation<T, T>(getType(), getType());
		SortPartitionOperatorBase<T> noop = new  SortPartitionOperatorBase<T>(operatorInfo, partitionOrdering, name);
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

}
