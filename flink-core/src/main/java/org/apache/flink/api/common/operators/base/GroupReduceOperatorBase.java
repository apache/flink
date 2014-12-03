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

package org.apache.flink.api.common.operators.base;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class GroupReduceOperatorBase<IN, OUT, FT extends GroupReduceFunction<IN, OUT>> extends SingleInputOperator<IN, OUT, FT> {

	/** The ordering for the order inside a reduce group. */
	private Ordering groupOrder;

	private boolean combinable;
	
	private Partitioner<?> customPartitioner;
	
	
	public GroupReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(udf, operatorInfo, keyPositions, name);
	}
	
	public GroupReduceOperatorBase(FT udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	public GroupReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions, name);
	}
	
	public GroupReduceOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}

	public GroupReduceOperatorBase(FT udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public GroupReduceOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the order of the elements within a reduce group.
	 * 
	 * @param order The order for the elements in a reduce group.
	 */
	public void setGroupOrder(Ordering order) {
		this.groupOrder = order;
	}

	/**
	 * Gets the order of elements within a reduce group. If no such order has been
	 * set, this method returns null.
	 * 
	 * @return The secondary order.
	 */
	public Ordering getGroupOrder() {
		return this.groupOrder;
	}
	
	/**
	 * Marks the group reduce operation as combinable. Combinable operations may pre-reduce the
	 * data before the actual group reduce operations. Combinable user-defined functions
	 * must implement the interface {@link org.apache.flink.api.common.functions.FlatCombineFunction}.
	 * 
	 * @param combinable Flag to mark the group reduce operation as combinable.
	 */
	public void setCombinable(boolean combinable) {
		// sanity check
		if (combinable && !FlatCombineFunction.class.isAssignableFrom(this.userFunction.getUserCodeClass())) {
			throw new IllegalArgumentException("Cannot set a UDF as combinable if it does not implement the interface " +
					FlatCombineFunction.class.getName());
		} else {
			this.combinable = combinable;
		}
	}
	
	/**
	 * Checks whether the operation is combinable.
	 * 
	 * @return True, if the UDF is combinable, false if not.
	 * 
	 * @see #setCombinable(boolean)
	 */
	public boolean isCombinable() {
		return this.combinable;
	}

	public void setCustomPartitioner(Partitioner<?> customPartitioner) {
		if (customPartitioner != null) {
			int[] keys = getKeyColumns(0);
			if (keys == null || keys.length == 0) {
				throw new IllegalArgumentException("Cannot use custom partitioner for a non-grouped GroupReduce (AllGroupReduce)");
			}
			if (keys.length > 1) {
				throw new IllegalArgumentException("Cannot use the key partitioner for composite keys (more than one key field)");
			}
		}
		this.customPartitioner = customPartitioner;
	}
	
	public Partitioner<?> getCustomPartitioner() {
		return customPartitioner;
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected List<OUT> executeOnCollections(List<IN> inputData, RuntimeContext ctx, boolean mutableObjectSafeMode) throws Exception {
		GroupReduceFunction<IN, OUT> function = this.userFunction.getUserCodeObject();

		UnaryOperatorInformation<IN, OUT> operatorInfo = getOperatorInfo();
		TypeInformation<IN> inputType = operatorInfo.getInputType();

		int[] keyColumns = getKeyColumns(0);

		if (!(inputType instanceof CompositeType) && (keyColumns.length > 0 || groupOrder != null)) {
			throw new InvalidProgramException("Grouping or group-sorting is only possible on composite type.");
		}

		int[] sortColumns = keyColumns;
		boolean[] sortOrderings = new boolean[sortColumns.length];

		if (groupOrder != null) {
			sortColumns = ArrayUtils.addAll(sortColumns, groupOrder.getFieldPositions());
			sortOrderings = ArrayUtils.addAll(sortOrderings, groupOrder.getFieldSortDirections());
		}

		if (inputType instanceof CompositeType) {
			if(sortColumns.length == 0) { // => all reduce. No comparator
				Preconditions.checkArgument(sortOrderings.length == 0);
			} else {
				final TypeComparator<IN> sortComparator = ((CompositeType<IN>) inputType).createComparator(sortColumns, sortOrderings, 0);
	
				Collections.sort(inputData, new Comparator<IN>() {
					@Override
					public int compare(IN o1, IN o2) {
						return sortComparator.compare(o1, o2);
					}
				});
			}
		}

		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, this.parameters);
		
		ArrayList<OUT> result = new ArrayList<OUT>();

		if (keyColumns.length == 0) {
			if (mutableObjectSafeMode) {
				final TypeSerializer<IN> inputSerializer = inputType.createSerializer();
				TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer();
				List<IN> inputDataCopy = new ArrayList<IN>(inputData.size());
				for (IN in: inputData) {
					inputDataCopy.add(inputSerializer.copy(in));
				}
				CopyingListCollector<OUT> collector = new CopyingListCollector<OUT>(result, outSerializer);

				function.reduce(inputDataCopy, collector);
			} else {
				ListCollector<OUT> collector = new ListCollector<OUT>(result);
				function.reduce(inputData, collector);
			}
		} else {
			final TypeSerializer<IN> inputSerializer = inputType.createSerializer();
			boolean[] keyOrderings = new boolean[keyColumns.length];
			final TypeComparator<IN> comparator = ((CompositeType<IN>) inputType).createComparator(keyColumns, keyOrderings, 0);

			ListKeyGroupedIterator<IN> keyedIterator = new ListKeyGroupedIterator<IN>(inputData, inputSerializer, comparator, mutableObjectSafeMode);

			if (mutableObjectSafeMode) {
				TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer();
				CopyingListCollector<OUT> collector = new CopyingListCollector<OUT>(result, outSerializer);

				while (keyedIterator.nextKey()) {
					function.reduce(keyedIterator.getValues(), collector);
				}
			} else {
				ListCollector<OUT> collector = new ListCollector<OUT>(result);
				while (keyedIterator.nextKey()) {
					function.reduce(keyedIterator.getValues(), collector);
				}
			}
		}

		FunctionUtils.closeFunction(function);
		return result;
	}
}
