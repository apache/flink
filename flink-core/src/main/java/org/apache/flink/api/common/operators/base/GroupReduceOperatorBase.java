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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
@Internal
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
	 * must implement the interface {@link GroupCombineFunction}.
	 * 
	 * @param combinable Flag to mark the group reduce operation as combinable.
	 */
	public void setCombinable(boolean combinable) {
		// sanity check
		if (combinable && !GroupCombineFunction.class.isAssignableFrom(this.userFunction.getUserCodeClass())) {
			throw new IllegalArgumentException("Cannot set a UDF as combinable if it does not implement the interface " +
					GroupCombineFunction.class.getName());
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

	private TypeComparator<IN> getTypeComparator(TypeInformation<IN> typeInfo, int[] sortColumns, boolean[] sortOrderings, ExecutionConfig executionConfig) {
		if (typeInfo instanceof CompositeType) {
			return ((CompositeType<IN>) typeInfo).createComparator(sortColumns, sortOrderings, 0, executionConfig);
		} else if (typeInfo instanceof AtomicType) {
			return ((AtomicType<IN>) typeInfo).createComparator(sortOrderings[0], executionConfig);
		}

		throw new InvalidProgramException("Input type of GroupReduce must be one of composite types or atomic types.");
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected List<OUT> executeOnCollections(List<IN> inputData, RuntimeContext ctx, ExecutionConfig executionConfig) throws Exception {
		GroupReduceFunction<IN, OUT> function = this.userFunction.getUserCodeObject();

		UnaryOperatorInformation<IN, OUT> operatorInfo = getOperatorInfo();
		TypeInformation<IN> inputType = operatorInfo.getInputType();

		int[] keyColumns = getKeyColumns(0);
		int[] sortColumns = keyColumns;
		boolean[] sortOrderings = new boolean[sortColumns.length];

		if (groupOrder != null) {
			sortColumns = ArrayUtils.addAll(sortColumns, groupOrder.getFieldPositions());
			sortOrderings = ArrayUtils.addAll(sortOrderings, groupOrder.getFieldSortDirections());
		}

		if(sortColumns.length == 0) { // => all reduce. No comparator
			checkArgument(sortOrderings.length == 0);
		} else {
			final TypeComparator<IN> sortComparator = getTypeComparator(inputType, sortColumns, sortOrderings, executionConfig);
			Collections.sort(inputData, new Comparator<IN>() {
				@Override
				public int compare(IN o1, IN o2) {
					return sortComparator.compare(o1, o2);
				}
			});
		}

		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, this.parameters);
		
		ArrayList<OUT> result = new ArrayList<OUT>();

		if (inputData.size() > 0) {
			final TypeSerializer<IN> inputSerializer = inputType.createSerializer(executionConfig);
			if (keyColumns.length == 0) {
				TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer(executionConfig);
				List<IN> inputDataCopy = new ArrayList<IN>(inputData.size());
				for (IN in : inputData) {
					inputDataCopy.add(inputSerializer.copy(in));
				}
				CopyingListCollector<OUT> collector = new CopyingListCollector<OUT>(result, outSerializer);

				function.reduce(inputDataCopy, collector);
			} else {
				boolean[] keyOrderings = new boolean[keyColumns.length];
				final TypeComparator<IN> comparator = getTypeComparator(inputType, keyColumns, keyOrderings, executionConfig);

				ListKeyGroupedIterator<IN> keyedIterator = new ListKeyGroupedIterator<IN>(inputData, inputSerializer, comparator);

				TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer(executionConfig);
				CopyingListCollector<OUT> collector = new CopyingListCollector<OUT>(result, outSerializer);

				while (keyedIterator.nextKey()) {
					function.reduce(keyedIterator.getValues(), collector);
				}
			}
		}

		FunctionUtils.closeFunction(function);
		return result;
	}
}
