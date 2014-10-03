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


import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.FlatCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.CompositeType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @see org.apache.flink.api.common.functions.GroupReduceFunction
 */
public class GroupReduceOperatorBase<IN, OUT, FT extends GroupReduceFunction<IN, OUT>> extends SingleInputOperator<IN, OUT, FT> {

	/**
	 * The ordering for the order inside a reduce group.
	 */
	private Ordering groupOrder;

	private boolean combinable;
	
	
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
	
	// --------------------------------------------------------------------------------------------
	
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

	// --------------------------------------------------------------------------------------------

	@Override
	protected List<OUT> executeOnCollections(List<IN> inputData, RuntimeContext ctx)
			throws Exception {
		GroupReduceFunction<IN, OUT> function = this.userFunction.getUserCodeObject();

		UnaryOperatorInformation<IN, OUT> operatorInfo = getOperatorInfo();
		TypeInformation<IN> inputType = operatorInfo.getInputType();

		if (!(inputType instanceof CompositeType)) {
			throw new InvalidProgramException("Input type of groupReduce operation must be" +
					" composite type.");
		}

		int[] inputColumns = getKeyColumns(0);
		boolean[] inputOrderings = new boolean[inputColumns.length];
		final TypeComparator<IN> inputComparator =
				((CompositeType<IN>) inputType).createComparator(inputColumns, inputOrderings);

		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, this.parameters);


		ArrayList<OUT> result = new ArrayList<OUT>(inputData.size());
		ListCollector<OUT> collector = new ListCollector<OUT>(result);

		inputData.sort( new Comparator<IN>() {
			@Override
			public int compare(IN o1, IN o2) {
				return - inputComparator.compare(o1, o2);
			}
		});
		ListKeyGroupedIterator<IN> keyedIterator =
				new ListKeyGroupedIterator<IN>(inputData, inputComparator);

		while (keyedIterator.nextKey()) {
			function.reduce(keyedIterator.getValues(), collector);
		}

		FunctionUtils.closeFunction(function);

		return result;
	}

}
