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

import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;

/**
 * @see org.apache.flink.api.common.functions.CoGroupFunction
 */
public class CoGroupOperatorBase<IN1, IN2, OUT, FT extends CoGroupFunction<IN1, IN2, OUT>> extends DualInputOperator<IN1, IN2, OUT, FT> {
	
	/**
	 * The ordering for the order inside a group from input one.
	 */
	private Ordering groupOrder1;
	
	/**
	 * The ordering for the order inside a group from input two.
	 */
	private Ordering groupOrder2;
	
	// --------------------------------------------------------------------------------------------

	private boolean combinableFirst;

	private boolean combinableSecond;

	public CoGroupOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, operatorInfo, keyPositions1, keyPositions2, name);
		this.combinableFirst = false;
		this.combinableSecond = false;
	}
	
	public CoGroupOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}
	
	public CoGroupOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the order of the elements within a group for the given input.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrder(int inputNum, Ordering order) {
		if (inputNum == 0) {
			this.groupOrder1 = order;
		} else if (inputNum == 1) {
			this.groupOrder2 = order;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Sets the order of the elements within a group for the first input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputOne(Ordering order) {
		setGroupOrder(0, order);
	}
	
	/**
	 * Sets the order of the elements within a group for the second input.
	 * 
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrderForInputTwo(Ordering order) {
		setGroupOrder(1, order);
	}
	
	/**
	 * Gets the value order for an input, i.e. the order of elements within a group.
	 * If no such order has been set, this method returns null.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @return The group order.
	 */
	public Ordering getGroupOrder(int inputNum) {
		if (inputNum == 0) {
			return this.groupOrder1;
		} else if (inputNum == 1) {
			return this.groupOrder2;
		} else {
			throw new IndexOutOfBoundsException();
		}
	}
	
	/**
	 * Gets the order of elements within a group for the first input.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The group order for the first input.
	 */
	public Ordering getGroupOrderForInputOne() {
		return getGroupOrder(0);
	}
	
	/**
	 * Gets the order of elements within a group for the second input.
	 * If no such order has been set, this method returns null.
	 * 
	 * @return The group order for the second input.
	 */
	public Ordering getGroupOrderForInputTwo() {
		return getGroupOrder(1);
	}
	
	// --------------------------------------------------------------------------------------------

	public boolean isCombinableFirst() {
		return this.combinableFirst;
	}
	
	public void setCombinableFirst(boolean combinableFirst) {
		this.combinableFirst = combinableFirst;
	}
	
	public boolean isCombinableSecond() {
		return this.combinableSecond;
	}

	public void setCombinableSecond(boolean combinableSecond) {
		this.combinableSecond = combinableSecond;
	}

	@Override
	protected List<OUT> executeOnCollections(List<IN1> inputData1, List<IN2> inputData2, RuntimeContext runtimeContext) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
