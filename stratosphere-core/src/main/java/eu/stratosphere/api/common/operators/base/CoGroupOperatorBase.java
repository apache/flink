/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.common.operators.base;

import eu.stratosphere.api.common.functions.GenericCoGrouper;
import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;

/**
 * @see GenericCoGrouper
 */
public class CoGroupOperatorBase<T extends GenericCoGrouper<?, ?, ?>> extends DualInputOperator<T> {
	
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

	
	public CoGroupOperatorBase(UserCodeWrapper<T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, keyPositions1, keyPositions2, name);
		this.combinableFirst = false;
		this.combinableSecond = false;
	}
	
	public CoGroupOperatorBase(T udf, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeObjectWrapper<T>(udf), keyPositions1, keyPositions2, name);
	}
	
	public CoGroupOperatorBase(Class<? extends T> udf, int[] keyPositions1, int[] keyPositions2, String name) {
		this(new UserCodeClassWrapper<T>(udf), keyPositions1, keyPositions2, name);
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the order of the elements within a group for the given input.
	 * 
	 * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
	 * @param order The order for the elements in a group.
	 */
	public void setGroupOrder(int inputNum, Ordering order) {
		if (inputNum == 0)
			this.groupOrder1 = order;
		else if (inputNum == 1)
			this.groupOrder2 = order;
		else
			throw new IndexOutOfBoundsException();
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
		if (inputNum == 0)
			return this.groupOrder1;
		else if (inputNum == 1)
			return this.groupOrder2;
		else
			throw new IndexOutOfBoundsException();
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
}
