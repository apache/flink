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

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;


/**
 * @see GenericGroupReduce
 */
public class GroupReduceOperatorBase<T extends GenericGroupReduce<?, ?>> extends SingleInputOperator<T> {
	

	/**
	 * The ordering for the order inside a reduce group.
	 */
	private Ordering groupOrder;

	private boolean combinable;
	
	
	public GroupReduceOperatorBase(UserCodeWrapper<T> udf, int[] keyPositions, String name) {
		super(udf, keyPositions, name);
		this.combinable = false;
	}
	
	public GroupReduceOperatorBase(T udf, int[] keyPositions, String name) {
		super(new UserCodeObjectWrapper<T>(udf), keyPositions, name);
		this.combinable = false;
	}
	
	public GroupReduceOperatorBase(Class<? extends T> udf, int[] keyPositions, String name) {
		super(new UserCodeClassWrapper<T>(udf), keyPositions, name);
		this.combinable = false;
	}
	
	public GroupReduceOperatorBase(UserCodeWrapper<T> udf, String name) {
		super(udf, name);
		this.combinable = false;
	}
	
	public GroupReduceOperatorBase(T udf, String name) {
		super(new UserCodeObjectWrapper<T>(udf), name);
		this.combinable = false;
	}
	
	public GroupReduceOperatorBase(Class<? extends T> udf, String name) {
		super(new UserCodeClassWrapper<T>(udf), name);
		this.combinable = false;
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
	
	public void setCombinable(boolean combinable) {
		this.combinable = combinable;
	}
	
	public boolean isCombinable() {
		return this.combinable;
	}

}
