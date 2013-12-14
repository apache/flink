/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.operators;

/**
 * Enumeration representing order. May represent no order, an ascending order or a descending order.
 */
public enum Order {
	
	/**
	 * Indicates no order.
	 */
	NONE,

	/**
	 * Indicates an ascending order.
	 */
	ASCENDING,

	/**
	 * Indicates a descending order.
	 */
	DESCENDING,

	/**
	 * Indicates an order without a direction. This constant is not used to indicate
	 * any existing order, but for example to indicate that an order of any direction
	 * is desirable.
	 */
	ANY;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Checks, if this enum constant represents in fact an order. That is,
	 * whether this property is not equal to <tt>Order.NONE</tt>.
	 * 
	 * @return True, if this enum constant is unequal to <tt>Order.NONE</tt>,
	 *         false otherwise.
	 */
	public boolean isOrdered() {
		return this != Order.NONE;
	}
	
	public String getShortName() {
		return this == ASCENDING ? "ASC" : this == DESCENDING ? "DESC" : this == ANY ? "*" : "-";
	}
}
