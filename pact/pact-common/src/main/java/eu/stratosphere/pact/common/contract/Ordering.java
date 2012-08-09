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

package eu.stratosphere.pact.common.contract;

import java.util.ArrayList;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;

/**
 *
 *
 */
public class Ordering
{	
	protected final FieldList indexes = new FieldList();
	
	protected final ArrayList<Class<? extends Key>> types = new ArrayList<Class<? extends Key>>();
	
	protected final ArrayList<Order> orders = new ArrayList<Order>();

	// --------------------------------------------------------------------------------------------
	
	/**
	 * 
	 */
	public Ordering() {
	}
	
	/**
	 * @param index
	 * @param type
	 * @param order
	 */
	public Ordering(int index, Class<? extends Key> type, Order order) {
		appendOrdering(index, type, order);
	}
	
	
	/**
	 * @param index
	 * @param type
	 * @param order
	 * @return
	 */
	public Ordering appendOrdering(int index, Class<? extends Key> type, Order order)
	{
		if (index < 0) {
			throw new IllegalArgumentException("The key index must not be negative.");
		}
		if (type == null || order == null) {
			throw new NullPointerException();
		}
		if (order == Order.NONE) {
			throw new IllegalArgumentException("An ordering must not be created with a NONE order.");
		}
		
		this.indexes.add(index);
		this.types.add(type);
		this.orders.add(order);
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public FieldList getInvolvedIndexes() {
		return this.indexes;
	}
	
	public int getNumberOfFields() {
		return this.indexes.size();
	}
	
	public int getFieldNumber(int index) {
		if (index < 0 || index >= this.indexes.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		
		return this.indexes.get(index);
	}
	
	public Class<? extends Key> getType(int index)
	{
		if (index < 0 || index >= this.types.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return this.types.get(index);
	}
	
	public Order getOrder(int index)
	{
		if (index < 0 || index >= this.types.size()) {
			throw new IndexOutOfBoundsException(String.valueOf(index));
		}
		return orders.get(index);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public Class<? extends Key>[] getTypes()
	{
		return this.types.toArray(new Class[this.types.size()]);
	}
	
	public int[] getFieldPositions() {
		final int[] ia = new int[this.indexes.size()];
		for (int i = 0; i < ia.length; i++) {
			ia[i] = this.indexes.get(i);
		}
		return ia;
	}
	
	public boolean[] getFieldSortDirections() {
		final boolean[] directions = new boolean[this.orders.size()];
		for (int i = 0; i < directions.length; i++) {
			directions[i] = this.orders.get(i) != Order.DESCENDING; 
		}
		return directions;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean isMetBy(Ordering otherOrdering)
	{
		if (otherOrdering == null || this.indexes.size() > otherOrdering.indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < this.indexes.size(); i++) {
			if (this.indexes.get(i) != otherOrdering.indexes.get(i)) {
				return false;
			}
			
			if(this.types.get(i) != otherOrdering.types.get(i)) {
				return false;
			}
			
			// if this one request no order, everything is good
			if (this.orders.get(i) != Order.NONE) {
				if (this.orders.get(i) == Order.ANY) {
					// if any order is requested, any not NONE order is good
					if (otherOrdering.orders.get(i) == Order.NONE) {
						return false;
					}
				} else if (otherOrdering.orders.get(i) != this.orders.get(i)) {
					// the orders must be equal
					return false;
				}
			}
		}	
		return true;
	}
	
	public boolean groupsFieldSet(FieldSet fieldSet)
	{
		if (fieldSet.size() > this.indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < fieldSet.size(); i++) {
			if (!fieldSet.contains(this.indexes.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * @param exclusiveIndex
	 * @return
	 */
	public Ordering createNewOrderingUpToIndex(int exclusiveIndex)
	{
		if (exclusiveIndex == 0) {
			return null;
		}
		final Ordering newOrdering = new Ordering(this.indexes.get(0),
										this.types.get(0), this.orders.get(0));
		for (int i = 1; i < exclusiveIndex; i++) {
			newOrdering.appendOrdering(this.indexes.get(i), this.types.get(i), this.orders.get(i));
		}
		return newOrdering;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public Ordering clone()
	{
		final Ordering newOrdering = new Ordering();
		newOrdering.indexes.addAll(this.indexes);
		newOrdering.types.addAll(this.types);
		newOrdering.orders.addAll(this.orders);
		return newOrdering;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		if (this.indexes.size() == 0) {
			return "(none)";
		}
		final StringBuffer buf = new StringBuffer();
		for (int i = 0; i < indexes.size(); i++) {
			if (buf.length() == 0) {
				buf.append("[");
			}
			else {
				buf.append(",");
			}
			buf.append(this.indexes.get(i));
			if (this.types.get(i) != null) {
				buf.append(":");
				buf.append(this.types.get(i).getName());
			}
			buf.append(":");
			buf.append(this.orders.get(i).name());
		}
		buf.append("]");
		return buf.toString();
	}
}
