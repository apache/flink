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

import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;

/**
 * TODO: add JavaDoc
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class Ordering {
	
	protected FieldList fields = new FieldList();
	protected ArrayList<Order> orders = new ArrayList<Order>();

	public Ordering() {
	}
	
	public Ordering(Integer fieldIndex, Order order) {
		appendOrdering(fieldIndex, order);
	}
	
	public void appendOrdering(Integer fieldIndex, Order order) {
		fields.add(fieldIndex);
		orders.add(order);
	}
	
	public FieldList getInvolvedFields() {
		return fields;
	}
	
	public Order getOrder(Integer index) {
		if (index > orders.size()) {
			return Order.NONE;
		}
		return orders.get(0);
	}
	
	public boolean isMetBy(Ordering otherOrdering) {
		if (otherOrdering == null || this.fields.size() > otherOrdering.fields.size()) {
			return false;
		}
		
		for (int i = 0; i < this.fields.size(); i++) {
			if (this.fields.get(i) != otherOrdering.fields.get(i)) {
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
	
	public boolean groupsFieldSet(FieldSet fieldSet) {
		if (fieldSet.size() > fields.size()) {
			return false;
		}
		
		for (int i = 0; i < fieldSet.size(); i++) {
			if (!fieldSet.contains(fields.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	public Ordering createNewOrderingUpToPos(int exclusivePos) {
		if (exclusivePos == 0) {
			return null;
		}
		Ordering newOrdering = new Ordering(fields.get(0), orders.get(0));
		for (int i = 1; i < exclusivePos; i++) {
			newOrdering.appendOrdering(fields.get(i), orders.get(i));
		}
		return newOrdering;
	}
	
	@SuppressWarnings("unchecked")
	public Ordering clone() {
		Ordering newOrdering = new Ordering();
		newOrdering.fields = (FieldList) this.fields.clone();
		newOrdering.orders = (ArrayList<Order>) this.orders.clone();
		return this;
	}
	
	public String toString() {
		if (fields.size() == 0) {
			return "(none)";
		}
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < fields.size(); i++) {
			if (buf.length() == 0) {
				buf.append("[");
			}
			else {
				buf.append(",");
			}
			buf.append(fields.get(i));
			buf.append(":");
			buf.append(orders.get(i).name());
		}
		buf.append("]");
		return buf.toString();
	}
}
