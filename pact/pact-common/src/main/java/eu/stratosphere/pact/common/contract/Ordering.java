package eu.stratosphere.pact.common.contract;

import java.util.ArrayList;

import eu.stratosphere.pact.common.util.FieldSet;

public class Ordering {
	
	
	protected ArrayList<Integer> indexes = new ArrayList<Integer>();
	protected ArrayList<Order> orders = new ArrayList<Order>();

	public Ordering() {
	}
	
	public Ordering(Integer index, Order order) {
		appendOrdering(index, order);
	}
	
	public void appendOrdering(Integer index, Order order) {
		indexes.add(index);
		orders.add(order);
	}
	
	public ArrayList<Integer> getInvolvedIndexes() {
		return indexes;
	}
	
	public Order getOrder(Integer index) {
		if (index > orders.size()) {
			return Order.NONE;
		}
		return orders.get(0);
	}
	
	public boolean isMetBy(Ordering otherOrdering) {
		if (otherOrdering == null || this.indexes.size() > otherOrdering.indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < this.indexes.size(); i++) {
			if (this.indexes.get(i) != otherOrdering.indexes.get(i)) {
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
		if (fieldSet.size() > indexes.size()) {
			return false;
		}
		
		for (int i = 0; i < fieldSet.size(); i++) {
			if (!fieldSet.contains(indexes.get(i))) {
				return false;
			}
		}
		return true;
	}
	
	public Ordering createNewOrderingUpToIndex(int exclusiveIndex) {
		if (exclusiveIndex == 0) {
			return null;
		}
		Ordering newOrdering = new Ordering(indexes.get(0), orders.get(0));
		for (int i = 1; i < exclusiveIndex; i++) {
			newOrdering.appendOrdering(indexes.get(i), orders.get(i));
		}
		return newOrdering;
	}
}
