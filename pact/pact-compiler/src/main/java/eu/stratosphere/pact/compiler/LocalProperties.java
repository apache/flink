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

package eu.stratosphere.pact.compiler;

import java.util.ArrayList;

import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class LocalProperties implements Cloneable {
	private Ordering ordering = null; // order inside a partition

	private FieldSet groupedFields = null;
	
	private boolean grouped = false; // flag indicating whether the keys are grouped

	/**
	 * Default constructor. Initiates the order to NONE and the uniqueness to false.
	 */
	public LocalProperties() {
	}
	
	
	public LocalProperties(boolean grouped, FieldSet groupedFields, Ordering ordering) {
		this.grouped = grouped;
		this.groupedFields = groupedFields;
		this.ordering = ordering;
	}


	/**
	 * Gets the key order.
	 * 
	 * @return The key order.
	 */
	public Ordering getOrdering() {
		return ordering;
	}

	/**
	 * Sets the key order for these global properties.
	 * 
	 * @param keyOrder
	 *        The key order to set.
	 */
	public void setOrdering(Ordering ordering) {
		this.ordering = ordering;
	}


	/**
	 * Checks whether the keys are grouped.
	 * 
	 * @return True, if the keys are grouped, false otherwise.
	 */
	public boolean isGrouped() {
		return this.grouped;
	}

	public FieldSet getGroupedFields() {
		return this.groupedFields;
	}
	

	/**
	 * Sets the flag that indicates whether the keys are grouped.
	 * 
	 * @param keysGrouped
	 *        The keys-grouped flag to set.
	 */
	public void setGrouped(boolean isGrouped, FieldSet groupedFields) {
		this.grouped = isGrouped;
		if (isGrouped) {
			this.groupedFields = groupedFields;	
		}
	}

	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		//return keyOrder == Order.NONE && !keyUnique && !keysGrouped;
		return !this.grouped && ordering == null;
	}

	/**
	 * This method resets the local properties to a state where no properties are given.
	 */
	public void reset() {
		this.ordering = null;
		this.grouped = false;
		this.groupedFields = null;
	}

	/**
	 * Filters these properties by what can be preserved through the given output contract.
	 * 
	 * @param contract
	 *        The output contract.
	 * @return True, if any non-default value is preserved, false otherwise.
	 */
	public boolean filterByNodesConstantSet(OptimizerNode node, int input) {
		
		// check, whether the local order is preserved
		if (ordering != null) {
			ArrayList<Integer> involvedIndexes = ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (node.isFieldKept(input, involvedIndexes.get(i)) == false) {
					ordering = ordering.createNewOrderingUpToIndex(i);
					break;
				}
			}
		}
		
		// check, whether the local key grouping is preserved
		if (this.groupedFields != null) {
			for (Integer index : this.groupedFields) {
				if (node.isFieldKept(input, index) == false) {
					this.groupedFields = null;
					this.grouped = false;
					break;
				}
			}	
		}
		else {
			this.grouped = false;
		}
		
		return !isTrivial();
	}
	
	public LocalProperties createInterestingLocalProperties(OptimizerNode node, int input) {
		// check, whether the local order is preserved
		boolean newGrouped = false;
		Ordering newOrdering = null;
		FieldSet newGroupedFields = null;
		
		
		// check, whether the local key grouping is preserved		
		if (this.groupedFields != null) {
			boolean groupingPreserved = true;
			for (Integer index : this.groupedFields) {
				if (node.isFieldKept(input, index) == false) {
					groupingPreserved = false;
					break;
				}
			}
			
			if (groupingPreserved) {
				newGroupedFields = (FieldSet) this.groupedFields.clone();
				newGrouped = true;
			}
		}
		
		// check, whether the global order is preserved
		if (ordering != null) {
			boolean orderingPreserved = true;
			ArrayList<Integer> involvedIndexes = ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (node.isFieldKept(input, i) == false) {
					orderingPreserved = false;
					break;
				}
			}
			
			if (orderingPreserved) {
				newOrdering = ordering.clone();
			}
		}
		
		if (newGrouped == false && newOrdering == null) {
			return null;	
		}
		else {
			return new LocalProperties(newGrouped, newGroupedFields, newOrdering);
		}
	}

	/**
	 * Checks, if this set of properties, as interesting properties, is met by the given
	 * properties.
	 * 
	 * @param other
	 *        The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(LocalProperties other) {

		// check the grouping. if this one requests a grouping, then an
		// order or a grouping are good.
		boolean groupingFulfilled = false;
		
		if (this.grouped) {
			if (other.isGrouped()) {
				groupingFulfilled = this.groupedFields.equals(other.groupedFields);
			}
			if (!groupingFulfilled && other.getOrdering() != null) {
				ArrayList<Integer> otherIndexes = other.getOrdering().getInvolvedIndexes();
				if (groupedFields.size() > otherIndexes.size()) {
					return false;
				}
				
				for (int i = 0; i < groupedFields.size(); i++) {
					if (groupedFields.contains(otherIndexes.get(i)) == false) {
						return false;
					}
				}
				groupingFulfilled = true;
			}
			
			if (groupingFulfilled == false) {
				return false;
			}
		}
		// check the order
		if (this.ordering != null && this.ordering.isMetBy(other.getOrdering()) == false) {
			return false;
		}
		
		return true;
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ordering == null) ? 0 : ordering.hashCode());
		result = prime * result + ((groupedFields == null) ? 0 : groupedFields.hashCode());
		result = prime * result + (grouped ? 1231 : 1237);
		

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		}

		LocalProperties other = (LocalProperties) obj;
		if ((ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering())))
			&& this.grouped == other.grouped 
			&& (this.groupedFields == other.groupedFields || (this.groupedFields != null && this.groupedFields.equals(other.groupedFields)))) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "LocalProperties [ordering=" + ordering+ ", grouped=" + grouped
				+ " on " + groupedFields
				// + ", keyUnique=" + keyUnique 
			+ "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public LocalProperties clone() throws CloneNotSupportedException {
		LocalProperties newProps = (LocalProperties) super.clone();
		if (this.ordering != null) {
			newProps.ordering = this.ordering.clone();	
		}
		if (this.groupedFields != null) {
			newProps.groupedFields = (FieldSet) this.groupedFields.clone();	
		}
		return newProps;
	}

	/**
	 * Convenience method to create copies without the cloning exception.
	 * 
	 * @return A perfect deep copy of this object.
	 */
	public final LocalProperties createCopy() {
		try {
			return this.clone();
		} catch (CloneNotSupportedException cnse) {
			// should never happen, but propagate just in case
			throw new RuntimeException(cnse);
		}
	}
}
