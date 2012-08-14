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

import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition.
 * 
 * @author Stephan Ewen
 */
public final class LocalProperties implements Cloneable
{
	private Ordering ordering;					// order inside a partition, null if not ordered

	private FieldSet groupedFields;	// fields by which the stream is grouped. null if not grouped.
	
	private FieldSet uniqueFields;		// fields whose value combination is unique in the stream

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor for trivial local properties. No order, no grouping, no uniqueness.
	 */
	public LocalProperties() {}
	
	/**
	 * Creates a new instance of local properties that have only the given ordering as property.
	 * The ordering does automatically imply a grouping, though.
	 * 
	 * @param ordering The ordering represented by these local properties.
	 */
	public LocalProperties(Ordering ordering) {
		this.ordering = ordering;
		this.groupedFields = ordering.getInvolvedIndexes();
	}
	
	/**
	 * Creates a new instance of local properties that have the given ordering as property,
	 * field grouping and field uniqueness. Any of the given parameters may be null. Beware, though,
	 * that a null grouping is inconsistent with a non-null ordering.
	 * 
	 * @param ordering The ordering represented by these local properties.
	 * @param groupedFields The grouped fields for these local properties.
	 * @param uniqueFields The unique fields for these local properties.
	 */
	public LocalProperties(Ordering ordering, FieldSet groupedFields, FieldSet uniqueFields) {
		this.ordering = ordering;
		this.groupedFields = groupedFields;
		this.uniqueFields = uniqueFields;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the key order.
	 * 
	 * @return The key order, or <code>null</code> if nothing is ordered.
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
	 * Gets the grouped fields.
	 * 
	 * @return The grouped fields, or <code>null</code> if nothing is grouped.
	 */
	public FieldSet getGroupedFields() {
		return this.groupedFields;
	}
	
	/**
	 * Sets the fields that are grouped in these data properties.
	 * 
	 * @param groupedFields The fields that are grouped in these data properties.
	 */
	public void setGroupedFields(FieldSet groupedFields) {
		this.groupedFields = groupedFields;	
	}

	/**
	 * Gets the fields whose combination is unique within the data set.
	 * 
	 * @return The unique field combination, or <code>null</code> if nothing is unique.
	 */
	public FieldSet getUniqueFields() {
		return this.uniqueFields;
	}
	
	/**
	 * Sets the fields that are unique in these data properties. This automatically sets the
	 * grouped properties as well, if they have not been set, because unique fields are always
	 * implicitly grouped.
	 * 
	 * @param uniqueFields The fields that are unique in these data properties.
	 */
	public void setUniqueFields(FieldSet uniqueFields) {
		this.uniqueFields = uniqueFields;
		if (this.groupedFields == null) {
			this.groupedFields = this.uniqueFields;
		}
	}
	
	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return ordering == null && this.groupedFields == null && this.uniqueFields == null;
	}

	/**
	 * This method resets the local properties to a state where no properties are given.
	 */
	public void reset() {
		this.ordering = null;
		this.groupedFields = null;
		this.uniqueFields = null;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Filters these properties by what can be preserved through a user function's constant fields set.
	 * 
	 * @param node The optimizer node that potentially modifies the properties.
	 * @param input The input of the node which is relevant.
	 * 
	 * @return True, if the resulting properties are non trivial.
	 */
	public boolean filterByNodesConstantSet(OptimizerNode node, int input) {
		// check, whether the local order is preserved
		if (this.ordering != null) {
			FieldList involvedIndexes = this.ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (!node.isFieldConstant(input, involvedIndexes.get(i))) {
					this.ordering = this.ordering.createNewOrderingUpToIndex(i);
					break;
				}
			}
		}
		
		// check, whether the local key grouping is preserved
		if (this.groupedFields != null) {
			for (Integer index : this.groupedFields) {
				if (!node.isFieldConstant(input, index)) {
					this.groupedFields = null;
					break;
				}
			}
		}
		
		// check, whether the local key grouping is preserved
		if (this.uniqueFields != null) {
			for (Integer index : this.uniqueFields) {
				if (!node.isFieldConstant(input, index)) {
					this.uniqueFields = null;
					break;
				}
			}
		}
		
		return !isTrivial();
	}
	
	public LocalProperties createInterestingLocalProperties(OptimizerNode node, int input)
	{
//		// check, whether the local order is preserved
//		boolean newGrouped = false;
//		Ordering newOrdering = null;
//		FieldSet newGroupedFields = null;
//		
//		
//		// no interesting LocalProperties for input of Unions
//		if (node instanceof UnionNode) return null;
//		
//		
//		// check, whether the local key grouping is preserved
//		if (this.groupedFields != null) {
//			boolean groupingPreserved = true;
//			for (Integer index : this.groupedFields) {
//				if (node.isFieldKept(input, index) == false) {
//					groupingPreserved = false;
//					break;
//				}
//			}
//			
//			if (groupingPreserved) {
//				newGroupedFields = (FieldSet) this.groupedFields.clone();
//				newGrouped = true;
//			}
//		}
//		
//		// check, whether the global order is preserved
//		if (ordering != null) {
//			boolean orderingPreserved = true;
//			ArrayList<Integer> involvedIndexes = ordering.getInvolvedIndexes();
//			for (int i = 0; i < involvedIndexes.size(); i++) {
//				if (node.isFieldKept(input, i) == false) {
//					orderingPreserved = false;
//					break;
//				}
//			}
//			
//			if (orderingPreserved) {
//				newOrdering = ordering.clone();
//			}
//		}
//		
//		if (newGrouped == false && newOrdering == null) {
			return null;	
//		}
//		else {
//			return new LocalProperties(newGrouped, newGroupedFields, newOrdering);
//		}
	}

	/**
	 * Checks, if this set of properties, as interesting properties, is met by the given
	 * properties.
	 * 
	 * @param other
	 *        The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(LocalProperties other)
	{
		if (this.groupedFields != null) {
			// we demand a grouping
			
			// check if the other fields are unique
			if (other.uniqueFields == null || !this.groupedFields.isValidSubset(other.uniqueFields)) {
				// not unique, check whether grouped
				if (other.groupedFields == null || !this.groupedFields.equals(other.groupedFields)) {
					// check whether the ordering does the grouping
					if (other.ordering == null ||
							!other.ordering.getInvolvedIndexes().isValidUnorderedPrefix(this.groupedFields)) {
						return false;
					}
				}
			}
		}
		
		if (this.ordering != null) {
			// we demand an ordering
			if (other.ordering == null || !this.ordering.isMetBy(other.ordering)) {
				return false;
			}
		}
		
		if (this.uniqueFields != null) {
			// we demand field uniqueness
			throw new RuntimeException("Uniqueness as a required property is not supported.");
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
		result = prime * result + (this.ordering == null ? 0 : this.ordering.hashCode());
		result = prime * result + (this.groupedFields == null ? 0 : this.groupedFields.hashCode());
		result = prime * result + (this.uniqueFields == null ? 0 : this.uniqueFields.hashCode());
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
		return (ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering()))) &&
			(groupedFields == other.getGroupedFields() || (groupedFields != null && groupedFields.equals(other.getGroupedFields()))) &&
			(uniqueFields == other.getUniqueFields() || (uniqueFields != null && uniqueFields.equals(other.getUniqueFields())));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "LocalProperties [ordering=" + this.ordering + ", grouped=" + this.groupedFields
			+ ", unique=" + this.uniqueFields + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public LocalProperties clone() {
		LocalProperties newProps = new LocalProperties();
		if (this.ordering != null) {
			newProps.ordering = this.ordering.clone();	
		}
		if (this.groupedFields != null) {
			newProps.groupedFields = this.groupedFields.clone();	
		}
		if (this.uniqueFields != null) {
			newProps.uniqueFields = this.uniqueFields.clone();	
		}
		return newProps;
	}

	/**
	 * Convenience method to create copies without the cloning exception.
	 * 
	 * @return A perfect deep copy of this object.
	 */
	public final LocalProperties createCopy() {
		return this.clone();
	}
}
