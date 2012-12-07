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

package eu.stratosphere.pact.compiler.dataproperties;

import java.util.HashSet;
import java.util.Set;

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
	private Ordering ordering;			// order inside a partition, null if not ordered

	private FieldList groupedFields;		// fields by which the stream is grouped. null if not grouped.
	
	private Set<FieldSet> uniqueFields;		// fields whose value combination is unique in the stream

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Default constructor for trivial local properties. No order, no grouping, no uniqueness.
	 */
	public LocalProperties() {}
	
	/**
	 * Creates a new instance of local properties that have the given ordering as property,
	 * field grouping and field uniqueness. Any of the given parameters may be null. Beware, though,
	 * that a null grouping is inconsistent with a non-null ordering.
	 * <p>
	 * This constructor is used only for internal copy creation.
	 * 
	 * @param ordering The ordering represented by these local properties.
	 * @param groupedFields The grouped fields for these local properties.
	 * @param uniqueFields The unique fields for these local properties.
	 */
	private LocalProperties(Ordering ordering, FieldList groupedFields, Set<FieldSet> uniqueFields) {
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
		this.groupedFields = ordering.getInvolvedIndexes();
	}
	
	/**
	 * Gets the grouped fields.
	 * 
	 * @return The grouped fields, or <code>null</code> if nothing is grouped.
	 */
	public FieldList getGroupedFields() {
		return this.groupedFields;
	}
	
	/**
	 * Sets the fields that are grouped in these data properties.
	 * 
	 * @param groupedFields The fields that are grouped in these data properties.
	 */
	public void setGroupedFields(FieldList groupedFields) {
		this.groupedFields = groupedFields;	
	}

	/**
	 * Gets the fields whose combination is unique within the data set.
	 * 
	 * @return The unique field combination, or <code>null</code> if nothing is unique.
	 */
	public Set<FieldSet> getUniqueFields() {
		return this.uniqueFields;
	}
	
	public boolean areFieldsUnique(FieldSet set) {
		return this.uniqueFields != null && this.uniqueFields.contains(set);
	}
	
	/**
	 * Adds a combination of fields that are unique in these data properties.
	 * 
	 * @param uniqueFields The fields that are unique in these data properties.
	 */
	public void addUniqueFields(FieldSet uniqueFields) {
		if (this.uniqueFields == null) {
			this.uniqueFields = new HashSet<FieldSet>();
		}
		this.uniqueFields.add(uniqueFields);
	}
	
	public void clearUniqueFieldSets() {
		if (this.uniqueFields != null) {
			this.uniqueFields = null;
		}
	}
	
	public boolean areFieldsGrouped(FieldSet set) {
		return this.groupedFields != null && this.groupedFields.isValidUnorderedPrefix(set);
	}
	
	public boolean meetsOrderingConstraint(Ordering o) {
		return o.isMetBy(this.ordering);
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
	public LocalProperties filterByNodesConstantSet(OptimizerNode node, int input)
	{
		// check, whether the local order is preserved
		Ordering no = this.ordering;
		FieldList ngf = this.groupedFields;
		Set<FieldSet> nuf = this.uniqueFields;
		
		if (this.ordering != null) {
			final FieldList involvedIndexes = this.ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (!node.isFieldConstant(input, involvedIndexes.get(i))) {
					if (i == 0) {
						no = null;
						ngf = null;
					} else {
						no = this.ordering.createNewOrderingUpToIndex(i);
						ngf = no.getInvolvedIndexes();
					}
					break;
				}
			}
		} else if (this.groupedFields != null) {
			// check, whether the local key grouping is preserved
			for (Integer index : this.groupedFields) {
				if (!node.isFieldConstant(input, index)) {
					ngf = null;
				}
			}
		}
		
		// check, whether the local key grouping is preserved
		if (this.uniqueFields != null) {
			Set<FieldSet> s = new HashSet<FieldSet>(this.uniqueFields);
			for (FieldSet fields : this.uniqueFields) {
				for (Integer index : fields) {
					if (!node.isFieldConstant(input, index)) {
						s.remove(fields);
						break;
					}
				}
			}
			if (s.size() != this.uniqueFields.size()) {
				nuf = s;
			}
		}
		
		return (no == this.ordering && ngf == this.groupedFields && nuf == this.uniqueFields) ? this :
			   (no == null && ngf == null && nuf == null) ? null :
					new LocalProperties(no, ngf, nuf);
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
		if (obj instanceof LocalProperties) {
			final LocalProperties other = (LocalProperties) obj;
			return (ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering()))) &&
				(groupedFields == other.getGroupedFields() || (groupedFields != null && groupedFields.equals(other.getGroupedFields()))) &&
				(uniqueFields == other.getUniqueFields() || (uniqueFields != null && uniqueFields.equals(other.getUniqueFields())));
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
		return "LocalProperties [ordering=" + this.ordering + ", grouped=" + this.groupedFields
			+ ", unique=" + this.uniqueFields + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public LocalProperties clone() {
		return new LocalProperties(this.ordering, this.groupedFields,
			this.uniqueFields == null ? null : new HashSet<FieldSet>(this.uniqueFields));
	}
}
