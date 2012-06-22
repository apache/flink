/***********************************************************************************************************************
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import java.util.ArrayList;

import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;

/**
 * This class represents global properties of the data. Global properties are properties that
 * describe data across different partitions.
 * NOTE: Currently, this class has a very simple property about the partitioning, namely simply whether
 * the data is partitioned on the key. Later, we might need to replace that by tracking partition maps.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class GlobalProperties implements Cloneable {
	private FieldList partitionedFields;

	private PartitionProperty partitioning; // the partitioning

	private Ordering ordering; // order across all partitions

	// across all partitions

	/**
	 * Initializes the global properties with no partitioning, no order and no uniqueness.
	 */
	public GlobalProperties() {
		partitioning = PartitionProperty.NONE;
		ordering = null;
	}

	/**
	 * Initializes the global properties with the given partitioning, order and uniqueness.
	 * 
	 * @param partitioning
	 *        The partitioning property.
	 * @param keyOrder
	 *        The order property.
	 * @param keyUnique
	 *        The flag that indicates, whether the keys are unique.
	 */
	public GlobalProperties(PartitionProperty partitioning, Ordering ordering, FieldList partitionedFields) {
		this.partitioning = partitioning;
		this.ordering = ordering;
		this.partitionedFields = partitionedFields;
	}

	public FieldList getPartitionedFields() {
		return partitionedFields;
	}

	/**
	 * Gets the partitioning property.
	 * 
	 * @return The partitioning property.
	 */
	public PartitionProperty getPartitioning() {
		return partitioning;
	}

	/**
	 * Sets the partitioning property for the global properties.
	 * 
	 * @param partitioning
	 *        The new partitioning to set.
	 */
	public void setPartitioning(PartitionProperty partitioning, FieldList partitionedFields) {
		this.partitioning = partitioning;
		this.partitionedFields = partitionedFields;
	}

	/**
	 * Gets the key order.
	 * 
	 * @return The key order.
	 */
	public Ordering getOrdering() {
		return this.ordering;
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
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return partitioning == PartitionProperty.NONE && ordering == null;
	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitionedFields = null;
		this.partitioning = PartitionProperty.NONE;
		this.ordering = null;
	}

	/**
	 * Filters these properties by what can be preserved through the given output contract.
	 * 
	 * @param contract
	 *        The output contract.
	 * @return True, if any non-default value is preserved, false otherwise.
	 */
	public boolean filterByNodesConstantSet(OptimizerNode node, int input) {

		// check if partitioning survives
		if (partitionedFields != null) {
			for (Integer index : partitionedFields) {
				if (node.isFieldKept(input, index) == false) {
					partitionedFields = null;
					partitioning = PartitionProperty.NONE;
				}
			}
		}

		// check, whether the global order is preserved
		if (ordering != null) {
			ArrayList<Integer> involvedIndexes = ordering.getInvolvedFields();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (node.isFieldKept(input, i) == false) {
					ordering = ordering.createNewOrderingUpToPos(i);
					break;
				}
			}
		}

		return !isTrivial();
	}

	public GlobalProperties createInterestingGlobalProperties(OptimizerNode node, int input) {
		// check if partitioning survives
		ArrayList<Integer> newPartitionedFields = null;
		PartitionProperty newPartitioning = PartitionProperty.NONE;
		Ordering newOrdering = null;
		if (partitionedFields != null) {
			for (Integer index : partitionedFields) {
				if (node.isFieldKept(input, index) == true) {
					if (newPartitionedFields == null) {
						newPartitioning = this.partitioning;
						newPartitionedFields = new ArrayList<Integer>();
					}
					newPartitionedFields.add(index);
				}
			}
		}

		// check, whether the global order is preserved
		if (ordering != null) {
			boolean orderingPreserved = true;
			ArrayList<Integer> involvedIndexes = ordering.getInvolvedFields();
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

		if (newPartitioning == PartitionProperty.NONE && newOrdering == null) {
			return null;
		} else {
			FieldList partitionFields = new FieldList();
			partitionedFields.addAll(newPartitionedFields);
			return new GlobalProperties(newPartitioning, newOrdering, partitionFields);
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
	public boolean isMetBy(GlobalProperties other) {
		if (this.partitioning != PartitionProperty.NONE) {
			if (this.partitioning == PartitionProperty.ANY) {
				if (other.partitioning == PartitionProperty.NONE) {
					return false;
				}
			} else if (other.partitioning != this.partitioning) {
				return false;
			}
		}

		FieldList otherPartitionedFields = other.getPartitionedFields();
		if (this.partitionedFields != null) {
			if (other.partitionedFields == null) {
				return false;
			}
			if(!otherPartitionedFields.containsAll(this.partitionedFields)) {
				return false;
			}
		}

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
		result = prime * result + ((partitioning == null) ? 0 : partitioning.hashCode());
		result = prime * result + ((partitionedFields == null) ? 0 : partitionedFields.hashCode());
		result = prime * result + ((ordering == null) ? 0 : ordering.hashCode());
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

		GlobalProperties other = (GlobalProperties) obj;
		if ((ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering())))
			&& partitioning == other.getPartitioning() && partitionedFields != null
			&& partitionedFields.equals(other.getPartitionedFields())) {
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
		return "GlobalProperties [partitioning=" + partitioning + " on fields=" + partitionedFields + ", ordering="
			+ ordering // + ", keyUnique=" + keyUnique
			+ "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public GlobalProperties clone() throws CloneNotSupportedException {
		GlobalProperties newProps = (GlobalProperties) super.clone();
		if (this.ordering != null) {
			newProps.ordering = this.ordering.clone();
		}

		return newProps;
	}

	/**
	 * Convenience method to create copies without the cloning exception.
	 * 
	 * @return A perfect deep copy of this object.
	 */
	public final GlobalProperties createCopy() {
		try {
			return this.clone();
		} catch (CloneNotSupportedException cnse) {
			// should never happen, but propagate just in case
			throw new RuntimeException(cnse);
		}
	}
}
