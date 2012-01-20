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
 * This class represents global properties of the data. Global properties are properties that
 * describe data across different partitions.
 * NOTE: Currently, this class has a very simple property about the partitioning, namely simply whether
 * the data is partitioned on the key. Later, we might need to replace that by tracking partition maps.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class GlobalProperties implements Cloneable
{
	private FieldSet partitionedFields;
	
	private PartitionProperty partitioning; // the partitioning

	private Ordering ordering; // order across all partitions

//	private boolean keyUnique = false; // flag indicating whether the keys are unique

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
	public GlobalProperties(PartitionProperty partitioning, Ordering ordering, boolean keyUnique) {
		this.partitioning = partitioning;
		this.ordering = ordering;
//		this.keyUnique = keyUnique;
	}

	
	public FieldSet getPartitionedFiels() {
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
	public void setPartitioning(PartitionProperty partitioning, FieldSet partitionedFields) {
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
	 * Checks whether the key is unique.
	 * 
	 * @return The keyUnique property.
	 */
//	public boolean isKeyUnique() {
//		return keyUnique;
//	}

	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return partitioning == PartitionProperty.NONE && ordering == null;
//				&& !keyUnique;
	}

	/**
	 * Sets the flag that indicates whether the key is unique.
	 * 
	 * @param keyUnique
	 *        The keyUnique to set.
	 */
//	public void setKeyUnique(boolean keyUnique) {
//		this.keyUnique = keyUnique;
//	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitionedFields = null;
		this.partitioning = PartitionProperty.NONE;
		this.ordering = null;
//		this.keyUnique = false;
	}

//	/**
//	 * Filters these properties by what can be preserved through the given output contract.
//	 * 
//	 * @param contract
//	 *        The output contract.
//	 * @return True, if any non-default value is preserved, false otherwise.
//	 */
//	public boolean filterByOutputContract(OutputContract contract) {
//		boolean nonTrivial = false;
//
//		// check, if the partitioning survives
//		if (partitioning == PartitionProperty.HASH_PARTITIONED || partitioning == PartitionProperty.RANGE_PARTITIONED
//			|| partitioning == PartitionProperty.ANY) {
//			if (contract == OutputContract.SameKey || contract == OutputContract.SameKeyFirst
//				|| contract == OutputContract.SameKeySecond || contract == OutputContract.SuperKey
//				|| contract == OutputContract.SuperKeyFirst || contract == OutputContract.SuperKeySecond) {
//				nonTrivial = true;
//			} else {
//				partitioning = PartitionProperty.NONE;
//			}
//		}
//
//		// check, whether the global order is preserved
//		if (keyOrder != Order.NONE) {
//			if (contract == OutputContract.SameKey || contract == OutputContract.SameKeyFirst
//				|| contract == OutputContract.SameKeySecond) {
//				nonTrivial = true;
//			} else {
//				keyOrder = Order.NONE;
//			}
//		}
//
//		// check, whether we have key uniqueness
//		nonTrivial |= (keyUnique = contract == OutputContract.UniqueKey);
//
//		return nonTrivial;
//	}
	
	public boolean filterByNodesConstantSet(OptimizerNode node, int input) {
		
		//check if partitioning survives
		if (partitionedFields != null) {
			for (Integer index : partitionedFields.toArray(new Integer[0])) {
				if (node.isFieldKept(input, index) == false) {
					partitionedFields.remove(index);
				}
			}
			
			if (partitionedFields.size() == 0) {
				partitioning = PartitionProperty.NONE;
			}	
		}
		
		// check, whether the global order is preserved
		if (ordering != null) {
			ArrayList<Integer> involvedIndexes = ordering.getInvolvedIndexes();
			for (int i = 0; i < involvedIndexes.size(); i++) {
				if (node.isFieldKept(input, i) == false) {
					ordering = ordering.createNewOrderingUpToIndex(i);
					break;
				}
			}
		}
		
		return partitioning != PartitionProperty.NONE || ordering != null;
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
		
		FieldSet otherPartitionedFields = other.getPartitionedFiels();
		if (this.partitionedFields != null) {
			if (other.partitionedFields == null) {
				return false;
			}
			if (this.partitionedFields.size() > otherPartitionedFields.size()) {
				return false;
			}
			
			for (Integer fieldIndex : this.partitionedFields) {
				if (otherPartitionedFields.contains(fieldIndex) == false) {
					return false;
				}
			}	
		}
		
		return (this.ordering == null || this.ordering.isMetBy(other.getOrdering()));

//		// check the order
//		// if this one request no order, everything is good
//		if (this.keyOrder != Order.NONE) {
//			if (this.keyOrder == Order.ANY) {
//				// if any order is requested, any not NONE order is good
//				if (other.keyOrder == Order.NONE) {
//					return false;
//				}
//			} else if (other.keyOrder != this.keyOrder) {
//				// the orders must be equal
//				return false;
//			}
//		}

//		return this.keyUnique == other.keyUnique;
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
//		result = prime * result + (keyUnique ? 1231 : 1237);

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
				&& partitioning == other.getPartitioning() && partitionedFields.equals(other.getPartitionedFiels())) {
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
		return "GlobalProperties [partitioning=" + partitioning + " on fields=" + partitionedFields + ", ordering=" + ordering //+ ", keyUnique=" + keyUnique
			+ "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public GlobalProperties clone() throws CloneNotSupportedException {
		return (GlobalProperties) super.clone();
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
