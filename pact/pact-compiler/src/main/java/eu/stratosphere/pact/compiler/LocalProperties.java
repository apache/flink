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

import eu.stratosphere.pact.common.contract.Order;

/**
 * This class represents local properties of the data. A local property is a property that exists
 * within the data of a single partition.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class LocalProperties implements Cloneable {
	private Order keyOrder; // order inside a partition

	private boolean keysGrouped = false; // flag indicating whether the keys are grouped

	private boolean keyUnique = false; // flag indicating whether the keys are unique

	/**
	 * Default constructor. Initiates the order to NONE and the uniqueness to false.
	 */
	public LocalProperties() {
		this.keyOrder = Order.NONE;
	}

	/**
	 * Gets the key order.
	 * 
	 * @return The key order.
	 */
	public Order getKeyOrder() {
		return keyOrder;
	}

	/**
	 * Sets the key order for these global properties.
	 * 
	 * @param keyOrder
	 *        The key order to set.
	 */
	public void setKeyOrder(Order keyOrder) {
		this.keyOrder = keyOrder;
	}

	/**
	 * Checks whether the key is unique.
	 * 
	 * @return The keyUnique property.
	 */
	public boolean isKeyUnique() {
		return keyUnique;
	}

	/**
	 * Sets the flag that indicates whether the key is unique.
	 * 
	 * @param keyUnique
	 *        The uniqueness flag to set.
	 */
	public void setKeyUnique(boolean keyUnique) {
		this.keyUnique = keyUnique;
	}

	/**
	 * Checks whether the keys are grouped.
	 * 
	 * @return True, if the keys are grouped, false otherwise.
	 */
	public boolean areKeysGrouped() {
		return this.keysGrouped;
	}

	/**
	 * Sets the flag that indicates whether the keys are grouped.
	 * 
	 * @param keysGrouped
	 *        The keys-grouped flag to set.
	 */
	public void setKeysGrouped(boolean keysGrouped) {
		this.keysGrouped = keysGrouped;
	}

	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return keyOrder == Order.NONE && !keyUnique && !keysGrouped;
	}

	/**
	 * This method resets the local properties to a state where no properties are given.
	 */
	public void reset() {
		this.keyOrder = Order.NONE;
		this.keyUnique = false;
		this.keysGrouped = false;
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
//		// check, whether the local order is preserved
//		if (keyOrder != Order.NONE) {
//			if (contract == OutputContract.SameKey || contract == OutputContract.SameKeyFirst
//				|| contract == OutputContract.SameKeySecond) {
//				nonTrivial = true;
//			} else {
//				keyOrder = Order.NONE;
//			}
//		}
//
//		// check, whether the local key grouping is preserved
//		if (keysGrouped) {
//			if (contract == OutputContract.SameKey || contract == OutputContract.SameKeyFirst
//				|| contract == OutputContract.SameKeySecond) {
//				nonTrivial = true;
//			} else {
//				keysGrouped = false;
//			}
//		}
//
//		// check, whether we have key uniqueness
//		nonTrivial |= (keyUnique = (contract == OutputContract.UniqueKey));
//
//		return nonTrivial;
//	}

	/**
	 * Checks, if this set of properties, as interesting properties, is met by the given
	 * properties.
	 * 
	 * @param other
	 *        The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(LocalProperties other) {
		// check the order
		// if this one request no order, everything is good
		if (this.keyOrder != Order.NONE) {
			if (this.keyOrder == Order.ANY) {
				// if any order is requested, any not NONE order is good
				if (other.keyOrder == Order.NONE) {
					return false;
				}
			} else if (other.keyOrder != this.keyOrder) {
				// the orders must be equal
				return false;
			}
		}

		// check the grouping. if this one requests a grouping, then an
		// order or a grouping are good.
		if (this.keysGrouped && !(other.keysGrouped || other.getKeyOrder().isOrdered())) {
			return false;
		}

		return this.keyUnique == other.keyUnique;
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
		result = prime * result + ((keyOrder == null) ? 0 : keyOrder.hashCode());
		result = prime * result + (keyUnique ? 1231 : 1237);
		result = prime * result + (keysGrouped ? 1231 : 1237);

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
		if (this.keyOrder == other.keyOrder && this.keyUnique == other.keyUnique
			&& this.keysGrouped == other.keysGrouped) {
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
		return "LocalProperties [keyOrder=" + keyOrder + ", keyUnique=" + keyUnique + ", keysGrouped=" + keysGrouped
			+ "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public LocalProperties clone() throws CloneNotSupportedException {
		return (LocalProperties) super.clone();
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
