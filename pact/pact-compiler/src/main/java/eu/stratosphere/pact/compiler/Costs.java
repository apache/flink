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

/**
 * Simple class to represent the costs of an operation.
 * The costs may frequently be unknown, which is represented by a -1 as a value for the unknown
 * components of the cost.
 * <p>
 * If an unknown cost is added with a known cost, the result is unknown. If an unknown cost is compared with a known
 * cost, it is always larger.
 */
public class Costs implements Comparable<Costs>, Cloneable {

	private long networkCost; // network cost, in transferred bytes

	private long secondaryStorageCost; // bytes to be written and read, in bytes

	/**
	 * Default constructor. Initialized the costs to "unknown" (-1).
	 */
	public Costs() {
		this.networkCost = -1;
		this.secondaryStorageCost = -1;
	}

	/**
	 * Creates a new costs object using the given values for the network and storage cost.
	 * 
	 * @param networkCost
	 *        The network cost, in bytes to be transferred.
	 * @param secondaryStorageCost
	 *        The cost for secondary storage, in bytes to be written and read.
	 */
	public Costs(long networkCost, long secondaryStorageCost) {
		this.networkCost = networkCost;
		this.secondaryStorageCost = secondaryStorageCost;
	}

	/**
	 * Gets the network cost.
	 * 
	 * @return The network cost, in bytes to be transferred.
	 */
	public long getNetworkCost() {
		return networkCost;
	}

	/**
	 * Sets the network cost for this Costs object.
	 * 
	 * @param bytes
	 *        The network cost to set, in bytes to be transferred.
	 */
	public void setNetworkCost(long bytes) {
		this.networkCost = bytes;
	}

	/**
	 * Gets the costs for secondary storage.
	 * 
	 * @return The secondary storage cost, in bytes to be written and read.
	 */
	public long getSecondaryStorageCost() {
		return secondaryStorageCost;
	}

	/**
	 * Sets the costs for secondary storage for this Costs object.
	 * 
	 * @param bytes
	 *        The secondary storage cost to set, in bytes to be written and read.
	 */
	public void setSecondaryStorageCost(long bytes) {
		this.secondaryStorageCost = bytes;
	}

	/**
	 * Adds the given costs to these costs. If for one of the different cost components (network, secondary storage),
	 * the costs are unknown, the resulting costs will be unknown.
	 * 
	 * @param other The costs to add.
	 */
	public void addCosts(Costs other) {
		if (this.secondaryStorageCost == -1 || other.secondaryStorageCost == -1) {
			this.secondaryStorageCost = -1;
		} else {
			this.secondaryStorageCost += other.secondaryStorageCost;
		}

		if (this.networkCost == -1 || other.networkCost == -1) {
			this.networkCost = -1;
		} else {
			this.networkCost += other.networkCost;
		}
	}
	
	/**
	 * Subtracts the given costs from these costs. If the given costs are unknown, then these costs are remain unchanged.
	 *  
	 * @param other The costs to subtract.
	 */
	public void subtractCosts(Costs other)
	{
		if (this.secondaryStorageCost != -1 && other.secondaryStorageCost != -1) {
			this.secondaryStorageCost -= other.secondaryStorageCost;
			if (this.secondaryStorageCost < 0) {
				this.secondaryStorageCost = -1;
			}
		}
		
		if (this.networkCost != -1 && other.networkCost != -1) {
			this.networkCost -= other.networkCost;
			if (this.networkCost < 0) {
				this.networkCost = -1;
			}
		}
	}

	/**
	 * Checks, if the given other costs are by more than the given delta larger that these costs.
	 * As in comparisons, the network cost is weighted more than the secondary storage cost.
	 * <p>
	 * If during a comparison, any value is undefined (-1), the result of the comparison is false.
	 * 
	 * @param other
	 *        The costs for which to check whether they are too large.
	 * @param delta
	 *        The delta costs.
	 * @return True, if the given costs are by more than a delta larger. False, if not or if unknown.
	 */
	public boolean isOtherMoreThanDeltaAbove(Costs other, Costs delta) {
		if (this.networkCost == -1 || other.networkCost == -1 || delta.networkCost == -1) {
			return false;
		} else if (other.networkCost > this.networkCost + delta.networkCost) {
			return true;
		} else if (other.networkCost == this.networkCost + delta.networkCost) {
			// net costs match, so determine by secondary storage cost
			if (this.secondaryStorageCost == -1 || other.secondaryStorageCost == -1 || delta.secondaryStorageCost == -1) {
				return false;
			} else if (other.secondaryStorageCost > this.secondaryStorageCost + delta.secondaryStorageCost) {
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	// ------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Costs o) {
		if (this.networkCost != -1 && (this.networkCost < o.networkCost || o.networkCost == -1)) {
			return -1;
		} else if (o.networkCost != -1 && (this.networkCost > o.networkCost || this.networkCost == -1)) {
			return 1;
		} else if (this.networkCost == -1 && o.networkCost == -1) {
			// if both have unknown network costs, they are equal
			return 0;
		} else if (this.secondaryStorageCost != -1
			&& (this.secondaryStorageCost < o.secondaryStorageCost || o.secondaryStorageCost == -1)) {
			return -1;
		} else if (o.secondaryStorageCost != -1
			&& (this.secondaryStorageCost > o.secondaryStorageCost || this.secondaryStorageCost == -1)) {
			return 1;
		} else {
			return 0;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (networkCost ^ (networkCost >>> 32));
		result = prime * result + (int) (secondaryStorageCost ^ (secondaryStorageCost >>> 32));
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Costs other = (Costs) obj;
		if (networkCost != other.networkCost)
			return false;
		if (secondaryStorageCost != other.secondaryStorageCost)
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Costs [networkCost=" + networkCost + ", secondaryStorageCost=" + secondaryStorageCost + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Costs clone() {
		return new Costs(this.networkCost, this.secondaryStorageCost);
	}

	/**
	 * Convenience method to create copies without potential exceptions.
	 * 
	 * @return A perfect copy of this object.
	 */
	public final Costs createCopy() {
		return clone();
	}
}
