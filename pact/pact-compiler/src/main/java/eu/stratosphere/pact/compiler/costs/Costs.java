/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.costs;

/**
 * Simple class to represent the costs of an operation. The costs are currently tracking, network, I/O and CPU costs.
 * 
 * Costs are composed of two parts of cost contributors:
 * <ol>
 *   <li>Quantifiable costs. Those costs are used when estimates are available and track a quantifiable
 *       measure, such as the number of bytes for network or I/O</li>
 *   <li>Heuristic costs. Those costs are used when no estimates are available. They can be used to track that
 *       an operator used a special operation which is heuristically considered more expensive than another
 *       operation.</li>
 * </ol>
 * <p>
 * The quantifiable costs may frequently be unknown, which is represented by a {@code -1} as a value for the unknown
 * components of the cost. In that case, all operations' costs are unknown and hence it is not decidable which
 * operation to favor during pruning. In that case, the heuristic costs should contain a value to make sure that
 * operators with different strategies are comparable, even in the absence of estimates. The heuristic
 * costs are hence the system's mechanism of realizing pruning heuristics that favor some operations over others.
 */
public class Costs implements Comparable<Costs>, Cloneable {

	private long networkCost;				// network cost, in transferred bytes

	private long diskCost;		// bytes to be written and read, in bytes
	
	private long cpuCost;					// CPU costs
	
	private long heuristicNetworkCost;
	
	private long heuristicDiskCost;
	
	private long heuristicCpuCost;
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Default constructor. Initializes all costs to 0;
	 */
	public Costs() {
	}

	/**
	 * Creates a new costs object using the given values for the network and storage cost.
	 * 
	 * @param networkCost The network cost, in bytes to be transferred.
	 * @param secondaryStorageCost The cost for disk, in bytes to be written and read.
	 */
	public Costs(long networkCost, long secondaryStorageCost) {
		this.networkCost = networkCost;
		this.diskCost = secondaryStorageCost;
	}
	
	/**
	 * Creates a new costs object using the given values for the network and storage cost.
	 * 
	 * @param networkCost The network cost, in bytes to be transferred.
	 * @param secondaryStorageCost The cost for disk, in bytes to be written and read.
	 * @param cpuCost The cost for CPU operations.
	 */
	public Costs(long networkCost, long secondaryStorageCost, long cpuCost) {
		this.networkCost = networkCost;
		this.diskCost = secondaryStorageCost;
		this.cpuCost = cpuCost;
	}

	// --------------------------------------------------------------------------------------------
	
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
	 * Adds the costs for network to the current network costs
	 * for this Costs object.
	 * 
	 * @param bytes The network cost to add, in bytes to be transferred.
	 */
	public void addNetworkCost(long bytes) {
		this.networkCost = (this.networkCost < 0 || bytes < 0) ? -1 : this.networkCost + bytes;
	}

	/**
	 * Gets the costs for disk.
	 * 
	 * @return The disk cost, in bytes to be written and read.
	 */
	public long getSecondaryStorageCost() {
		return diskCost;
	}

	/**
	 * Sets the costs for disk for this Costs object.
	 * 
	 * @param bytes The disk cost to set, in bytes to be written and read.
	 */
	public void setSecondaryStorageCost(long bytes) {
		this.diskCost = bytes;
	}
	
	/**
	 * Adds the costs for disk to the current disk costs
	 * for this Costs object.
	 * 
	 * @param bytes The disk cost to add, in bytes to be written and read.
	 */
	public void addSecondaryStorageCost(long bytes) {
		this.diskCost = 
			(this.diskCost < 0 || bytes < 0) ? -1 : this.diskCost + bytes;
	}
	
	/**
	 * Gets the cost for the CPU.
	 * 
	 * @return The CPU Cost.
	 */
	public long getCpuCost() {
		return this.cpuCost;
	}

	/**
	 * Sets the cost for the CPU.
	 * 
	 * @param cost The CPU Cost.
	 */
	public void setCpuCost(long cost) {
		this.cpuCost = cost;
	}
	
	/**
	 * Adds the given CPU cost to the current CPU cost for this Costs object.
	 * 
	 * @param cost The CPU cost to add.
	 */
	public void addCpuCost(long cost) {
		this.cpuCost = 
			(this.cpuCost < 0 || cost < 0) ? -1 : this.cpuCost + cost;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the heuristic network cost.
	 * 
	 * @return The heuristic network cost, in bytes to be transferred.
	 */
	public long getHeuristicNetworkCost() {
		return this.heuristicNetworkCost;
	}

	/**
	 * Sets the heuristic network cost for this Costs object.
	 * 
	 * @param cost The heuristic network cost to set, in bytes to be transferred.
	 */
	public void setHeuristicNetworkCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicNetworkCost = cost;
	}
	
	/**
	 * Adds the heuristic costs for network to the current heuristic network costs
	 * for this Costs object.
	 * 
	 * @param cost The heuristic network cost to add.
	 */
	public void addHeuristicNetworkCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicNetworkCost += cost;
		// check for overflow
		if (this.heuristicNetworkCost < 0) {
			this.heuristicNetworkCost = Long.MAX_VALUE;
		}
	}

	/**
	 * Gets the heuristic costs for disk.
	 * 
	 * @return The heuristic disk cost.
	 */
	public long getHeuristicDiskCost() {
		return this.heuristicDiskCost;
	}

	/**
	 * Sets the heuristic costs for disk for this Costs object.
	 * 
	 * @param cost The heuristic disk cost to set.
	 */
	public void setHeuristicDiskCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicDiskCost = cost;
	}
	
	/**
	 * Adds the heuristic costs for disk to the current heuristic disk costs
	 * for this Costs object.
	 * 
	 * @param cost The heuristic disk cost to add.
	 */
	public void addHeuristicDiskCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicDiskCost += cost;
		// check for overflow
		if (this.heuristicDiskCost < 0) {
			this.heuristicDiskCost = Long.MAX_VALUE;
		}
	}
	
	/**
	 * Gets the heuristic cost for the CPU.
	 * 
	 * @return The heuristic CPU Cost.
	 */
	public long getHeuristicCpuCost() {
		return this.heuristicCpuCost;
	}

	/**
	 * Sets the heuristic cost for the CPU.
	 * 
	 * @param cost The heuristic CPU Cost.
	 */
	public void setHeuristicCpuCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicCpuCost = cost;
	}
	
	/**
	 * Adds the given heuristic CPU cost to the current heuristic CPU cost for this Costs object.
	 * 
	 * @param cost The heuristic CPU cost to add.
	 */
	public void addHeuristicCpuCost(long cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicCpuCost += cost;
		// check for overflow
		if (this.heuristicCpuCost < 0) {
			this.heuristicCpuCost = Long.MAX_VALUE;
		}
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Adds the given costs to these costs. If for one of the different cost components (network, disk),
	 * the costs are unknown, the resulting costs will be unknown.
	 * 
	 * @param other The costs to add.
	 */
	public void addCosts(Costs other) {
		// ---------- quantifiable costs ----------
		if (this.networkCost == -1 || other.networkCost == -1) {
			this.networkCost = -1;
		} else {
			this.networkCost += other.networkCost;
		}
		
		if (this.diskCost == -1 || other.diskCost == -1) {
			this.diskCost = -1;
		} else {
			this.diskCost += other.diskCost;
		}
		
		if (this.cpuCost == -1 || other.cpuCost == -1) {
			this.cpuCost = -1;
		} else {
			this.cpuCost += other.cpuCost;
		}
		
		// ---------- relative costs ----------
		
		this.heuristicNetworkCost += other.heuristicNetworkCost;
		this.heuristicDiskCost += other.heuristicDiskCost;
		this.heuristicCpuCost += other.heuristicCpuCost;
	}
	
	/**
	 * Subtracts the given costs from these costs. If the given costs are unknown, then these costs are remain unchanged.
	 *  
	 * @param other The costs to subtract.
	 */
	public void subtractCosts(Costs other) {
		if (this.networkCost != -1 && other.networkCost != -1) {
			this.networkCost -= other.networkCost;
			if (this.networkCost < 0) {
				throw new IllegalArgumentException("Cannot subtract more cost then there is.");
			}
		}
		if (this.diskCost != -1 && other.diskCost != -1) {
			this.diskCost -= other.diskCost;
			if (this.diskCost < 0) {
				throw new IllegalArgumentException("Cannot subtract more cost then there is.");
			}
		}
		if (this.cpuCost != -1 && other.cpuCost != -1) {
			this.cpuCost -= other.cpuCost;
			if (this.cpuCost < 0) {
				throw new IllegalArgumentException("Cannot subtract more cost then there is.");
			}
		}
		
		// ---------- relative costs ----------
		
		this.heuristicNetworkCost -= other.heuristicNetworkCost;
		if (this.heuristicNetworkCost < 0) {
			throw new IllegalArgumentException("Cannot subtract more cost then there is.");
		}
		this.heuristicDiskCost -= other.heuristicDiskCost;
		if (this.heuristicDiskCost < 0) {
			throw new IllegalArgumentException("Cannot subtract more cost then there is.");
		}
		this.heuristicCpuCost -= other.heuristicCpuCost;
		if (this.heuristicCpuCost < 0) {
			throw new IllegalArgumentException("Cannot subtract more cost then there is.");
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * The order of comparison is: network first, then disk, then CPU. The comparison here happens each time
	 * primarily after the heuristic costs, then after the quantifiable costs.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Costs o) {
		// check the network cost. first heuristic, then quantifiable
		if (this.heuristicNetworkCost < o.heuristicNetworkCost) {
			return -1;
		} else if (this.heuristicNetworkCost > o.heuristicNetworkCost) {
			return 1;
		} else if (this.networkCost != -1 && (this.networkCost < o.networkCost || o.networkCost == -1)) {
			return -1;
		} else if (o.networkCost != -1 && (this.networkCost > o.networkCost || this.networkCost == -1)) {
			return 1;
		} else if (this.networkCost == -1 && o.networkCost == -1) {
			// both have unknown network cost (and equal or no heuristic net cost). treat the costs as equal
			return 0;
		}
		
		// next, check the disk cost. again heuristic before quantifiable
		if (this.heuristicDiskCost < o.heuristicDiskCost) {
			return -1;
		} else if (this.heuristicDiskCost > o.heuristicDiskCost) {
			return 1;
		} else if (this.diskCost != -1 && (this.diskCost < o.diskCost || o.diskCost == -1)) {
			return -1;
		} else if (o.diskCost != -1 && (this.diskCost > o.diskCost || this.diskCost == -1)) {
			return 1;
		} else if (this.diskCost == -1 && o.diskCost == -1) {
			// both have unknown disk cost (and equal or no heuristic net cost). treat the costs as equal
			return 0;
		}
		
		// finally, check the CPU cost. heuristic before quantifiable
		if (this.heuristicCpuCost < o.heuristicCpuCost) {
			return -1;
		} else if (this.heuristicCpuCost > o.heuristicCpuCost) {
			return 1;
		} else if (this.cpuCost != -1 && (this.cpuCost < o.cpuCost || o.cpuCost == -1)) {
			return -1;
		} else if (o.cpuCost != -1 && (this.cpuCost > o.cpuCost || this.cpuCost == -1)) {
			return 1;
		} else {
			return 0;
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (cpuCost ^ (cpuCost >>> 32));
		result = prime * result + (int) (heuristicCpuCost ^ (heuristicCpuCost >>> 32));
		result = prime * result + (int) (heuristicNetworkCost ^ (heuristicNetworkCost >>> 32));
		result = prime * result + (int) (heuristicDiskCost ^ (heuristicDiskCost >>> 32));
		result = prime * result + (int) (networkCost ^ (networkCost >>> 32));
		result = prime * result + (int) (diskCost ^ (diskCost >>> 32));
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj.getClass() == getClass()) {
			final Costs other = (Costs) obj;
			return this.networkCost == other.networkCost &
					this.diskCost == other.diskCost &
					this.cpuCost == other.cpuCost &
					this.heuristicNetworkCost == other.heuristicNetworkCost &
					this.heuristicDiskCost == other.heuristicDiskCost &
					this.heuristicCpuCost == other.heuristicCpuCost;
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
		return "Costs [networkCost=" + networkCost + ", diskCost=" + diskCost + 
				", cpuCost=" + cpuCost + ", heuristicNetworkCost=" + heuristicNetworkCost + 
				", heuristicDiskCost=" + heuristicDiskCost + ", heuristicCpuCost=" + heuristicCpuCost + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Costs clone() {
		try {
			return (Costs) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);	// should never happen
		}
	}
}
