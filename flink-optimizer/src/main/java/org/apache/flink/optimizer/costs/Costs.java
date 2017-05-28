/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.optimizer.costs;

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

	public static final double UNKNOWN = -1;
	
	private double networkCost;				// network cost, in transferred bytes

	private double diskCost;		// bytes to be written and read, in bytes
	
	private double cpuCost;					// CPU costs
	
	private double heuristicNetworkCost;
	
	private double heuristicDiskCost;
	
	private double heuristicCpuCost;
	
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
	 * @param diskCost The cost for disk, in bytes to be written and read.
	 */
	public Costs(double networkCost, double diskCost) {
		setNetworkCost(networkCost);
		setDiskCost(diskCost);
	}
	
	/**
	 * Creates a new costs object using the given values for the network and storage cost.
	 * 
	 * @param networkCost The network cost, in bytes to be transferred.
	 * @param diskCost The cost for disk, in bytes to be written and read.
	 * @param cpuCost The cost for CPU operations.
	 */
	public Costs(double networkCost, double diskCost, double cpuCost) {
		setNetworkCost(networkCost);
		setDiskCost(diskCost);
		setCpuCost(cpuCost);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the network cost.
	 * 
	 * @return The network cost, in bytes to be transferred.
	 */
	public double getNetworkCost() {
		return networkCost;
	}

	/**
	 * Sets the network cost for this Costs object.
	 * 
	 * @param bytes
	 *        The network cost to set, in bytes to be transferred.
	 */
	public void setNetworkCost(double bytes) {
		if (bytes == UNKNOWN || bytes >= 0) {
			this.networkCost = bytes;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Adds the costs for network to the current network costs
	 * for this Costs object.
	 * 
	 * @param bytes The network cost to add, in bytes to be transferred.
	 */
	public void addNetworkCost(double bytes) {
		this.networkCost = (this.networkCost < 0 || bytes < 0) ? UNKNOWN : this.networkCost + bytes;
	}

	/**
	 * Gets the costs for disk.
	 * 
	 * @return The disk cost, in bytes to be written and read.
	 */
	public double getDiskCost() {
		return diskCost;
	}

	/**
	 * Sets the costs for disk for this Costs object.
	 * 
	 * @param bytes The disk cost to set, in bytes to be written and read.
	 */
	public void setDiskCost(double bytes) {
		if (bytes == UNKNOWN || bytes >= 0) {
			this.diskCost = bytes;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Adds the costs for disk to the current disk costs
	 * for this Costs object.
	 * 
	 * @param bytes The disk cost to add, in bytes to be written and read.
	 */
	public void addDiskCost(double bytes) {
		this.diskCost = 
			(this.diskCost < 0 || bytes < 0) ? UNKNOWN : this.diskCost + bytes;
	}
	
	/**
	 * Gets the cost for the CPU.
	 * 
	 * @return The CPU Cost.
	 */
	public double getCpuCost() {
		return this.cpuCost;
	}

	/**
	 * Sets the cost for the CPU.
	 * 
	 * @param cost The CPU Cost.
	 */
	public void setCpuCost(double cost) {
		if (cost == UNKNOWN || cost >= 0) {
			this.cpuCost = cost;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Adds the given CPU cost to the current CPU cost for this Costs object.
	 * 
	 * @param cost The CPU cost to add.
	 */
	public void addCpuCost(double cost) {
		this.cpuCost = 
			(this.cpuCost < 0 || cost < 0) ? UNKNOWN : this.cpuCost + cost;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the heuristic network cost.
	 * 
	 * @return The heuristic network cost, in bytes to be transferred.
	 */
	public double getHeuristicNetworkCost() {
		return this.heuristicNetworkCost;
	}

	/**
	 * Sets the heuristic network cost for this Costs object.
	 * 
	 * @param cost The heuristic network cost to set, in bytes to be transferred.
	 */
	public void setHeuristicNetworkCost(double cost) {
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
	public void addHeuristicNetworkCost(double cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicNetworkCost += cost;
		// check for overflow
		if (this.heuristicNetworkCost < 0) {
			this.heuristicNetworkCost = Double.MAX_VALUE;
		}
	}

	/**
	 * Gets the heuristic costs for disk.
	 * 
	 * @return The heuristic disk cost.
	 */
	public double getHeuristicDiskCost() {
		return this.heuristicDiskCost;
	}

	/**
	 * Sets the heuristic costs for disk for this Costs object.
	 * 
	 * @param cost The heuristic disk cost to set.
	 */
	public void setHeuristicDiskCost(double cost) {
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
	public void addHeuristicDiskCost(double cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicDiskCost += cost;
		// check for overflow
		if (this.heuristicDiskCost < 0) {
			this.heuristicDiskCost = Double.MAX_VALUE;
		}
	}
	
	/**
	 * Gets the heuristic cost for the CPU.
	 * 
	 * @return The heuristic CPU Cost.
	 */
	public double getHeuristicCpuCost() {
		return this.heuristicCpuCost;
	}

	/**
	 * Sets the heuristic cost for the CPU.
	 * 
	 * @param cost The heuristic CPU Cost.
	 */
	public void setHeuristicCpuCost(double cost) {
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
	public void addHeuristicCpuCost(double cost) {
		if (cost <= 0) {
			throw new IllegalArgumentException("Heuristic costs must be positive.");
		}
		this.heuristicCpuCost += cost;
		// check for overflow
		if (this.heuristicCpuCost < 0) {
			this.heuristicCpuCost = Double.MAX_VALUE;
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
		if (this.networkCost == UNKNOWN || other.networkCost == UNKNOWN) {
			this.networkCost = UNKNOWN;
		} else {
			this.networkCost += other.networkCost;
		}
		
		if (this.diskCost == UNKNOWN || other.diskCost == UNKNOWN) {
			this.diskCost = UNKNOWN;
		} else {
			this.diskCost += other.diskCost;
		}
		
		if (this.cpuCost == UNKNOWN || other.cpuCost == UNKNOWN) {
			this.cpuCost = UNKNOWN;
		} else {
			this.cpuCost += other.cpuCost;
		}
		
		// ---------- heuristic costs ----------
		
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
		if (this.networkCost != UNKNOWN && other.networkCost != UNKNOWN) {
			this.networkCost -= other.networkCost;
			if (this.networkCost < 0) {
				throw new IllegalArgumentException("Cannot subtract more cost then there is.");
			}
		}
		if (this.diskCost != UNKNOWN && other.diskCost != UNKNOWN) {
			this.diskCost -= other.diskCost;
			if (this.diskCost < 0) {
				throw new IllegalArgumentException("Cannot subtract more cost then there is.");
			}
		}
		if (this.cpuCost != UNKNOWN && other.cpuCost != UNKNOWN) {
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
	
	public void multiplyWith(int factor) {
		this.networkCost = this.networkCost < 0 ? -1 : this.networkCost * factor;
		this.diskCost = this.diskCost < 0 ? -1 : this.diskCost * factor;
		this.cpuCost = this.cpuCost < 0 ? -1 : this.cpuCost * factor;
		this.heuristicNetworkCost = this.heuristicNetworkCost < 0 ? -1 : this.heuristicNetworkCost * factor;
		this.heuristicDiskCost = this.heuristicDiskCost < 0 ? -1 : this.heuristicDiskCost * factor;
		this.heuristicCpuCost = this.heuristicCpuCost < 0 ? -1 : this.heuristicCpuCost * factor;
	}

	public void divideBy(int factor) {
		this.networkCost = this.networkCost < 0 ? -1 : this.networkCost / factor;
		this.diskCost = this.diskCost < 0 ? -1 : this.diskCost / factor;
		this.cpuCost = this.cpuCost < 0 ? -1 : this.cpuCost / factor;
		this.heuristicNetworkCost = this.heuristicNetworkCost < 0 ? -1 : this.heuristicNetworkCost / factor;
		this.heuristicDiskCost = this.heuristicDiskCost < 0 ? -1 : this.heuristicDiskCost / factor;
		this.heuristicCpuCost = this.heuristicCpuCost < 0 ? -1 : this.heuristicCpuCost / factor;
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
		// check the network cost. if we have actual costs on both, use them, otherwise use the heuristic costs.
		if (this.networkCost != UNKNOWN && o.networkCost != UNKNOWN) {
			if (this.networkCost != o.networkCost) {
				return this.networkCost < o.networkCost ? -1 : 1;
			}
		} else if (this.heuristicNetworkCost < o.heuristicNetworkCost) {
			return -1;
		} else if (this.heuristicNetworkCost > o.heuristicNetworkCost) {
			return 1;
		}
		
		// next, check the disk cost. again, if we have actual costs on both, use them, otherwise use the heuristic costs.
		if (this.diskCost != UNKNOWN && o.diskCost != UNKNOWN) {
			if (this.diskCost != o.diskCost) {
				return this.diskCost < o.diskCost ? -1 : 1;
			}
		} else if (this.heuristicDiskCost < o.heuristicDiskCost) {
			return -1;
		} else if (this.heuristicDiskCost > o.heuristicDiskCost) {
			return 1;
		}
		
		// next, check the disk cost. again, if we have actual costs on both, use them, otherwise use the heuristic costs.
		if (this.cpuCost != UNKNOWN && o.cpuCost != UNKNOWN) {
			return this.cpuCost < o.cpuCost ? -1 : this.cpuCost > o.cpuCost ? 1 : 0;
		} else if (this.heuristicCpuCost < o.heuristicCpuCost) {
			return -1;
		} else if (this.heuristicCpuCost > o.heuristicCpuCost) {
			return 1;
		} else {
			return 0;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long cpuCostBits = Double.doubleToLongBits(cpuCost);
		long heuristicCpuCostBits = Double.doubleToLongBits(heuristicCpuCost);
		long heuristicNetworkCostBits = Double.doubleToLongBits(heuristicNetworkCost);
		long heuristicDiskCostBits = Double.doubleToLongBits(heuristicDiskCost);
		long networkCostBits = Double.doubleToLongBits(networkCost);
		long diskCostBits = Double.doubleToLongBits(diskCost);

		result = prime * result + (int) (cpuCostBits ^ (cpuCostBits >>> 32));
		result = prime * result + (int) (heuristicCpuCostBits ^ (heuristicCpuCostBits >>> 32));
		result = prime * result + (int) (heuristicNetworkCostBits ^ (heuristicNetworkCostBits >>> 32));
		result = prime * result + (int) (heuristicDiskCostBits ^ (heuristicDiskCostBits >>> 32));
		result = prime * result + (int) (networkCostBits ^ (networkCostBits >>> 32));
		result = prime * result + (int) (diskCostBits ^ (diskCostBits >>> 32));
		return result;
	}

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

	@Override
	public String toString() {
		return "Costs [networkCost=" + networkCost + ", diskCost=" + diskCost + 
				", cpuCost=" + cpuCost + ", heuristicNetworkCost=" + heuristicNetworkCost + 
				", heuristicDiskCost=" + heuristicDiskCost + ", heuristicCpuCost=" + heuristicCpuCost + "]";
	}

	@Override
	public Costs clone() {
		try {
			return (Costs) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);	// should never happen
		}
	}
}
