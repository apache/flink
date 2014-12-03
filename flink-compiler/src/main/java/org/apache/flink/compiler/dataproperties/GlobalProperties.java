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

package org.apache.flink.compiler.dataproperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.dag.OptimizerNode;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.util.Utils;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * This class represents global properties of the data at a certain point in the plan.
 * Global properties are properties that describe data across different partitions.
 * <p>
 * Currently, the properties are the following: A partitioning type (ANY, HASH, RANGE), and EITHER an ordering (for range partitioning)
 * or an FieldSet with the hash partitioning columns.
 */
public class GlobalProperties implements Cloneable {
	
	private PartitioningProperty partitioning;	// the type partitioning
	
	private FieldList partitioningFields;		// the fields which are partitioned
	
	private Ordering ordering;					// order of the partitioned fields, if it is an ordered (range) range partitioning
	
	private Set<FieldSet> uniqueFieldCombinations;
	
	private Partitioner<?> customPartitioner;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the global properties with no partitioning.
	 */
	public GlobalProperties() {
		this.partitioning = PartitioningProperty.RANDOM;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the partitioning property for the global properties.
	 * 
	 * @param partitionedFields 
	 */
	public void setHashPartitioned(FieldList partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.HASH_PARTITIONED;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	

	public void setRangePartitioned(Ordering ordering) {
		if (ordering == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.RANGE_PARTITIONED;
		this.ordering = ordering;
		this.partitioningFields = ordering.getInvolvedIndexes();
	}
	
	public void setAnyPartitioning(FieldList partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.ANY_PARTITIONING;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	
	public void setRandomDistribution() {
		this.partitioning = PartitioningProperty.RANDOM;
		this.partitioningFields = null;
		this.ordering = null;
	}
	
	public void setFullyReplicated() {
		this.partitioning = PartitioningProperty.FULL_REPLICATION;
		this.partitioningFields = null;
		this.ordering = null;
	}
	
	public void setForcedRebalanced() {
		this.partitioning = PartitioningProperty.FORCED_REBALANCED;
		this.partitioningFields = null;
		this.ordering = null;
	}
	
	public void setCustomPartitioned(FieldList partitionedFields, Partitioner<?> partitioner) {
		if (partitionedFields == null || partitioner == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.CUSTOM_PARTITIONING;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
		this.customPartitioner = partitioner;
	}
	
	public void addUniqueFieldCombination(FieldSet fields) {
		if (fields == null) {
			return;
		}
		if (this.uniqueFieldCombinations == null) {
			this.uniqueFieldCombinations = new HashSet<FieldSet>();
		}
		this.uniqueFieldCombinations.add(fields);
	}
	
	public void clearUniqueFieldCombinations() {
		if (this.uniqueFieldCombinations != null) {
			this.uniqueFieldCombinations = null;
		}
	}
	
	public Set<FieldSet> getUniqueFieldCombination() {
		return this.uniqueFieldCombinations;
	}
	
	public FieldList getPartitioningFields() {
		return this.partitioningFields;
	}
	
	public Ordering getPartitioningOrdering() {
		return this.ordering;
	}
	
	public PartitioningProperty getPartitioning() {
		return this.partitioning;
	}
	
	public Partitioner<?> getCustomPartitioner() {
		return this.customPartitioner;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean isPartitionedOnFields(FieldSet fields) {
		if (this.partitioning.isPartitionedOnKey() && fields.isValidSubset(this.partitioningFields)) {
			return true;
		} else if (this.uniqueFieldCombinations != null) {
			for (FieldSet set : this.uniqueFieldCombinations) {
				if (fields.isValidSubset(set)) {
					return true;
				}
			}
			return false;
		} else {
			return false;
		}
	}
	
	public boolean matchesOrderedPartitioning(Ordering o) {
		if (this.partitioning == PartitioningProperty.RANGE_PARTITIONED) {
			if (this.ordering.getNumberOfFields() > o.getNumberOfFields()) {
				return false;
			}
			
			for (int i = 0; i < this.ordering.getNumberOfFields(); i++) {
				if (this.ordering.getFieldNumber(i) != o.getFieldNumber(i)) {
					return false;
				}
				
				// if this one request no order, everything is good
				final Order oo = o.getOrder(i);
				final Order to = this.ordering.getOrder(i);
				if (oo != Order.NONE) {
					if (oo == Order.ANY) {
						// if any order is requested, any not NONE order is good
						if (to == Order.NONE) {
							return false;
						}
					} else if (oo != to) {
						// the orders must be equal
						return false;
					}
				}
			}
			return true;
		} else {
			return false;
		}
	}
	
	public boolean isFullyReplicated() {
		return this.partitioning == PartitioningProperty.FULL_REPLICATION;
	}

	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return partitioning == PartitioningProperty.RANDOM;
	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitioning = PartitioningProperty.RANDOM;
		this.ordering = null;
		this.partitioningFields = null;
	}

	/**
	 * Filters these properties by what can be preserved through the given output contract.
	 * 
	 * @param node The optimizer node.
	 * @param input The input of the node to filter against.
	 * @return The adjusted global properties.
	 */
	public GlobalProperties filterByNodesConstantSet(OptimizerNode node, int input) {
		// check if partitioning survives
		if (this.ordering != null) {
			for (int col : this.ordering.getInvolvedIndexes()) {
				if (!node.isFieldConstant(input, col)) {
					return new GlobalProperties();
				}
			}
		}
		if (this.partitioningFields != null) {
			for (int colIndex : this.partitioningFields) {
				if (!node.isFieldConstant(input, colIndex)) {
					return new GlobalProperties();
				}
			}
		}
		if (this.uniqueFieldCombinations != null) {
			HashSet<FieldSet> newSet = new HashSet<FieldSet>();
			newSet.addAll(this.uniqueFieldCombinations);
			
			for (Iterator<FieldSet> combos = newSet.iterator(); combos.hasNext(); ){
				FieldSet current = combos.next();
				for (Integer field : current) {
					if (!node.isFieldConstant(input, field)) {
						combos.remove();
						break;
					}
				}
			}
			
			if (newSet.size() != this.uniqueFieldCombinations.size()) {
				GlobalProperties gp = clone();
				gp.uniqueFieldCombinations = newSet.isEmpty() ? null : newSet;
				return gp;
			}
		}
		
		if (this.partitioning == PartitioningProperty.FULL_REPLICATION) {
			return new GlobalProperties();
		}
		
		return this;
	}
	
	public void parameterizeChannel(Channel channel, boolean globalDopChange) {
		switch (this.partitioning) {
			case RANDOM:
				channel.setShipStrategy(globalDopChange ? ShipStrategyType.PARTITION_RANDOM : ShipStrategyType.FORWARD);
				break;
			case FULL_REPLICATION:
				channel.setShipStrategy(ShipStrategyType.BROADCAST);
				break;
			case ANY_PARTITIONING:
			case HASH_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_HASH, Utils.createOrderedFromSet(this.partitioningFields));
				break;
			case RANGE_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_RANGE, this.ordering.getInvolvedIndexes(), this.ordering.getFieldSortDirections());
				break;
			case FORCED_REBALANCED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_RANDOM);
				break;
			case CUSTOM_PARTITIONING:
				channel.setShipStrategy(ShipStrategyType.PARTITION_CUSTOM, this.partitioningFields, this.customPartitioner);
				break;
			default:
				throw new CompilerException("Unsupported partitioning strategy");
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((partitioning == null) ? 0 : partitioning.ordinal());
		result = prime * result + ((partitioningFields == null) ? 0 : partitioningFields.hashCode());
		result = prime * result + ((ordering == null) ? 0 : ordering.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof GlobalProperties) {
			final GlobalProperties other = (GlobalProperties) obj;
			return (this.partitioning == other.partitioning)
				&& (this.ordering == other.ordering || (this.ordering != null && this.ordering.equals(other.ordering)))
				&& (this.partitioningFields == other.partitioningFields || 
							(this.partitioningFields != null && this.partitioningFields.equals(other.partitioningFields)))
				&& (this.uniqueFieldCombinations == other.uniqueFieldCombinations || 
							(this.uniqueFieldCombinations != null && this.uniqueFieldCombinations.equals(other.uniqueFieldCombinations)));
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		final StringBuilder bld = new StringBuilder(
			"GlobalProperties [partitioning=" + partitioning + 
			(this.partitioningFields == null ? "" : ", on fields " + this.partitioningFields) + 
			(this.ordering == null ? "" : ", with ordering " + this.ordering));
		
		if (this.uniqueFieldCombinations == null) {
			bld.append(']');
		} else {
			bld.append(" - Unique field groups: ");
			bld.append(this.uniqueFieldCombinations);
			bld.append(']');
		}
		return bld.toString();
	}

	@Override
	public GlobalProperties clone() {
		final GlobalProperties newProps = new GlobalProperties();
		newProps.partitioning = this.partitioning;
		newProps.partitioningFields = this.partitioningFields;
		newProps.ordering = this.ordering;
		newProps.customPartitioner = this.customPartitioner;
		newProps.uniqueFieldCombinations = this.uniqueFieldCombinations == null ? null : new HashSet<FieldSet>(this.uniqueFieldCombinations);
		return newProps;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final GlobalProperties combine(GlobalProperties gp1, GlobalProperties gp2) {
		if (gp1.isFullyReplicated()) {
			if (gp2.isFullyReplicated()) {
				return new GlobalProperties();
			} else {
				return gp2;
			}
		} else if (gp2.isFullyReplicated()) {
			return gp1;
		} else if (gp1.ordering != null) {
			return gp1; 
		} else if (gp2.ordering != null) {
			return gp2;
		} else if (gp1.partitioningFields != null) {
			return gp1;
		} else if (gp2.partitioningFields != null) {
			return gp2;
		} else if (gp1.uniqueFieldCombinations != null) {
			return gp1;
		} else if (gp2.uniqueFieldCombinations != null) {
			return gp2;
		} else if (gp1.getPartitioning().isPartitioned()) {
			return gp1;
		} else if (gp2.getPartitioning().isPartitioned()) {
			return gp2;
		} else {
			return gp1;
		}
	}
}
