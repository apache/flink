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
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 * This class represents global properties of the data. Global properties are properties that
 * describe data across different partitions.
 * <p>
 * Currently, the properties are the following: A partitioning type (ANY, HASH, RANGE), and EITHER an ordering (for range partitioning)
 * or an FieldSet with the hash partitioning columns.
 */
public final class GlobalProperties implements Cloneable
{
	private PartitioningProperty partitioning;	// the type partitioning
	
	private FieldSet partitioningFields;		// the fields which are partitioned
	
	private Ordering ordering;					// order of the partitioned fields, if it is an ordered (range) range partitioning
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the global properties with no partitioning.
	 */
	public GlobalProperties() {
		this.partitioning = PartitioningProperty.RANDOM;
	}
	
	/**
	 * @param partitioning2
	 * @param newFields
	 */
	private GlobalProperties(PartitioningProperty partitioning, FieldSet fields) {
		this.partitioning = partitioning;
		this.partitioningFields = fields;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the partitioning property for the global properties.
	 * 
	 * @param partitioning The new partitioning to set.
	 * @param partitionedFields 
	 */
	public void setHashPartitioned(FieldSet partitionedFields) {
		this.partitioning = PartitioningProperty.HASH_PARTITIONED;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	

	public void setRangePartitioned(Ordering ordering) {
		this.partitioning = PartitioningProperty.RANGE_PARTITIONED;
		this.ordering = ordering;
		this.partitioningFields = null;
	}
	
	public void setAnyPartitioning(FieldSet partitionedFields) {
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
	


	/**
	 * Gets the partitioning property.
	 * 
	 * @return The partitioning property.
	 */
	public PartitioningProperty getPartitioning() {
		return partitioning;
	}
	
	/**
	 * Gets the fields on which the data is partitioned.
	 * 
	 * @return The partitioning fields.
	 */
	public FieldSet getPartitionedFields() {
		return this.partitioningFields;
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
	 * @param contract
	 *        The output contract.
	 * @return True, if any non-default value is preserved, false otherwise.
	 */
	public GlobalProperties filterByNodesConstantSet(OptimizerNode node, int input)
	{
		// check if partitioning survives
		if (this.ordering != null) {
			for (int col : this.ordering.getInvolvedIndexes()) {
				if (!node.isFieldConstant(input, col)) {
					return null;
				}
			}
		} else if (this.partitioningFields != null) {
			for (int colIndex : this.partitioningFields) {
				if (!node.isFieldConstant(input, colIndex)) {
					return null;
				}
			}
		}
		return this;
	}

	public GlobalProperties createInterestingGlobalPropertiesTopDownSubset(OptimizerNode node, int input)
	{
		// check, whether the global order is preserved
		if (this.ordering != null) {
			for (int i : this.ordering.getInvolvedIndexes()) {
				if (!node.isFieldConstant(input, i)) {
					return null;
				}
			}
			return this;
		}
		else if (partitioningFields != null) {
			boolean allIn = true;
			boolean atLeasOneIn = false;
			for (int col : this.partitioningFields) {
				boolean res = node.isFieldConstant(input, col);
				allIn &= res;
				atLeasOneIn |= res;
			}
			if (allIn) {
				return this;
			} else if (atLeasOneIn) {
				FieldSet newFields = new FieldSet();
				for (Integer nc : this.partitioningFields) {
					if (node.isFieldConstant(input, nc)) {
						newFields.add(nc);
					}
				}
				return new GlobalProperties(this.partitioning, newFields);
			} else {
				return null;
			}
		} else {
			return this;
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
	public boolean isMetBy(GlobalProperties other)
	{
		if (this.partitioning == PartitioningProperty.FULL_REPLICATION || other.partitioning == PartitioningProperty.FULL_REPLICATION) {
			return other.partitioning == this.partitioning;
		}
		
		if (this.partitioning == PartitioningProperty.RANDOM) {
			return true;
		}
		
		if (this.partitioning == PartitioningProperty.ANY_PARTITIONING) {
			if (other.partitioning == PartitioningProperty.RANDOM) {
				return false;
			}
		} else if (other.partitioning != this.partitioning) {
			return false;
		}

		if (this.ordering != null) {
			if (other.ordering == null)
				throw new CompilerException("BUG: Equal partitioning property, ordering not equally set.");
			if (this.ordering.getInvolvedIndexes().isValidSubset(other.ordering.getInvolvedIndexes())) {
				// check if the directions match
				for (int i = 0; i < other.ordering.getNumberOfFields(); i++) {
					Order to = this.ordering.getOrder(i);
					Order oo = other.ordering.getOrder(i);
					if (to == Order.NONE)
						continue;
					if (oo == Order.NONE)
						return false;
					if (to != Order.ANY && to != oo)
						return false;
				}
				return true;
			} else {
				return false;
			}
		} else if (this.partitioningFields != null) {
			return this.partitioningFields.isValidSubset(other.partitioningFields);
		} else {
			throw new RuntimeException("Found a partitioning property, but no fields.");
		}
	}
	
	/**
	 * Parameterizes the ship strategy fields of a channel such that the channel produces the desired global properties.
	 * 
	 * @param channel The channel to parameterize.
	 */
	public void parameterizeChannel(Channel channel)
	{
		if (this.partitioning == null || this.partitioning == PartitioningProperty.RANDOM) {
			channel.setShipStrategy(ShipStrategyType.FORWARD);
		} else switch (this.partitioning) {
			case FULL_REPLICATION:
				channel.setShipStrategy(ShipStrategyType.BROADCAST);
			case ANY_PARTITIONING:
			case HASH_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_HASH, Utils.createOrderedFromSet(this.partitioningFields));
				break;
			case RANGE_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_RANGE, this.ordering.getInvolvedIndexes(), this.ordering.getFieldSortDirections());
				break;
			default:
				throw new CompilerException();
		}
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
		result = prime * result + ((partitioningFields == null) ? 0 : partitioningFields.hashCode());
		result = prime * result + ((ordering == null) ? 0 : ordering.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof GlobalProperties) {
			GlobalProperties other = (GlobalProperties) obj;
			return (ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering())))
					&& (partitioning == other.getPartitioning())
					&& (partitioningFields == other.partitioningFields || 
							(partitioningFields != null && partitioningFields.equals(other.getPartitionedFields())));
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
		return "GlobalProperties [partitioning=" + partitioning + 
			(this.partitioningFields == null ? "" : ", on fields " + this.partitioningFields) + 
			(this.ordering == null ? "" : ", with ordering " + this.ordering) + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public GlobalProperties clone() {
		try {
			return (GlobalProperties) super.clone();
		} catch (CloneNotSupportedException cnse) {
			// should never happen, but propagate just in case
			throw new RuntimeException(cnse);
		}
	}
}
