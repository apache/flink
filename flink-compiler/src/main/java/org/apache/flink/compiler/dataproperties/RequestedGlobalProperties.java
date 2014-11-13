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

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.dag.OptimizerNode;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.util.Utils;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * This class represents global properties of the data that an operator is interested in, because it needs those
 * properties for its contract.
 * <p>
 * Currently, the properties are the following: A partitioning type (ANY, HASH, RANGE), and EITHER an ordering (for range partitioning)
 * or an FieldSet with the hash partitioning columns.
 */
public final class RequestedGlobalProperties implements Cloneable {
	
	private PartitioningProperty partitioning;	// the type partitioning
	
	private FieldSet partitioningFields;		// the fields which are partitioned
	
	private Ordering ordering;					// order of the partitioned fields, if it is an ordered (range) range partitioning
	
	private DataDistribution dataDistribution;	// optional data distribution, for a range partitioning
	
	private Partitioner<?> customPartitioner;	// optional, partitioner for custom partitioning
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the global properties with no partitioning.
	 */
	public RequestedGlobalProperties() {
		this.partitioning = PartitioningProperty.RANDOM;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets the partitioning property for the global properties.
	 * 
	 * @param partitionedFields
	 */
	public void setHashPartitioned(FieldSet partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		this.partitioning = PartitioningProperty.HASH_PARTITIONED;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	

	public void setRangePartitioned(Ordering ordering) {
		this.setRangePartitioned(ordering, null);
	}
	
	public void setRangePartitioned(Ordering ordering, DataDistribution dataDistribution) {
		if (ordering == null) {
			throw new NullPointerException();
		}
		this.partitioning = PartitioningProperty.RANGE_PARTITIONED;
		this.ordering = ordering;
		this.partitioningFields = null;
		this.dataDistribution = dataDistribution;
	}
	
	public void setAnyPartitioning(FieldSet partitionedFields) {
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
	
	public void setForceRebalancing() {
		this.partitioning = PartitioningProperty.FORCED_REBALANCED;
		this.partitioningFields = null;
		this.ordering = null;
	}
	
	public void setCustomPartitioned(FieldSet partitionedFields, Partitioner<?> partitioner) {
		if (partitionedFields == null || partitioner == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.CUSTOM_PARTITIONING;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
		this.customPartitioner = partitioner;
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
	 * Gets the data distribution.
	 * 
	 * @return The data distribution.
	 */
	public DataDistribution getDataDistribution() {
		return this.dataDistribution;
	}
	
	/**
	 * Gets the custom partitioner associated with these properties.
	 * 
	 * @return The custom partitioner associated with these properties.
	 */
	public Partitioner<?> getCustomPartitioner() {
		return customPartitioner;
	}

	/**
	 * Checks, if the properties in this object are trivial, i.e. only standard values.
	 */
	public boolean isTrivial() {
		return this.partitioning == null || this.partitioning == PartitioningProperty.RANDOM;
	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitioning = PartitioningProperty.RANDOM;
		this.ordering = null;
		this.partitioningFields = null;
		this.dataDistribution = null;
		this.customPartitioner = null;
	}

	/**
	 * Filters these properties by what can be preserved by the given node when propagated down
	 * to the given input.
	 * 
	 * @param node The node representing the contract.
	 * @param input The index of the input.
	 * @return True, if any non-default value is preserved, false otherwise.
	 */
	public RequestedGlobalProperties filterByNodesConstantSet(OptimizerNode node, int input) {
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
		
		// make sure that certain properties are not pushed down
		final PartitioningProperty partitioning = this.partitioning;
		if (partitioning == PartitioningProperty.FULL_REPLICATION ||
				partitioning == PartitioningProperty.FORCED_REBALANCED ||
				partitioning == PartitioningProperty.CUSTOM_PARTITIONING)
		{
			return null;
		}
		
		return this;
	}

	/**
	 * Checks, if this set of interesting properties, is met by the given
	 * produced properties.
	 * 
	 * @param props The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(GlobalProperties props) {
		if (this.partitioning == PartitioningProperty.FULL_REPLICATION) {
			return props.isFullyReplicated();
		}
		else if (props.isFullyReplicated()) {
			return false;
		}
		else if (this.partitioning == PartitioningProperty.RANDOM) {
			return true;
		}
		else if (this.partitioning == PartitioningProperty.ANY_PARTITIONING) {
			return props.isPartitionedOnFields(this.partitioningFields);
		}
		else if (this.partitioning == PartitioningProperty.HASH_PARTITIONED) {
			return props.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
					props.isPartitionedOnFields(this.partitioningFields);
		}
		else if (this.partitioning == PartitioningProperty.RANGE_PARTITIONED) {
			return props.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
					props.matchesOrderedPartitioning(this.ordering);
		}
		else if (this.partitioning == PartitioningProperty.FORCED_REBALANCED) {
			return props.getPartitioning() == PartitioningProperty.FORCED_REBALANCED;
		}
		else if (this.partitioning == PartitioningProperty.CUSTOM_PARTITIONING) {
			return props.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING &&
					props.isPartitionedOnFields(this.partitioningFields) &&
					props.getCustomPartitioner().equals(this.customPartitioner);
		}
		else {
			throw new CompilerException("Properties matching logic leaves open cases.");
		}
	}
	
	/**
	 * Parameterizes the ship strategy fields of a channel such that the channel produces the desired global properties.
	 * 
	 * @param channel The channel to parameterize.
	 * @param globalDopChange
	 */
	public void parameterizeChannel(Channel channel, boolean globalDopChange) {
		// if we request nothing, then we need no special strategy. forward, if the number of instances remains
		// the same, randomly repartition otherwise
		if (isTrivial()) {
			channel.setShipStrategy(globalDopChange ? ShipStrategyType.PARTITION_RANDOM : ShipStrategyType.FORWARD);
			return;
		}
		
		final GlobalProperties inGlobals = channel.getSource().getGlobalProperties();
		// if we have no global parallelism change, check if we have already compatible global properties
		if (!globalDopChange && isMetBy(inGlobals)) {
			channel.setShipStrategy(ShipStrategyType.FORWARD);
			return;
		}
		
		// if we fall through the conditions until here, we need to re-establish
		switch (this.partitioning) {
			case FULL_REPLICATION:
				channel.setShipStrategy(ShipStrategyType.BROADCAST);
				break;
			
			case ANY_PARTITIONING:
			case HASH_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_HASH, Utils.createOrderedFromSet(this.partitioningFields));
				break;
			
			case RANGE_PARTITIONED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_RANGE, this.ordering.getInvolvedIndexes(), this.ordering.getFieldSortDirections());
				if(this.dataDistribution != null) {
					channel.setDataDistribution(this.dataDistribution);
				}
				break;
			
			case FORCED_REBALANCED:
				channel.setShipStrategy(ShipStrategyType.PARTITION_FORCED_REBALANCE);
				break;
				
			case CUSTOM_PARTITIONING:
				channel.setShipStrategy(ShipStrategyType.PARTITION_CUSTOM, Utils.createOrderedFromSet(this.partitioningFields), this.customPartitioner);
				break;
				
			default:
				throw new CompilerException("Invalid partitioning to create through a data exchange: " + this.partitioning.name());
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
		if (obj != null && obj instanceof RequestedGlobalProperties) {
			RequestedGlobalProperties other = (RequestedGlobalProperties) obj;
			return (ordering == other.getOrdering() || (ordering != null && ordering.equals(other.getOrdering())))
					&& (partitioning == other.getPartitioning())
					&& (partitioningFields == other.partitioningFields || 
							(partitioningFields != null && partitioningFields.equals(other.getPartitionedFields())));
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "Requested Global Properties [partitioning=" + partitioning + 
			(this.partitioningFields == null ? "" : ", on fields " + this.partitioningFields) + 
			(this.ordering == null ? "" : ", with ordering " + this.ordering) + "]";
	}

	public RequestedGlobalProperties clone() {
		try {
			return (RequestedGlobalProperties) super.clone();
		} catch (CloneNotSupportedException cnse) {
			// should never happen, but propagate just in case
			throw new RuntimeException(cnse);
		}
	}
}
