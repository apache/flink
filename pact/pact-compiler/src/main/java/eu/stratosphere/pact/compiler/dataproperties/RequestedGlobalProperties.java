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

import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.costs.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.util.Utils;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 * This class represents global properties of the data that an operator is interested in, because it needs those
 * properties for its contract.
 * <p>
 * Currently, the properties are the following: A partitioning type (ANY, HASH, RANGE), and EITHER an ordering (for range partitioning)
 * or an FieldSet with the hash partitioning columns.
 */
public final class RequestedGlobalProperties implements Cloneable
{
	private PartitioningProperty partitioning;	// the type partitioning
	
	private FieldSet partitioningFields;		// the fields which are partitioned
	
	private Ordering ordering;					// order of the partitioned fields, if it is an ordered (range) range partitioning
	
	private DataDistribution<?> dataDistribution; // optional data distribution, for a range partitioning
	
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
	 * @param partitioning The new partitioning to set.
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
	
	public void setRangePartitioned(Ordering ordering, DataDistribution<?> dataDistribution) {
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
	public DataDistribution<?> getDataDistribution() {
		return this.dataDistribution;
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
	}

	/**
	 * Filters these properties by what can be preserved by the given node when propagated down
	 * to the given input.
	 * 
	 * @param node The node representing the contract.
	 * @param input The index of the input.
	 * @return True, if any non-default value is preserved, false otherwise.
	 */
	public RequestedGlobalProperties filterByNodesConstantSet(OptimizerNode node, int input)
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
		} else if (props.isFullyReplicated()) {
			return false;
		} else if (this.partitioning == PartitioningProperty.RANDOM) {
			return true;
		} else if (this.partitioning == PartitioningProperty.ANY_PARTITIONING) {
			return props.isPartitionedOnFields(this.partitioningFields);
		} else if (this.partitioning == PartitioningProperty.HASH_PARTITIONED) {
			return props.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
					props.isPartitionedOnFields(this.partitioningFields);
		} else if (this.partitioning == PartitioningProperty.RANGE_PARTITIONED) {
			return props.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
					props.matchesOrderedPartitioning(this.ordering);
		} else {
			throw new CompilerException("Bug in properties matching logic.");
		}
	}
	
	/**
	 * Parameterizes the ship strategy fields of a channel such that the channel produces the desired global properties.
	 * 
	 * @param channel The channel to parameterize.
	 * @param globalDopChange
	 * @param localDopChange
	 */
	public void parameterizeChannel(Channel channel, boolean globalDopChange, boolean localDopChange) {
		// if we request nothing, then we need no special strategy. forward, if the number of instances remains
		// the same, randomly repartition otherwise
		if (isTrivial()) {
			channel.setShipStrategy(globalDopChange ? ShipStrategyType.PARTITION_RANDOM : ShipStrategyType.FORWARD);
			return;
		}
		
		final GlobalProperties inGlobals = channel.getSource().getGlobalProperties();
		// if we have no global parallelism change, check if we have already compatible global properties
		if (!globalDopChange && isMetBy(inGlobals)) {
			if (localDopChange) {
				// if the local degree of parallelism changes, we need to adjust
				if (inGlobals.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {
					// to preserve the hash partitioning, we need to locally hash re-partition
					channel.setShipStrategy(ShipStrategyType.PARTITION_LOCAL_HASH, inGlobals.getPartitioningFields());
					return;
				}
				// else fall though
			} else {
				// we meet already everything, so go forward
				channel.setShipStrategy(ShipStrategyType.FORWARD);
				return;
			}
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
				
				if(this.dataDistribution != null) {
					// check that provided data distribution is compatible with sort order and 
					// adapt ship strategy keys and orders to data distribution
					final int[] distKeyPositions = this.dataDistribution.getBoundaryKeyPositions();
					final Class<? extends Key>[] distKeyTypes = this.dataDistribution.getBoundaryKeyTypes();
					final Order[] distKeyOrders = this.dataDistribution.getBoundaryKeyOrders();
					
					final Ordering partitionOrdering = new Ordering();
					for(int i=0; i<distKeyPositions.length; i++) {
						if(this.ordering.getFieldNumber(i) == distKeyPositions[i] && 
								this.ordering.getType(i).equals(distKeyTypes[i]) && 
								this.ordering.getOrder(i).equals(distKeyOrders[i])) {
							
							partitionOrdering.appendOrdering(distKeyPositions[i], 
									distKeyTypes[i],
									distKeyOrders[i]);
						} else {
							throw new IllegalArgumentException("Provided data distribution is incompatible with desired sort order.");
						}
					}
					channel.setShipStrategy(ShipStrategyType.PARTITION_RANGE, partitionOrdering.getInvolvedIndexes(), partitionOrdering.getFieldSortDirections());
					channel.setDataDistribution(this.dataDistribution);
				} else {
					channel.setShipStrategy(ShipStrategyType.PARTITION_RANGE, this.ordering.getInvolvedIndexes(), this.ordering.getFieldSortDirections());
				}
				break;
			default:
				throw new CompilerException();
		}
	}
	
	public void addMinimalRequiredCosts(Costs to, CostEstimator estimator, OptimizerNode source, OptimizerNode target) {
		if (this.partitioning == null || this.partitioning == PartitioningProperty.RANDOM) {
			return;
		} else switch (this.partitioning) {
			case FULL_REPLICATION:
				estimator.addBroadcastCost(source, target.getDegreeOfParallelism(), to);
			case ANY_PARTITIONING:
			case HASH_PARTITIONED:
				estimator.addHashPartitioningCost(source, to);
				break;
			case RANGE_PARTITIONED:
				estimator.addRangePartitionCost(source, to);
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
		result = prime * result + ((partitioning == null) ? 0 : partitioning.ordinal());
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Requested Global Properties [partitioning=" + partitioning + 
			(this.partitioningFields == null ? "" : ", on fields " + this.partitioningFields) + 
			(this.ordering == null ? "" : ", with ordering " + this.ordering) + "]";
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public RequestedGlobalProperties clone() {
		try {
			return (RequestedGlobalProperties) super.clone();
		} catch (CloneNotSupportedException cnse) {
			// should never happen, but propagate just in case
			throw new RuntimeException(cnse);
		}
	}
}
