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

package org.apache.flink.optimizer.dataproperties;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * This class represents the global properties of the data that are requested by an operator.
 * Operators request the global properties they need for correct execution. This list is an example of global
 * properties requested by certain operators:
 * <ul>
 *     <li>"groupBy/reduce" will request the data to be partitioned in some way after the key fields.</li>
 *     <li>"map" will request the data to be in an arbitrary distribution - it has no prerequisites</li>
 *     <li>"join" will request certain properties for each input. This class represents the properties
 *         on an input alone. The properties may be partitioning on the key fields, or a combination of
 *         replication on one input and anything-but-replication on the other input.</li>
 * </ul>
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
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets these properties to request a hash partitioning on the given fields.
	 *
	 * If the fields are provided as {@link FieldSet}, then any permutation of the fields is a
	 * valid partitioning, including subsets. If the fields are given as a {@link FieldList},
	 * then only an exact partitioning on the fields matches this requested partitioning.
	 *
	 * @param partitionedFields The key fields for the partitioning.
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

	/**
	 * Sets these properties to request some partitioning on the given fields. This will allow
	 * both hash partitioning and range partitioning to match.
	 *
	 * If the fields are provided as {@link FieldSet}, then any permutation of the fields is a
	 * valid partitioning, including subsets. If the fields are given as a {@link FieldList},
	 * then only an exact partitioning on the fields matches this requested partitioning.
	 *
	 * @param partitionedFields The key fields for the partitioning.
	 */
	public void setAnyPartitioning(FieldSet partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		this.partitioning = PartitioningProperty.ANY_PARTITIONING;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	
	public void setRandomPartitioning() {
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
		this.partitioningFields = null;
		this.ordering = null;
	}

	public void setAnyDistribution() {
		this.partitioning = PartitioningProperty.ANY_DISTRIBUTION;
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

	/**
	 * Sets these properties to request a custom partitioning with the given {@link Partitioner} instance.
	 *
	 * If the fields are provided as {@link FieldSet}, then any permutation of the fields is a
	 * valid partitioning, including subsets. If the fields are given as a {@link FieldList},
	 * then only an exact partitioning on the fields matches this requested partitioning.
	 *
	 * @param partitionedFields The key fields for the partitioning.
	 */
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
		return this.partitioning == null || this.partitioning == PartitioningProperty.RANDOM_PARTITIONED;
	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
		this.ordering = null;
		this.partitioningFields = null;
		this.dataDistribution = null;
		this.customPartitioner = null;
	}

	/**
	 * Filters these properties by what can be preserved by the given SemanticProperties when propagated down
	 * to the given input.
	 *
	 * @param props The SemanticProperties which define which fields are preserved.
	 * @param input The index of the operator's input.
	 * @return The filtered RequestedGlobalProperties
	 */
	public RequestedGlobalProperties filterBySemanticProperties(SemanticProperties props, int input) {
		// no semantic properties available. All global properties are filtered.
		if (props == null) {
			throw new NullPointerException("SemanticProperties may not be null.");
		}

		RequestedGlobalProperties rgProp = new RequestedGlobalProperties();

		switch(this.partitioning) {
			case FULL_REPLICATION:
			case FORCED_REBALANCED:
			case CUSTOM_PARTITIONING:
			case RANDOM_PARTITIONED:
			case ANY_DISTRIBUTION:
				// make sure that certain properties are not pushed down
				return null;
			case HASH_PARTITIONED:
			case ANY_PARTITIONING:
				FieldSet newFields;
				if(this.partitioningFields instanceof FieldList) {
					newFields = new FieldList();
				} else {
					newFields = new FieldSet();
				}

				for (Integer targetField : this.partitioningFields) {
					int sourceField = props.getForwardingSourceField(input, targetField);
					if (sourceField >= 0) {
						newFields = newFields.addField(sourceField);
					} else {
						// partial partitionings are not preserved to avoid skewed partitioning
						return null;
					}
				}
				rgProp.partitioning = this.partitioning;
				rgProp.partitioningFields = newFields;
				return rgProp;
			case RANGE_PARTITIONED:
				// range partitioning
				Ordering newOrdering = new Ordering();
				for (int i = 0; i < this.ordering.getInvolvedIndexes().size(); i++) {
					int value = this.ordering.getInvolvedIndexes().get(i);
					int sourceField = props.getForwardingSourceField(input, value);
					if (sourceField >= 0) {
						newOrdering.appendOrdering(sourceField, this.ordering.getType(i), this.ordering.getOrder(i));
					} else {
						return null;
					}
				}
				rgProp.partitioning = this.partitioning;
				rgProp.ordering = newOrdering;
				rgProp.dataDistribution = this.dataDistribution;
				return rgProp;
			default:
				throw new RuntimeException("Unknown partitioning type encountered.");
		}
	}

	/**
	 * Checks, if this set of interesting properties, is met by the given
	 * produced properties.
	 * 
	 * @param props The properties for which to check whether they meet these properties.
	 * @return True, if the properties are met, false otherwise.
	 */
	public boolean isMetBy(GlobalProperties props) {
		if (this.partitioning == PartitioningProperty.ANY_DISTRIBUTION) {
			return true;
		} else if (this.partitioning == PartitioningProperty.FULL_REPLICATION) {
			return props.isFullyReplicated();
		}
		else if (props.isFullyReplicated()) {
			return false;
		}
		else if (this.partitioning == PartitioningProperty.RANDOM_PARTITIONED) {
			return true;
		}
		else if (this.partitioning == PartitioningProperty.ANY_PARTITIONING) {
			return checkCompatiblePartitioningFields(props);
		}
		else if (this.partitioning == PartitioningProperty.HASH_PARTITIONED) {
			return props.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
					checkCompatiblePartitioningFields(props);
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
					checkCompatiblePartitioningFields(props) &&
					props.getCustomPartitioner().equals(this.customPartitioner);

		}
		else {
			throw new CompilerException("Properties matching logic leaves open cases.");
		}
	}

	/**
	 * Parametrizes the ship strategy fields of a channel such that the channel produces
	 * the desired global properties.
	 * 
	 * @param channel The channel to parametrize.
	 * @param globalDopChange Flag indicating whether the parallelism changes
	 *                        between sender and receiver.
	 * @param exchangeMode The mode of data exchange (pipelined, always batch,
	 *                     batch only on shuffle, ...)
	 * @param breakPipeline Indicates whether this data exchange should break
	 *                      pipelines (unless pipelines are forced).
	 */
	public void parameterizeChannel(Channel channel, boolean globalDopChange,
									ExecutionMode exchangeMode, boolean breakPipeline) {

		// safety check. Fully replicated input must be preserved.
		if (channel.getSource().getGlobalProperties().isFullyReplicated() &&
				!(this.partitioning == PartitioningProperty.FULL_REPLICATION ||
					this.partitioning == PartitioningProperty.ANY_DISTRIBUTION))
		{
			throw new CompilerException("Fully replicated input must be preserved " +
					"and may not be converted into another global property.");
		}

		// if we request nothing, then we need no special strategy. forward, if the number of instances remains
		// the same, randomly repartition otherwise
		if (isTrivial() || this.partitioning == PartitioningProperty.ANY_DISTRIBUTION) {
			ShipStrategyType shipStrategy = globalDopChange ? ShipStrategyType.PARTITION_RANDOM :
																ShipStrategyType.FORWARD;

			DataExchangeMode em = DataExchangeMode.select(exchangeMode, shipStrategy, breakPipeline);
			channel.setShipStrategy(shipStrategy, em);
			return;
		}
		
		final GlobalProperties inGlobals = channel.getSource().getGlobalProperties();
		// if we have no global parallelism change, check if we have already compatible global properties
		if (!globalDopChange && isMetBy(inGlobals)) {
			DataExchangeMode em = DataExchangeMode.select(exchangeMode, ShipStrategyType.FORWARD, breakPipeline);
			channel.setShipStrategy(ShipStrategyType.FORWARD, em);
			return;
		}
		
		// if we fall through the conditions until here, we need to re-establish
		ShipStrategyType shipType;
		FieldList partitionKeys;
		boolean[] sortDirection;
		Partitioner<?> partitioner;

		switch (this.partitioning) {
			case FULL_REPLICATION:
				shipType = ShipStrategyType.BROADCAST;
				partitionKeys = null;
				sortDirection = null;
				partitioner = null;
				break;

			case ANY_PARTITIONING:
			case HASH_PARTITIONED:
				shipType = ShipStrategyType.PARTITION_HASH;
				partitionKeys = Utils.createOrderedFromSet(this.partitioningFields);
				sortDirection = null;
				partitioner = null;
				break;
			
			case RANGE_PARTITIONED:
				shipType = ShipStrategyType.PARTITION_RANGE;
				partitionKeys = this.ordering.getInvolvedIndexes();
				sortDirection = this.ordering.getFieldSortDirections();
				partitioner = null;

				if (this.dataDistribution != null) {
					channel.setDataDistribution(this.dataDistribution);
				}
				break;

			case FORCED_REBALANCED:
				shipType = ShipStrategyType.PARTITION_FORCED_REBALANCE;
				partitionKeys = null;
				sortDirection = null;
				partitioner = null;
				break;

			case CUSTOM_PARTITIONING:
				shipType = ShipStrategyType.PARTITION_CUSTOM;
				partitionKeys = Utils.createOrderedFromSet(this.partitioningFields);
				sortDirection = null;
				partitioner = this.customPartitioner;
				break;

			default:
				throw new CompilerException("Invalid partitioning to create through a data exchange: "
											+ this.partitioning.name());
		}

		DataExchangeMode exMode = DataExchangeMode.select(exchangeMode, shipType, breakPipeline);
		channel.setShipStrategy(shipType, partitionKeys, sortDirection, partitioner, exMode);
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
		if (obj instanceof RequestedGlobalProperties) {
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

	private boolean checkCompatiblePartitioningFields(GlobalProperties props) {
		if(this.partitioningFields instanceof FieldList) {
			// partitioningFields as FieldList requires strict checking!
			return props.isExactlyPartitionedOnFields((FieldList)this.partitioningFields);
		} else {
			return props.isPartitionedOnFields(this.partitioningFields);
		}
	}
}
