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
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents global properties of the data at a certain point in the plan.
 * Global properties are properties that describe data across different partitions, such as
 * whether the data is hash partitioned, range partitioned, replicated, etc.
 */
public class GlobalProperties implements Cloneable {

	public static final Logger LOG = LoggerFactory.getLogger(GlobalProperties.class);
	
	private PartitioningProperty partitioning;	// the type partitioning
	
	private FieldList partitioningFields;		// the fields which are partitioned
	
	private Ordering ordering;					// order of the partitioned fields, if it is an ordered (range) range partitioning
	
	private Set<FieldSet> uniqueFieldCombinations;
	
	private Partitioner<?> customPartitioner;
	
	private DataDistribution distribution;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Initializes the global properties with no partitioning.
	 */
	public GlobalProperties() {
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Sets this global properties to represent a hash partitioning.
	 * 
	 * @param partitionedFields The key fields on which the data is hash partitioned.
	 */
	public void setHashPartitioned(FieldList partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.HASH_PARTITIONED;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}

	/**
	 * Set the parameters for range partition.
	 *
	 * @param ordering Order of the partitioned fields
	 */
	public void setRangePartitioned(Ordering ordering) {
		if (ordering == null) {
			throw new NullPointerException();
		}

		this.partitioning = PartitioningProperty.RANGE_PARTITIONED;
		this.ordering = ordering;
		this.partitioningFields = ordering.getInvolvedIndexes();
	}

	/**
	 * Set the parameters for range partition.
	 * 
	 * @param ordering Order of the partitioned fields
	 * @param distribution The data distribution for range partition. User can supply a customized data distribution,
	 *                     also the data distribution can be null.  
	 */
	public void setRangePartitioned(Ordering ordering, DataDistribution distribution) {
		if (ordering == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.RANGE_PARTITIONED;
		this.ordering = ordering;
		this.partitioningFields = ordering.getInvolvedIndexes();
		this.distribution = distribution;
	}
	
	public void setAnyPartitioning(FieldList partitionedFields) {
		if (partitionedFields == null) {
			throw new NullPointerException();
		}
		
		this.partitioning = PartitioningProperty.ANY_PARTITIONING;
		this.partitioningFields = partitionedFields;
		this.ordering = null;
	}
	
	public void setRandomPartitioned() {
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
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
	
	public DataDistribution getDataDistribution() {
		return this.distribution;
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

	public boolean isExactlyPartitionedOnFields(FieldList fields) {
		return this.partitioning.isPartitionedOnKey() && fields.isExactMatch(this.partitioningFields);
	}
	
	public boolean matchesOrderedPartitioning(Ordering o) {
		if (this.partitioning == PartitioningProperty.RANGE_PARTITIONED) {
			if (this.ordering.getNumberOfFields() > o.getNumberOfFields()) {
				return false;
			}
			
			for (int i = 0; i < this.ordering.getNumberOfFields(); i++) {
				if (!this.ordering.getFieldNumber(i).equals(o.getFieldNumber(i))) {
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
		return partitioning == PartitioningProperty.RANDOM_PARTITIONED;
	}

	/**
	 * This method resets the properties to a state where no properties are given.
	 */
	public void reset() {
		this.partitioning = PartitioningProperty.RANDOM_PARTITIONED;
		this.ordering = null;
		this.partitioningFields = null;
	}

	/**
	 * Filters these GlobalProperties by the fields that are forwarded to the output
	 * as described by the SemanticProperties.
	 *
	 * @param props The semantic properties holding information about forwarded fields.
	 * @param input The index of the input.
	 * @return The filtered GlobalProperties
	 */
	public GlobalProperties filterBySemanticProperties(SemanticProperties props, int input) {

		if (props == null) {
			throw new NullPointerException("SemanticProperties may not be null.");
		}

		GlobalProperties gp = new GlobalProperties();

		// filter partitioning
		switch(this.partitioning) {
			case RANGE_PARTITIONED:
				// check if ordering is preserved
				Ordering newOrdering = new Ordering();
				for (int i = 0; i < this.ordering.getInvolvedIndexes().size(); i++) {
					int sourceField = this.ordering.getInvolvedIndexes().get(i);
					FieldSet targetField = props.getForwardingTargetFields(input, sourceField);

					if (targetField == null || targetField.size() == 0) {
						// partitioning is destroyed
						newOrdering = null;
						break;
					} else {
						// use any field of target fields for now. We should use something like field equivalence sets in the future.
						if(targetField.size() > 1) {
							LOG.warn("Found that a field is forwarded to more than one target field in " +
									"semantic forwarded field information. Will only use the field with the lowest index.");
						}
						newOrdering.appendOrdering(targetField.toArray()[0], this.ordering.getType(i), this.ordering.getOrder(i));
					}
				}
				if(newOrdering != null) {
					gp.partitioning = PartitioningProperty.RANGE_PARTITIONED;
					gp.ordering = newOrdering;
					gp.partitioningFields = newOrdering.getInvolvedIndexes();
					gp.distribution = this.distribution;
				}
				break;
			case HASH_PARTITIONED:
			case ANY_PARTITIONING:
			case CUSTOM_PARTITIONING:
				FieldList newPartitioningFields = new FieldList();
				for (int sourceField : this.partitioningFields) {
					FieldSet targetField = props.getForwardingTargetFields(input, sourceField);

					if (targetField == null || targetField.size() == 0) {
						newPartitioningFields = null;
						break;
					} else {
						// use any field of target fields for now.  We should use something like field equivalence sets in the future.
						if(targetField.size() > 1) {
							LOG.warn("Found that a field is forwarded to more than one target field in " +
									"semantic forwarded field information. Will only use the field with the lowest index.");
						}
						newPartitioningFields = newPartitioningFields.addField(targetField.toArray()[0]);
					}
				}
				if(newPartitioningFields != null) {
					gp.partitioning = this.partitioning;
					gp.partitioningFields = newPartitioningFields;
					gp.customPartitioner = this.customPartitioner;
				}
				break;
			case FORCED_REBALANCED:
			case FULL_REPLICATION:
			case RANDOM_PARTITIONED:
				gp.partitioning = this.partitioning;
				break;
			default:
				throw new RuntimeException("Unknown partitioning type.");
		}

		// filter unique field combinations
		if (this.uniqueFieldCombinations != null) {
			Set<FieldSet> newUniqueFieldCombinations = new HashSet<FieldSet>();
			for (FieldSet fieldCombo : this.uniqueFieldCombinations) {
				FieldSet newFieldCombo = new FieldSet();
				for (Integer sourceField : fieldCombo) {
					FieldSet targetField = props.getForwardingTargetFields(input, sourceField);

					if (targetField == null || targetField.size() == 0) {
						newFieldCombo = null;
						break;
					} else {
						// use any field of target fields for now.  We should use something like field equivalence sets in the future.
						if(targetField.size() > 1) {
							LOG.warn("Found that a field is forwarded to more than one target field in " +
									"semantic forwarded field information. Will only use the field with the lowest index.");
						}
						newFieldCombo = newFieldCombo.addField(targetField.toArray()[0]);
					}
				}
				if (newFieldCombo != null) {
					newUniqueFieldCombinations.add(newFieldCombo);
				}
			}
			if(!newUniqueFieldCombinations.isEmpty()) {
				gp.uniqueFieldCombinations = newUniqueFieldCombinations;
			}
		}

		return gp;
	}


	public void parameterizeChannel(Channel channel, boolean globalDopChange,
									ExecutionMode exchangeMode, boolean breakPipeline) {

		ShipStrategyType shipType;
		FieldList partitionKeys;
		boolean[] sortDirection;
		Partitioner<?> partitioner;

		switch (this.partitioning) {
			case RANDOM_PARTITIONED:
				shipType = globalDopChange ? ShipStrategyType.PARTITION_RANDOM : ShipStrategyType.FORWARD;
				partitionKeys = null;
				sortDirection = null;
				partitioner = null;
				break;

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
				break;

			case FORCED_REBALANCED:
				shipType = ShipStrategyType.PARTITION_RANDOM;
				partitionKeys = null;
				sortDirection = null;
				partitioner = null;
				break;

			case CUSTOM_PARTITIONING:
				shipType = ShipStrategyType.PARTITION_CUSTOM;
				partitionKeys = this.partitioningFields;
				sortDirection = null;
				partitioner = this.customPartitioner;
				break;

			default:
				throw new CompilerException("Unsupported partitioning strategy");
		}

		channel.setDataDistribution(this.distribution);
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
		if (obj instanceof GlobalProperties) {
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
		newProps.distribution = this.distribution;
		newProps.customPartitioner = this.customPartitioner;
		newProps.uniqueFieldCombinations = this.uniqueFieldCombinations == null ? null : new HashSet<FieldSet>(this.uniqueFieldCombinations);
		return newProps;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static GlobalProperties combine(GlobalProperties gp1, GlobalProperties gp2) {
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
