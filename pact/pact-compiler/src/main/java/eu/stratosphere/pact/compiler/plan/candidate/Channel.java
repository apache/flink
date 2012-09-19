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

package eu.stratosphere.pact.compiler.plan.candidate;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.plan.EstimateProvider;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 *
 * @author Stephan Ewen
 */
public class Channel implements EstimateProvider
{
	/**
	 * Enumeration to indicate the mode of temporarily materializing the data that flows across a connection.
	 * Introducing such an artificial dam is sometimes necessary to avoid that a certain data flows deadlock
	 * themselves.
	 */
	public enum TempMode {
		NONE, TEMP_SENDER_SIDE, TEMP_RECEIVER_SIDE
	}
	
	// --------------------------------------------------------------------------------------------
	
	private PlanNode source;
	
	private PlanNode target;
	
	private ShipStrategyType shipStrategy = ShipStrategyType.NONE;
	
	private LocalStrategy localStrategy = LocalStrategy.NONE;
	
	private FieldList shipKeys;
	
	private FieldList localKeys;
	
	private boolean[] shipSortOrder;
	
	private boolean[] localSortOrder;
	
	private GlobalProperties globalProps;
	
	private LocalProperties localProps;
	
	private TempMode tempMode;
	
	private int replicationFactor = 1;
	
	// --------------------------------------------------------------------------------------------
	
	public Channel(PlanNode sourceNode) {
		this.source = sourceNode;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the source of this Channel.
	 *
	 * @return The source.
	 */
	public PlanNode getSource() {
		return this.source;
	}
	
	/**
	 * Gets the target of this Channel.
	 *
	 * @return The target.
	 */
	public PlanNode getTarget() {
		return this.target;
	}
	
	public void setShipStrategy(ShipStrategyType strategy) {
		setShipStrategy(strategy, null, null);
	}
	
	public void setShipStrategy(ShipStrategyType strategy, FieldList keys) {
		setShipStrategy(strategy, keys, null);
	}
	
	public void setShipStrategy(ShipStrategyType strategy, FieldList keys, boolean[] sortDirection) {
		this.shipStrategy = strategy;
		this.shipKeys = keys;
		this.shipSortOrder = sortDirection;
	}
	
	
	public ShipStrategyType getShipStrategy() {
		return this.shipStrategy;
	}
	
	public FieldList getShipStrategyKeys() {
		return this.shipKeys;
	}
	
	public boolean[] getShipStrategySortOrder() {
		return this.shipSortOrder;
	}
	
	
	
	public void setLocalStrategy(LocalStrategy strategy) {
		setLocalStrategy(strategy, null, null);
	}
	
	public void setLocalStrategy(LocalStrategy strategy, FieldList keys) {
		setLocalStrategy(strategy, keys, null);
	}
	
	public void setLocalStrategy(LocalStrategy strategy, FieldList keys, boolean[] sortDirection) {
		this.localStrategy = strategy;
		this.localKeys = keys;
		this.localSortOrder = sortDirection;
	}
	
	public LocalStrategy getLocalStrategy() {
		return this.localStrategy;
	}
	
	public FieldList getLocalStrategyKeys() {
		return this.localKeys;
	}
	
	public boolean[] getLocalStrategySortOrder() {
		return this.localSortOrder;
	}
	
	
	/**
	 * Returns the TempMode of the Connection. NONE if the connection is not temped,
	 * TEMP_SENDER_SIDE if the connection is temped on the sender node, and
	 * TEMP_RECEIVER_SIDE if the connection is temped on the receiver node.
	 * 
	 * @return TempMode of the connection
	 */
	public TempMode getTempMode() {
		return this.tempMode;
	}

	/**
	 * Sets the temp mode of the connection.
	 * 
	 * @param tempMode
	 *        The temp mode of the connection.
	 */
	public void setTempMode(TempMode tempMode) {
		this.tempMode = tempMode;
	}
	
	/**
	 * Sets the replication factor of the connection.
	 * 
	 * @param factor The replication factor of the connection.
	 */
	public void setReplicationFactor(int factor) {
		this.replicationFactor = factor;
	}
	
	/**
	 * Returns the replication factor of the connection.
	 * 
	 * @return The replication factor of the connection.
	 */
	public int getReplicationFactor() {
		return this.replicationFactor;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedOutputSize()
	 */
	@Override
	public long getEstimatedOutputSize() {
		return this.source.template.getEstimatedOutputSize() * this.replicationFactor;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedNumRecords()
	 */
	@Override
	public long getEstimatedNumRecords() {
		return this.source.template.getEstimatedNumRecords() * this.replicationFactor;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedCardinalities()
	 */
	@Override
	public Map<FieldSet, Long> getEstimatedCardinalities() {
		final Map<FieldSet, Long> m = this.source.template.getEstimatedCardinalities();
		final Map<FieldSet, Long> res = new HashMap<FieldSet, Long>();
		for (Map.Entry<FieldSet, Long> entry : m.entrySet()) {
			res.put(entry.getKey(), entry.getValue() * this.replicationFactor);
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.EstimateProvider#getEstimatedCardinality(eu.stratosphere.pact.common.util.FieldSet)
	 */
	@Override
	public long getEstimatedCardinality(FieldSet cP) {
		return this.source.template.getEstimatedCardinality(cP) * this.replicationFactor;
	}
	
	// --------------------------------------------------------------------------------------------
	

	public GlobalProperties getGlobalProperties() {
		if (this.globalProps == null) {
			this.globalProps = this.source.getGlobalProperties().clone();
			switch (this.shipStrategy) {
				case BROADCAST:
					this.globalProps.setFullyReplicated();
					break;
				case PARTITION_HASH:
				case PARTITION_LOCAL_HASH:
					this.globalProps.setHashPartitioned(this.shipKeys);
					break;
				case PARTITION_RANGE:
				case PARTITION_LOCAL_RANGE:
					Ordering o = new Ordering();
					for (int i = 0; i < this.shipKeys.size(); i++) {
						o.appendOrdering(this.shipKeys.get(i), null, this.shipSortOrder == null || this.shipSortOrder[i] ? Order.ASCENDING : Order.DESCENDING);
					}
					this.globalProps.setRangePartitioned(o);
					break;
				case FORWARD:
					break;
				case NONE:
					throw new CompilerException("Cannot produce GlobalProperties before ship strategy is set.");
			}
		}
		
		return this.globalProps;
	}
	
	public LocalProperties getLocalProperties() {
		if (this.localProps == null) {
			this.localProps = getLocalPropertiesAfterShippingOnly().clone();
			switch (this.localStrategy) {
				case NONE:
					break;
				case SORT:
					Ordering o = new Ordering();
					for (int i = 0; i < this.localKeys.size(); i++) {
						o.appendOrdering(this.localKeys.get(i), null, this.localSortOrder == null || this.localSortOrder[i] ? Order.ASCENDING : Order.DESCENDING);
					}
					this.localProps.setOrdering(o);
					this.localProps.setGroupedFields(o.getInvolvedIndexes());
					break;
				default:
					throw new CompilerException("Unsupported local strategy for channel.");
			}
		}
		
		return this.localProps;
	}
	
	public LocalProperties getLocalPropertiesAfterShippingOnly() {
		if (this.shipStrategy == ShipStrategyType.FORWARD) {
			return this.source.getLocalProperties();
		} else {
			final LocalProperties props = this.source.getLocalProperties().clone();
			switch (this.shipStrategy) {
				case BROADCAST:
				case PARTITION_HASH:
				case PARTITION_RANGE:
				case PARTITION_LOCAL_HASH:
				case PARTITION_LOCAL_RANGE:
					props.reset();
					break;
				case NONE:
					throw new CompilerException("ShipStrategy has not yet been set.");
				default:
					throw new CompilerException();
			}
			return props;
		}
	}
}
