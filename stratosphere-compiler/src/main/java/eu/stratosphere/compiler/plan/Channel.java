/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.plan;

import eu.stratosphere.api.common.distributions.DataDistribution;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.dag.EstimateProvider;
import eu.stratosphere.compiler.dag.TempMode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plandump.DumpableConnection;
import eu.stratosphere.compiler.util.Utils;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * 
 */
public class Channel implements EstimateProvider, Cloneable, DumpableConnection<PlanNode> {
	
	private PlanNode source;
	
	private PlanNode target;
	
	private ShipStrategyType shipStrategy = ShipStrategyType.NONE;
	
	private LocalStrategy localStrategy = LocalStrategy.NONE;
	
	private FieldList shipKeys;
	
	private FieldList localKeys;
	
	private boolean[] shipSortOrder;
	
	private boolean[] localSortOrder;
	
	private RequestedGlobalProperties requiredGlobalProps;
	
	private RequestedLocalProperties requiredLocalProps;
	
	private GlobalProperties globalProps;
	
	private LocalProperties localProps;
	
	private TypeSerializerFactory<?> serializer;
	
	private TypeComparatorFactory<?> shipStrategyComparator;
	
	private TypeComparatorFactory<?> localStrategyComparator;
	
	private DataDistribution dataDistribution;
	
	private TempMode tempMode;
	
	private double relativeTempMemory;
	
	private double relativeMemoryGlobalStrategy;
	
	private double relativeMemoryLocalStrategy;
	
	private int replicationFactor = 1;
	
	// --------------------------------------------------------------------------------------------
	
	public Channel(PlanNode sourceNode) {
		this(sourceNode, null);
	}
	
	public Channel(PlanNode sourceNode, TempMode tempMode) {
		this.source = sourceNode;
		this.tempMode = (tempMode == null ? TempMode.NONE : tempMode);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                         Accessors
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
	 * Sets the target of this Channel.
	 *
	 * @param target The target.
	 */
	public void setTarget(PlanNode target) {
		this.target = target;
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
		this.globalProps = null;		// reset the global properties
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
		this.localProps = null;		// reset the local properties
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
	
	public void setDataDistribution(DataDistribution dataDistribution) {
		this.dataDistribution = dataDistribution;
	}
	
	public DataDistribution getDataDistribution() {
		return this.dataDistribution;
	}
	
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
	 * Gets the memory for materializing the channel's result from this Channel.
	 *
	 * @return The temp memory.
	 */
	public double getRelativeTempMemory() {
		return this.relativeTempMemory;
	}
	
	/**
	 * Sets the memory for materializing the channel's result from this Channel.
	 *
	 * @param relativeTempMemory The memory for materialization.
	 */
	public void setRelativeTempMemory(double relativeTempMemory) {
		this.relativeTempMemory = relativeTempMemory;
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
	
	/**
	 * Gets the serializer from this Channel.
	 *
	 * @return The serializer.
	 */
	public TypeSerializerFactory<?> getSerializer() {
		return serializer;
	}

	
	/**
	 * Sets the serializer for this Channel.
	 *
	 * @param serializer The serializer to set.
	 */
	public void setSerializer(TypeSerializerFactory<?> serializer) {
		this.serializer = serializer;
	}
	
	/**
	 * Gets the ship strategy comparator from this Channel.
	 *
	 * @return The ship strategy comparator.
	 */
	public TypeComparatorFactory<?> getShipStrategyComparator() {
		return shipStrategyComparator;
	}
	
	/**
	 * Sets the ship strategy comparator for this Channel.
	 *
	 * @param shipStrategyComparator The ship strategy comparator to set.
	 */
	public void setShipStrategyComparator(TypeComparatorFactory<?> shipStrategyComparator) {
		this.shipStrategyComparator = shipStrategyComparator;
	}
	
	/**
	 * Gets the local strategy comparator from this Channel.
	 *
	 * @return The local strategy comparator.
	 */
	public TypeComparatorFactory<?> getLocalStrategyComparator() {
		return localStrategyComparator;
	}
	
	/**
	 * Sets the local strategy comparator for this Channel.
	 *
	 * @param localStrategyComparator The local strategy comparator to set.
	 */
	public void setLocalStrategyComparator(TypeComparatorFactory<?> localStrategyComparator) {
		this.localStrategyComparator = localStrategyComparator;
	}
	
	public double getRelativeMemoryGlobalStrategy() {
		return relativeMemoryGlobalStrategy;
	}
	
	public void setRelativeMemoryGlobalStrategy(double relativeMemoryGlobalStrategy) {
		this.relativeMemoryGlobalStrategy = relativeMemoryGlobalStrategy;
	}
	
	public double getRelativeMemoryLocalStrategy() {
		return relativeMemoryLocalStrategy;
	}
	
	public void setRelativeMemoryLocalStrategy(double relativeMemoryLocalStrategy) {
		this.relativeMemoryLocalStrategy = relativeMemoryLocalStrategy;
	}
	
	public boolean isOnDynamicPath() {
		return this.source.isOnDynamicPath();
	}
	
	public boolean isInsideIteration() {
		return this.source.isInsideIteration();
	}
	
	public int getCostWeight() {
		return this.source.getCostWeight();
	}

	// --------------------------------------------------------------------------------------------
	//                                Statistic Estimates
	// --------------------------------------------------------------------------------------------
	

	@Override
	public long getEstimatedOutputSize() {
		return this.source.template.getEstimatedOutputSize() * this.replicationFactor;
	}

	@Override
	public long getEstimatedNumRecords() {
		return this.source.template.getEstimatedNumRecords() * this.replicationFactor;
	}
	
	@Override
	public float getEstimatedAvgWidthPerOutputRecord() {
		return this.source.template.getEstimatedAvgWidthPerOutputRecord();
	}
	
	// --------------------------------------------------------------------------------------------
	//                                Data Property Handling
	// --------------------------------------------------------------------------------------------
	

	public RequestedGlobalProperties getRequiredGlobalProps() {
		return requiredGlobalProps;
	}

	public void setRequiredGlobalProps(RequestedGlobalProperties requiredGlobalProps) {
		this.requiredGlobalProps = requiredGlobalProps;
	}

	public RequestedLocalProperties getRequiredLocalProps() {
		return requiredLocalProps;
	}

	public void setRequiredLocalProps(RequestedLocalProperties requiredLocalProps) {
		this.requiredLocalProps = requiredLocalProps;
	}

	public GlobalProperties getGlobalProperties() {
		if (this.globalProps == null) {
			this.globalProps = this.source.getGlobalProperties().clone();
			switch (this.shipStrategy) {
				case BROADCAST:
					this.globalProps.clearUniqueFieldCombinations();
					this.globalProps.setFullyReplicated();
					break;
				case PARTITION_HASH:
					this.globalProps.setHashPartitioned(this.shipKeys);
					break;
				case PARTITION_RANGE:
					this.globalProps.setRangePartitioned(Utils.createOrdering(this.shipKeys, this.shipSortOrder));
					break;
				case FORWARD:
					break;
				case PARTITION_RANDOM:
					this.globalProps.reset();
					break;
				case NONE:
					throw new CompilerException("Cannot produce GlobalProperties before ship strategy is set.");
			}
		}
		
		return this.globalProps;
	}
	
	public LocalProperties getLocalProperties() {
		if (this.localProps == null) {
			computeLocalPropertiesAfterShippingOnly();
			switch (this.localStrategy) {
				case NONE:
					break;
				case SORT:
				case COMBININGSORT:
					this.localProps = LocalProperties.forOrdering(Utils.createOrdering(this.localKeys, this.localSortOrder));
					break;
				default:
					throw new CompilerException("Unsupported local strategy for channel.");
			}
		}
		
		return this.localProps;
	}
	
	private void computeLocalPropertiesAfterShippingOnly() {
		switch (this.shipStrategy) {
			case BROADCAST:
			case PARTITION_HASH:
			case PARTITION_RANGE:
			case PARTITION_RANDOM:
				this.localProps = new LocalProperties();
				break;
			case FORWARD:
				this.localProps = this.source.getLocalProperties();
				break;
			case NONE:
				throw new CompilerException("ShipStrategy has not yet been set.");
		}
	}
	
	public void adjustGlobalPropertiesForFullParallelismChange() {
		if (this.shipStrategy == null || this.shipStrategy == ShipStrategyType.NONE) {
			throw new IllegalStateException("Cannot adjust channel for degree of parallelism " +
					"change before the ship strategy is set.");
		}
		
		// make sure the properties are acquired
		if (this.globalProps == null) {
			getGlobalProperties();
		}
		
		// some strategies globally reestablish properties
		switch (this.shipStrategy) {
		case FORWARD:
			throw new CompilerException("Cannot use FORWARD strategy between operations " +
					"with different number of parallel instances.");
		case NONE: // excluded by sanity check. lust here for verification check completion
		case BROADCAST:
		case PARTITION_HASH:
		case PARTITION_RANGE:
		case PARTITION_RANDOM:
			return;
		}
		throw new CompilerException("Unrecognized Ship Strategy Type: " + this.shipStrategy);
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility method used while swapping binary union nodes for n-ary union nodes.
	 * 
	 * @param newUnionNode
	 */
	public void swapUnionNodes(PlanNode newUnionNode) {
		if (!(this.source instanceof BinaryUnionPlanNode)) {
			throw new IllegalStateException();
		} else {
			this.source = newUnionNode;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public int getMaxDepth() {
		return this.source.getOptimizerNode().getMaxDepth() + 1;
	}
	// --------------------------------------------------------------------------------------------

	

	public String toString() {
		return "Channel (" + this.source + (this.target == null ? ')' : ") -> (" + this.target + ')') +
				'[' + this.shipStrategy + "] [" + this.localStrategy + "] " +
				(this.tempMode == null || this.tempMode == TempMode.NONE ? "{NO-TEMP}" : this.tempMode);
	}
	
	public Channel clone() {
		try {
			return (Channel) super.clone();
		} catch (CloneNotSupportedException cnsex) {
			throw new RuntimeException(cnsex);
		}
	}
}
