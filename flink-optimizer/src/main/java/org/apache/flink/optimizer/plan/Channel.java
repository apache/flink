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

package org.apache.flink.optimizer.plan;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.EstimateProvider;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plandump.DumpableConnection;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Channel represents the result produced by an operator and the data exchange before the
 * consumption by the target operator.
 *
 * <p>The channel defines and tracks various properties and characteristics of the data set and data
 * exchange.
 *
 * <p>Data set characteristics:
 *
 * <ul>
 *   <li>The "global properties" of the data, i.e., how the data is distributed across partitions
 *   <li>The "required global properties" of the data, i.e., the global properties that, if absent,
 *       would cause the program to return a wrong result.
 *   <li>The "local properties" of the data, i.e., how the data is organized within a partition
 *   <li>The "required local properties" of the data, i.e., the local properties that, if absent,
 *       would cause the program to return a wrong result.
 * </ul>
 *
 * Data exchange parameters:
 *
 * <ul>
 *   <li>The "ship strategy", i.e., whether to forward the data, shuffle it, broadcast it, ...
 *   <li>The "ship keys", which are the positions of the key fields in the exchanged records.
 *   <li>The "data exchange mode", which defines whether to pipeline or batch the exchange
 *   <li>Several more...
 * </ul>
 */
public class Channel implements EstimateProvider, Cloneable, DumpableConnection<PlanNode> {

    private PlanNode source;

    private PlanNode target;

    private ShipStrategyType shipStrategy = ShipStrategyType.NONE;

    private DataExchangeMode dataExchangeMode;

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

    private Partitioner<?> partitioner;

    private TempMode tempMode;

    private double relativeTempMemory;

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
    @Override
    public PlanNode getSource() {
        return this.source;
    }

    public void setSource(PlanNode source) {
        this.source = source;
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

    public void setShipStrategy(ShipStrategyType strategy, DataExchangeMode dataExchangeMode) {
        setShipStrategy(strategy, null, null, null, dataExchangeMode);
    }

    public void setShipStrategy(
            ShipStrategyType strategy, FieldList keys, DataExchangeMode dataExchangeMode) {
        setShipStrategy(strategy, keys, null, null, dataExchangeMode);
    }

    public void setShipStrategy(
            ShipStrategyType strategy,
            FieldList keys,
            boolean[] sortDirection,
            DataExchangeMode dataExchangeMode) {
        setShipStrategy(strategy, keys, sortDirection, null, dataExchangeMode);
    }

    public void setShipStrategy(
            ShipStrategyType strategy,
            FieldList keys,
            Partitioner<?> partitioner,
            DataExchangeMode dataExchangeMode) {
        setShipStrategy(strategy, keys, null, partitioner, dataExchangeMode);
    }

    public void setShipStrategy(
            ShipStrategyType strategy,
            FieldList keys,
            boolean[] sortDirection,
            Partitioner<?> partitioner,
            DataExchangeMode dataExchangeMode) {
        this.shipStrategy = strategy;
        this.shipKeys = keys;
        this.shipSortOrder = sortDirection;
        this.partitioner = partitioner;
        this.dataExchangeMode = dataExchangeMode;
        this.globalProps = null; // reset the global properties
    }

    /**
     * Sets the data exchange mode (batch / pipelined) to use for the data exchange of this channel.
     */
    public void setDataExchangeMode(DataExchangeMode dataExchangeMode) {
        this.dataExchangeMode = checkNotNull(dataExchangeMode);
    }

    /**
     * Gets the data exchange mode (batch / pipelined) to use for the data exchange of this channel.
     *
     * @return The data exchange mode of this channel.
     */
    public DataExchangeMode getDataExchangeMode() {
        return dataExchangeMode;
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

    public void setLocalStrategy(LocalStrategy strategy, FieldList keys, boolean[] sortDirection) {
        this.localStrategy = strategy;
        this.localKeys = keys;
        this.localSortOrder = sortDirection;
        this.localProps = null; // reset the local properties
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

    public Partitioner<?> getPartitioner() {
        return partitioner;
    }

    public TempMode getTempMode() {
        return this.tempMode;
    }

    /**
     * Sets the temp mode of the connection.
     *
     * @param tempMode The temp mode of the connection.
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

    public double getRelativeMemoryLocalStrategy() {
        return relativeMemoryLocalStrategy;
    }

    public void setRelativeMemoryLocalStrategy(double relativeMemoryLocalStrategy) {
        this.relativeMemoryLocalStrategy = relativeMemoryLocalStrategy;
    }

    public boolean isOnDynamicPath() {
        return this.source.isOnDynamicPath();
    }

    public int getCostWeight() {
        return this.source.getCostWeight();
    }

    // --------------------------------------------------------------------------------------------
    //                                Statistic Estimates
    // --------------------------------------------------------------------------------------------

    @Override
    public long getEstimatedOutputSize() {
        long estimate = this.source.template.getEstimatedOutputSize();
        return estimate < 0 ? estimate : estimate * this.replicationFactor;
    }

    @Override
    public long getEstimatedNumRecords() {
        long estimate = this.source.template.getEstimatedNumRecords();
        return estimate < 0 ? estimate : estimate * this.replicationFactor;
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
                    this.globalProps.setRangePartitioned(
                            Utils.createOrdering(this.shipKeys, this.shipSortOrder),
                            this.dataDistribution);
                    break;
                case FORWARD:
                    break;
                case PARTITION_RANDOM:
                    this.globalProps.reset();
                    break;
                case PARTITION_FORCED_REBALANCE:
                    this.globalProps.setForcedRebalanced();
                    break;
                case PARTITION_CUSTOM:
                    this.globalProps.setCustomPartitioned(this.shipKeys, this.partitioner);
                    break;
                case NONE:
                    throw new CompilerException(
                            "Cannot produce GlobalProperties before ship strategy is set.");
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
                    this.localProps =
                            LocalProperties.forOrdering(
                                    Utils.createOrdering(this.localKeys, this.localSortOrder));
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
            case PARTITION_CUSTOM:
            case PARTITION_RANGE:
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
                this.localProps = new LocalProperties();
                break;
            case FORWARD:
                this.localProps = this.source.getLocalProperties();
                break;
            case NONE:
                throw new CompilerException("ShipStrategy has not yet been set.");
            default:
                throw new CompilerException("Unknown ShipStrategy.");
        }
    }

    public void adjustGlobalPropertiesForFullParallelismChange() {
        if (this.shipStrategy == null || this.shipStrategy == ShipStrategyType.NONE) {
            throw new IllegalStateException(
                    "Cannot adjust channel for parallelism "
                            + "change before the ship strategy is set.");
        }

        // make sure the properties are acquired
        if (this.globalProps == null) {
            getGlobalProperties();
        }

        // some strategies globally reestablish properties
        switch (this.shipStrategy) {
            case FORWARD:
                throw new CompilerException(
                        "Cannot use FORWARD strategy between operations "
                                + "with different number of parallel instances.");
            case NONE: // excluded by sanity check. left here for verification check completion
            case BROADCAST:
            case PARTITION_HASH:
            case PARTITION_RANGE:
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
            case PARTITION_CUSTOM:
                return;
        }
        throw new CompilerException("Unrecognized Ship Strategy Type: " + this.shipStrategy);
    }

    // --------------------------------------------------------------------------------------------

    /** Utility method used while swapping binary union nodes for n-ary union nodes. */
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

    @Override
    public String toString() {
        return "Channel ("
                + this.source
                + (this.target == null ? ')' : ") -> (" + this.target + ')')
                + '['
                + this.shipStrategy
                + "] ["
                + this.localStrategy
                + "] "
                + (this.tempMode == null || this.tempMode == TempMode.NONE
                        ? "{NO-TEMP}"
                        : this.tempMode);
    }

    @Override
    public Channel clone() {
        try {
            return (Channel) super.clone();
        } catch (CloneNotSupportedException cnsex) {
            throw new RuntimeException(cnsex);
        }
    }
}
