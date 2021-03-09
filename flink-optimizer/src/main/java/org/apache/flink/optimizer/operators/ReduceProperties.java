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

package org.apache.flink.optimizer.operators;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.PartitionNode;
import org.apache.flink.optimizer.dag.ReduceNode;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public final class ReduceProperties extends OperatorDescriptorSingle {
    private static final Logger LOG = LoggerFactory.getLogger(ReduceProperties.class);

    private final Partitioner<?> customPartitioner;

    private final DriverStrategy combinerStrategy;

    public ReduceProperties(FieldSet keys, DriverStrategy combinerStrategy) {
        this(keys, null, combinerStrategy);
    }

    public ReduceProperties(
            FieldSet keys, Partitioner<?> customPartitioner, DriverStrategy combinerStrategy) {
        super(keys);
        this.customPartitioner = customPartitioner;
        this.combinerStrategy = combinerStrategy;
    }

    @Override
    public DriverStrategy getStrategy() {
        return DriverStrategy.SORTED_REDUCE;
    }

    @Override
    public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
        Channel toReducer = in;

        if (in.getShipStrategy() == ShipStrategyType.FORWARD
                || (node.getBroadcastConnections() != null
                        && !node.getBroadcastConnections().isEmpty())) {
            if (in.getSource().getOptimizerNode() instanceof PartitionNode) {
                LOG.warn(
                        "Cannot automatically inject combiner for ReduceFunction. Please add an explicit combiner with combineGroup() in front of the partition operator.");
            }
        } else if (combinerStrategy != DriverStrategy.NONE) {
            // non forward case. all local properties are killed anyways, so we can safely plug in a
            // combiner
            Channel toCombiner = new Channel(in.getSource());
            toCombiner.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);

            // create an input node for combine with same parallelism as input node
            ReduceNode combinerNode = ((ReduceNode) node).getCombinerUtilityNode();
            combinerNode.setParallelism(in.getSource().getParallelism());

            SingleInputPlanNode combiner =
                    new SingleInputPlanNode(
                            combinerNode,
                            "Combine (" + node.getOperator().getName() + ")",
                            toCombiner,
                            this.combinerStrategy,
                            this.keyList);

            combiner.setCosts(new Costs(0, 0));
            combiner.initProperties(
                    toCombiner.getGlobalProperties(), toCombiner.getLocalProperties());

            toReducer = new Channel(combiner);
            toReducer.setShipStrategy(
                    in.getShipStrategy(),
                    in.getShipStrategyKeys(),
                    in.getShipStrategySortOrder(),
                    in.getDataExchangeMode());
            toReducer.setLocalStrategy(
                    LocalStrategy.SORT, in.getLocalStrategyKeys(), in.getLocalStrategySortOrder());
        }

        return new SingleInputPlanNode(
                node,
                "Reduce (" + node.getOperator().getName() + ")",
                toReducer,
                DriverStrategy.SORTED_REDUCE,
                this.keyList);
    }

    @Override
    protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
        RequestedGlobalProperties props = new RequestedGlobalProperties();
        if (customPartitioner == null) {
            props.setAnyPartitioning(this.keys);
        } else {
            props.setCustomPartitioned(this.keys, this.customPartitioner);
        }
        return Collections.singletonList(props);
    }

    @Override
    protected List<RequestedLocalProperties> createPossibleLocalProperties() {
        RequestedLocalProperties props = new RequestedLocalProperties();
        props.setGroupedFields(this.keys);
        return Collections.singletonList(props);
    }

    @Override
    public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
        if (gProps.getUniqueFieldCombination() != null
                && gProps.getUniqueFieldCombination().size() > 0
                && gProps.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED) {
            gProps.setAnyPartitioning(
                    gProps.getUniqueFieldCombination().iterator().next().toFieldList());
        }
        gProps.clearUniqueFieldCombinations();
        return gProps;
    }

    @Override
    public LocalProperties computeLocalProperties(LocalProperties lProps) {
        return lProps.clearUniqueFieldSets();
    }
}
