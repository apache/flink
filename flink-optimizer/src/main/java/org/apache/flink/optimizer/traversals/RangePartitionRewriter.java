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
package org.apache.flink.optimizer.traversals;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.MapPartitionOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.functions.SampleInCoordinator;
import org.apache.flink.api.java.functions.SampleInPartition;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.MapNode;
import org.apache.flink.optimizer.dag.MapPartitionNode;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.IterationPlanNode;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.udf.AssignRangeIndex;
import org.apache.flink.runtime.operators.udf.RangeBoundaryBuilder;
import org.apache.flink.runtime.operators.udf.RemoveRangeIndex;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** */
public class RangePartitionRewriter implements Visitor<PlanNode> {

    static final long SEED = 0;
    static final String SIP_NAME = "RangePartition: LocalSample";
    static final String SIC_NAME = "RangePartition: GlobalSample";
    static final String RB_NAME = "RangePartition: Histogram";
    static final String ARI_NAME = "RangePartition: PreparePartition";
    static final String PR_NAME = "RangePartition: Partition";

    static final int SAMPLES_PER_PARTITION = 1000;

    static final IdPartitioner idPartitioner = new IdPartitioner();

    final OptimizedPlan plan;
    final Set<IterationPlanNode> visitedIterationNodes;

    public RangePartitionRewriter(OptimizedPlan plan) {
        this.plan = plan;
        this.visitedIterationNodes = new HashSet<>();
    }

    @Override
    public boolean preVisit(PlanNode visitable) {
        return true;
    }

    @Override
    public void postVisit(PlanNode node) {

        if (node instanceof IterationPlanNode) {
            IterationPlanNode iNode = (IterationPlanNode) node;
            if (!visitedIterationNodes.contains(iNode)) {
                visitedIterationNodes.add(iNode);
                iNode.acceptForStepFunction(this);
            }
        }

        final Iterable<Channel> inputChannels = node.getInputs();
        for (Channel channel : inputChannels) {
            ShipStrategyType shipStrategy = channel.getShipStrategy();
            // Make sure we only optimize the DAG for range partition, and do not optimize multi
            // times.
            if (shipStrategy == ShipStrategyType.PARTITION_RANGE) {

                if (channel.getDataDistribution() == null) {
                    if (node.isOnDynamicPath()) {
                        throw new InvalidProgramException(
                                "Range Partitioning not supported within iterations if users do not supply the data distribution.");
                    }

                    PlanNode channelSource = channel.getSource();
                    List<Channel> newSourceOutputChannels = rewriteRangePartitionChannel(channel);
                    channelSource.getOutgoingChannels().remove(channel);
                    channelSource.getOutgoingChannels().addAll(newSourceOutputChannels);
                }
            }
        }
    }

    private List<Channel> rewriteRangePartitionChannel(Channel channel) {
        final List<Channel> sourceNewOutputChannels = new ArrayList<>();
        final PlanNode sourceNode = channel.getSource();
        final PlanNode targetNode = channel.getTarget();
        final int sourceParallelism = sourceNode.getParallelism();
        final int targetParallelism = targetNode.getParallelism();
        final Costs defaultZeroCosts = new Costs(0, 0, 0);
        final TypeComparatorFactory<?> comparator =
                Utils.getShipComparator(channel, this.plan.getOriginalPlan().getExecutionConfig());
        // 1. Fixed size sample in each partitions.
        final int sampleSize = SAMPLES_PER_PARTITION * targetParallelism;
        final SampleInPartition sampleInPartition = new SampleInPartition(false, sampleSize, SEED);
        final TypeInformation<?> sourceOutputType =
                sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType();
        final TypeInformation<IntermediateSampleData> isdTypeInformation =
                TypeExtractor.getForClass(IntermediateSampleData.class);
        final UnaryOperatorInformation sipOperatorInformation =
                new UnaryOperatorInformation(sourceOutputType, isdTypeInformation);
        final MapPartitionOperatorBase sipOperatorBase =
                new MapPartitionOperatorBase(sampleInPartition, sipOperatorInformation, SIP_NAME);
        final MapPartitionNode sipNode = new MapPartitionNode(sipOperatorBase);
        final Channel sipChannel = new Channel(sourceNode, TempMode.NONE);
        sipChannel.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);
        final SingleInputPlanNode sipPlanNode =
                new SingleInputPlanNode(
                        sipNode, SIP_NAME, sipChannel, DriverStrategy.MAP_PARTITION);
        sipNode.setParallelism(sourceParallelism);
        sipPlanNode.setParallelism(sourceParallelism);
        sipPlanNode.initProperties(new GlobalProperties(), new LocalProperties());
        sipPlanNode.setCosts(defaultZeroCosts);
        sipChannel.setTarget(sipPlanNode);
        this.plan.getAllNodes().add(sipPlanNode);
        sourceNewOutputChannels.add(sipChannel);

        // 2. Fixed size sample in a single coordinator.
        final SampleInCoordinator sampleInCoordinator =
                new SampleInCoordinator(false, sampleSize, SEED);
        final UnaryOperatorInformation sicOperatorInformation =
                new UnaryOperatorInformation(isdTypeInformation, sourceOutputType);
        final GroupReduceOperatorBase sicOperatorBase =
                new GroupReduceOperatorBase(sampleInCoordinator, sicOperatorInformation, SIC_NAME);
        final GroupReduceNode sicNode = new GroupReduceNode(sicOperatorBase);
        final Channel sicChannel = new Channel(sipPlanNode, TempMode.NONE);
        sicChannel.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);
        final SingleInputPlanNode sicPlanNode =
                new SingleInputPlanNode(
                        sicNode, SIC_NAME, sicChannel, DriverStrategy.ALL_GROUP_REDUCE);
        sicNode.setParallelism(1);
        sicPlanNode.setParallelism(1);
        sicPlanNode.initProperties(new GlobalProperties(), new LocalProperties());
        sicPlanNode.setCosts(defaultZeroCosts);
        sicChannel.setTarget(sicPlanNode);
        sipPlanNode.addOutgoingChannel(sicChannel);
        this.plan.getAllNodes().add(sicPlanNode);

        // 3. Use sampled data to build range boundaries.
        final RangeBoundaryBuilder rangeBoundaryBuilder =
                new RangeBoundaryBuilder(comparator, targetParallelism);
        final TypeInformation<CommonRangeBoundaries> rbTypeInformation =
                TypeExtractor.getForClass(CommonRangeBoundaries.class);
        final UnaryOperatorInformation rbOperatorInformation =
                new UnaryOperatorInformation(sourceOutputType, rbTypeInformation);
        final MapPartitionOperatorBase rbOperatorBase =
                new MapPartitionOperatorBase(rangeBoundaryBuilder, rbOperatorInformation, RB_NAME);
        final MapPartitionNode rbNode = new MapPartitionNode(rbOperatorBase);
        final Channel rbChannel = new Channel(sicPlanNode, TempMode.NONE);
        rbChannel.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);
        final SingleInputPlanNode rbPlanNode =
                new SingleInputPlanNode(rbNode, RB_NAME, rbChannel, DriverStrategy.MAP_PARTITION);
        rbNode.setParallelism(1);
        rbPlanNode.setParallelism(1);
        rbPlanNode.initProperties(new GlobalProperties(), new LocalProperties());
        rbPlanNode.setCosts(defaultZeroCosts);
        rbChannel.setTarget(rbPlanNode);
        sicPlanNode.addOutgoingChannel(rbChannel);
        this.plan.getAllNodes().add(rbPlanNode);

        // 4. Take range boundaries as broadcast input and take the tuple of partition id and record
        // as output.
        final AssignRangeIndex assignRangeIndex = new AssignRangeIndex(comparator);
        final TypeInformation<Tuple2> ariOutputTypeInformation =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, sourceOutputType);
        final UnaryOperatorInformation ariOperatorInformation =
                new UnaryOperatorInformation(sourceOutputType, ariOutputTypeInformation);
        final MapPartitionOperatorBase ariOperatorBase =
                new MapPartitionOperatorBase(assignRangeIndex, ariOperatorInformation, ARI_NAME);
        final MapPartitionNode ariNode = new MapPartitionNode(ariOperatorBase);
        final Channel ariChannel = new Channel(sourceNode, TempMode.NONE);
        // To avoid deadlock, set the DataExchangeMode of channel between source node and this to
        // Batch.
        ariChannel.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.BATCH);
        final SingleInputPlanNode ariPlanNode =
                new SingleInputPlanNode(
                        ariNode, ARI_NAME, ariChannel, DriverStrategy.MAP_PARTITION);
        ariNode.setParallelism(sourceParallelism);
        ariPlanNode.setParallelism(sourceParallelism);
        ariPlanNode.initProperties(new GlobalProperties(), new LocalProperties());
        ariPlanNode.setCosts(defaultZeroCosts);
        ariChannel.setTarget(ariPlanNode);
        this.plan.getAllNodes().add(ariPlanNode);
        sourceNewOutputChannels.add(ariChannel);

        final NamedChannel broadcastChannel = new NamedChannel("RangeBoundaries", rbPlanNode);
        broadcastChannel.setShipStrategy(ShipStrategyType.BROADCAST, DataExchangeMode.PIPELINED);
        broadcastChannel.setTarget(ariPlanNode);
        List<NamedChannel> broadcastChannels = new ArrayList<>(1);
        broadcastChannels.add(broadcastChannel);
        ariPlanNode.setBroadcastInputs(broadcastChannels);

        // 5. Remove the partition id.
        final Channel partChannel = new Channel(ariPlanNode, TempMode.NONE);
        final FieldList keys = new FieldList(0);
        partChannel.setShipStrategy(
                ShipStrategyType.PARTITION_CUSTOM, keys, idPartitioner, DataExchangeMode.PIPELINED);
        ariPlanNode.addOutgoingChannel(partChannel);

        final RemoveRangeIndex partitionIDRemoveWrapper = new RemoveRangeIndex();
        final UnaryOperatorInformation prOperatorInformation =
                new UnaryOperatorInformation(ariOutputTypeInformation, sourceOutputType);
        final MapOperatorBase prOperatorBase =
                new MapOperatorBase(partitionIDRemoveWrapper, prOperatorInformation, PR_NAME);
        final MapNode prRemoverNode = new MapNode(prOperatorBase);
        final SingleInputPlanNode prPlanNode =
                new SingleInputPlanNode(prRemoverNode, PR_NAME, partChannel, DriverStrategy.MAP);
        partChannel.setTarget(prPlanNode);
        prRemoverNode.setParallelism(targetParallelism);
        prPlanNode.setParallelism(targetParallelism);
        GlobalProperties globalProperties = new GlobalProperties();
        globalProperties.setRangePartitioned(new Ordering(0, null, Order.ASCENDING));
        prPlanNode.initProperties(globalProperties, new LocalProperties());
        prPlanNode.setCosts(defaultZeroCosts);
        this.plan.getAllNodes().add(prPlanNode);

        // 6. Connect to target node.
        channel.setSource(prPlanNode);
        channel.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);
        prPlanNode.addOutgoingChannel(channel);

        return sourceNewOutputChannels;
    }
}
