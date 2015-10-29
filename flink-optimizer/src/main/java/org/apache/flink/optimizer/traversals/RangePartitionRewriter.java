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

import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.MapPartitionOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.runtime.operators.udf.AssignRangeIndex;
import org.apache.flink.runtime.operators.udf.PartitionIDRemoveWrapper;
import org.apache.flink.runtime.operators.udf.RangeBoundaryBuilder;
import org.apache.flink.api.java.functions.SampleInCoordinator;
import org.apache.flink.api.java.functions.SampleInPartition;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.MapNode;
import org.apache.flink.optimizer.dag.MapPartitionNode;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RangePartitionRewriter implements Visitor<PlanNode> {

	final OptimizedPlan plan;

	public RangePartitionRewriter(OptimizedPlan plan) {
		this.plan = plan;
	}

	@Override
	public boolean preVisit(PlanNode visitable) {
		return true;
	}

	@Override
	public void postVisit(PlanNode visitable) {
		final List<Channel> outgoingChannels = visitable.getOutgoingChannels();
		final List<Channel> newOutGoingChannels = new LinkedList<>();
		final List<Channel> toBeRemoveChannels = new ArrayList<>();
		for (Channel channel : outgoingChannels) {
			ShipStrategyType shipStrategy = channel.getShipStrategy();
			if (shipStrategy == ShipStrategyType.PARTITION_RANGE) {
				newOutGoingChannels.addAll(rewriteRangePartitionChannel(channel));
				toBeRemoveChannels.add(channel);
			}
		}

		for (Channel chan : toBeRemoveChannels) {
			outgoingChannels.remove(chan);
		}
		outgoingChannels.addAll(newOutGoingChannels);
	}

	private List<Channel> rewriteRangePartitionChannel(Channel channel) {
		final List<Channel> sourceNewOutputChannels = new ArrayList<>();
		final PlanNode sourceNode = channel.getSource();
		final PlanNode targetNode = channel.getTarget();
		final int sourceParallelism = sourceNode.getParallelism();
		final int targetParallelism = targetNode.getParallelism();
		final TypeComparatorFactory<?> comparator = Utils.getShipComparator(channel, this.plan.getOriginalPlan().getExecutionConfig());
		// 1. Fixed size sample in each partitions.
		final long seed = org.apache.flink.api.java.Utils.RNG.nextLong();
		final int sampleSize = 20 * targetParallelism;
		final SampleInPartition sampleInPartition = new SampleInPartition(false, sampleSize, seed);
		final TypeInformation<?> sourceOutputType = sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType();
		final TypeInformation<IntermediateSampleData> isdTypeInformation = TypeExtractor.getForClass(IntermediateSampleData.class);
		final UnaryOperatorInformation sipOperatorInformation = new UnaryOperatorInformation(sourceOutputType, isdTypeInformation);
		final MapPartitionOperatorBase sipOperatorBase = new MapPartitionOperatorBase(sampleInPartition, sipOperatorInformation, "Sample in partitions");
		final MapPartitionNode sipNode = new MapPartitionNode(sipOperatorBase);
		final Channel sipChannel = new Channel(sourceNode, TempMode.NONE);
		sipChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		final SingleInputPlanNode sipPlanNode = new SingleInputPlanNode(sipNode, "SampleInPartition PlanNode", sipChannel, DriverStrategy.MAP_PARTITION);
		sipPlanNode.setParallelism(sourceParallelism);
		sipChannel.setTarget(sipPlanNode);
		this.plan.getAllNodes().add(sipPlanNode);
		sourceNewOutputChannels.add(sipChannel);

		// 2. Fixed size sample in a single coordinator.
		final SampleInCoordinator sampleInCoordinator = new SampleInCoordinator(false, sampleSize, seed);
		final UnaryOperatorInformation sicOperatorInformation = new UnaryOperatorInformation(isdTypeInformation, sourceOutputType);
		final GroupReduceOperatorBase sicOperatorBase = new GroupReduceOperatorBase(sampleInCoordinator, sicOperatorInformation, "Sample in coordinator");
		final GroupReduceNode sicNode = new GroupReduceNode(sicOperatorBase);
		final Channel sicChannel = new Channel(sipPlanNode, TempMode.NONE);
		sicChannel.setShipStrategy(ShipStrategyType.PARTITION_HASH, channel.getShipStrategyKeys(), channel.getShipStrategySortOrder(), null, channel.getDataExchangeMode());
		final SingleInputPlanNode sicPlanNode = new SingleInputPlanNode(sicNode, "SampleInCoordinator PlanNode", sicChannel, DriverStrategy.ALL_GROUP_REDUCE);
		sicPlanNode.setParallelism(1);
		sicChannel.setTarget(sicPlanNode);
		sipPlanNode.addOutgoingChannel(sicChannel);
		this.plan.getAllNodes().add(sicPlanNode);

		// 3. Use sampled data to build range boundaries.
		final RangeBoundaryBuilder rangeBoundaryBuilder = new RangeBoundaryBuilder(comparator, targetParallelism);
		final TypeInformation<CommonRangeBoundaries> rbTypeInformation = TypeExtractor.getForClass(CommonRangeBoundaries.class);
		final UnaryOperatorInformation rbOperatorInformation = new UnaryOperatorInformation(sourceOutputType, rbTypeInformation);
		final MapPartitionOperatorBase rbOperatorBase = new MapPartitionOperatorBase(rangeBoundaryBuilder, rbOperatorInformation, "RangeBoundaryBuilder");
		final MapPartitionNode rbNode = new MapPartitionNode(rbOperatorBase);
		final Channel rbChannel = new Channel(sicPlanNode, TempMode.NONE);
		rbChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		final SingleInputPlanNode rbPlanNode = new SingleInputPlanNode(rbNode, "RangeBoundary PlanNode", rbChannel, DriverStrategy.MAP_PARTITION);
		rbPlanNode.setParallelism(1);
		rbChannel.setTarget(rbPlanNode);
		sicPlanNode.addOutgoingChannel(rbChannel);
		this.plan.getAllNodes().add(rbPlanNode);

		// 4. Take range boundaries as broadcast input and take the tuple of partition id and record as output.
		final AssignRangeIndex assignRangeIndex = new AssignRangeIndex(comparator);
		final TypeInformation<Tuple2> ariOutputTypeInformation = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, sourceOutputType);
		final UnaryOperatorInformation ariOperatorInformation = new UnaryOperatorInformation(sourceOutputType, ariOutputTypeInformation);
		final MapPartitionOperatorBase ariOperatorBase = new MapPartitionOperatorBase(assignRangeIndex, ariOperatorInformation, "Assign Range Index");
		final MapPartitionNode ariNode = new MapPartitionNode(ariOperatorBase);
		final Channel ariChannel = new Channel(sourceNode, TempMode.NONE);
		ariChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		final SingleInputPlanNode ariPlanNode = new SingleInputPlanNode(ariNode, "AssignRangeIndex PlanNode", ariChannel, DriverStrategy.MAP_PARTITION);
		ariPlanNode.setParallelism(sourceParallelism);
		ariChannel.setTarget(ariPlanNode);
		this.plan.getAllNodes().add(ariPlanNode);
		sourceNewOutputChannels.add(ariChannel);

		final NamedChannel broadcastChannel = new NamedChannel("RangeBoundaries", rbPlanNode);
		broadcastChannel.setShipStrategy(ShipStrategyType.BROADCAST, channel.getDataExchangeMode());
		broadcastChannel.setTarget(ariPlanNode);
		List<NamedChannel> broadcastChannels = new ArrayList<>(1);
		broadcastChannels.add(broadcastChannel);
		ariPlanNode.setBroadcastInputs(broadcastChannels);

		// 6. Remove the partition id.
		final Channel partChannel = new Channel(ariPlanNode, channel.getTempMode());
		partChannel.setDataExchangeMode(channel.getDataExchangeMode());
		final FieldList keys = new FieldList(0);
		final boolean[] sortDirection = { true };
		partChannel.setShipStrategy(channel.getShipStrategy(), keys, sortDirection, channel.getPartitioner(), channel.getDataExchangeMode());
		ariPlanNode.addOutgoingChannel(channel);
		partChannel.setLocalStrategy(channel.getLocalStrategy(), keys, sortDirection);
		this.plan.getAllNodes().remove(targetNode);

		final PartitionIDRemoveWrapper partitionIDRemoveWrapper = new PartitionIDRemoveWrapper();
		final UnaryOperatorInformation prOperatorInformation = new UnaryOperatorInformation(ariOutputTypeInformation, sourceOutputType);
		final MapOperatorBase prOperatorBase = new MapOperatorBase(partitionIDRemoveWrapper, prOperatorInformation, "PartitionID Remover");
		final MapNode prRemoverNode = new MapNode(prOperatorBase);
		final SingleInputPlanNode prPlanNode = new SingleInputPlanNode(prRemoverNode, "PartitionIDRemover", partChannel, DriverStrategy.MAP);
		partChannel.setTarget(prPlanNode);
		prPlanNode.setParallelism(targetParallelism);
		this.plan.getAllNodes().add(prPlanNode);

		final List<Channel> outgoingChannels = targetNode.getOutgoingChannels();
		for (Channel outgoingChannel : outgoingChannels) {
			outgoingChannel.setSource(prPlanNode);
			prPlanNode.addOutgoingChannel(outgoingChannel);
		}

		return sourceNewOutputChannels;
	}
}
