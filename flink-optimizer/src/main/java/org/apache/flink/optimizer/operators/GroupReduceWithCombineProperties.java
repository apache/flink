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

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.costs.Costs;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.PartitionNode;
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

public final class GroupReduceWithCombineProperties extends OperatorDescriptorSingle {
	private static final Logger LOG = LoggerFactory.getLogger(GroupReduceWithCombineProperties.class);
	
	private final Ordering ordering;		// ordering that we need to use if an additional ordering is requested 
	
	private final Partitioner<?> customPartitioner;
	
	
	public GroupReduceWithCombineProperties(FieldSet groupKeys) {
		this(groupKeys, null, null);
	}
	
	public GroupReduceWithCombineProperties(FieldSet groupKeys, Ordering additionalOrderKeys) {
		this(groupKeys, additionalOrderKeys, null);
	}
	
	public GroupReduceWithCombineProperties(FieldSet groupKeys, Partitioner<?> customPartitioner) {
		this(groupKeys, null, customPartitioner);
	}
	
	public GroupReduceWithCombineProperties(FieldSet groupKeys, Ordering additionalOrderKeys, Partitioner<?> customPartitioner) {
		super(groupKeys);
		
		// if we have an additional ordering, construct the ordering to have primarily the grouping fields
		if (additionalOrderKeys != null) {
			this.ordering = new Ordering();
			for (Integer key : this.keyList) {
				this.ordering.appendOrdering(key, null, Order.ANY);
			}
		
			// and next the additional order fields
			for (int i = 0; i < additionalOrderKeys.getNumberOfFields(); i++) {
				Integer field = additionalOrderKeys.getFieldNumber(i);
				Order order = additionalOrderKeys.getOrder(i);
				this.ordering.appendOrdering(field, additionalOrderKeys.getType(i), order);
			}
		} else {
			this.ordering = null;
		}
		
		this.customPartitioner = customPartitioner;
	}
	
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.SORTED_GROUP_REDUCE;
	}

	@Override
	public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
		if (in.getShipStrategy() == ShipStrategyType.FORWARD) {
			if(in.getSource().getOptimizerNode() instanceof PartitionNode) {
				LOG.warn("Cannot automatically inject combiner for GroupReduceFunction. Please add an explicit combiner with combineGroup() in front of the partition operator.");
			}
			// adjust a sort (changes grouping, so it must be for this driver to combining sort
			if (in.getLocalStrategy() == LocalStrategy.SORT) {
				if (!in.getLocalStrategyKeys().isValidUnorderedPrefix(this.keys)) {
					throw new RuntimeException("Bug: Inconsistent sort for group strategy.");
				}
				in.setLocalStrategy(LocalStrategy.COMBININGSORT, in.getLocalStrategyKeys(),
									in.getLocalStrategySortOrder());
			}
			return new SingleInputPlanNode(node, "Reduce ("+node.getOperator().getName()+")", in,
											DriverStrategy.SORTED_GROUP_REDUCE, this.keyList);
		} else {
			// non forward case. all local properties are killed anyways, so we can safely plug in a combiner
			Channel toCombiner = new Channel(in.getSource());
			toCombiner.setShipStrategy(ShipStrategyType.FORWARD, DataExchangeMode.PIPELINED);

			// create an input node for combine with same parallelism as input node
			GroupReduceNode combinerNode = ((GroupReduceNode) node).getCombinerUtilityNode();
			combinerNode.setParallelism(in.getSource().getParallelism());

			SingleInputPlanNode combiner = new SingleInputPlanNode(combinerNode, "Combine ("+node.getOperator()
					.getName()+")", toCombiner, DriverStrategy.SORTED_GROUP_COMBINE);
			combiner.setCosts(new Costs(0, 0));
			combiner.initProperties(toCombiner.getGlobalProperties(), toCombiner.getLocalProperties());
			// set sorting comparator key info
			combiner.setDriverKeyInfo(in.getLocalStrategyKeys(), in.getLocalStrategySortOrder(), 0);
			// set grouping comparator key info
			combiner.setDriverKeyInfo(this.keyList, 1);
			
			Channel toReducer = new Channel(combiner);
			toReducer.setShipStrategy(in.getShipStrategy(), in.getShipStrategyKeys(),
									in.getShipStrategySortOrder(), in.getDataExchangeMode());
			if (in.getShipStrategy() == ShipStrategyType.PARTITION_RANGE) {
				toReducer.setDataDistribution(in.getDataDistribution());
			}
			toReducer.setLocalStrategy(LocalStrategy.COMBININGSORT, in.getLocalStrategyKeys(),
										in.getLocalStrategySortOrder());

			return new SingleInputPlanNode(node, "Reduce ("+node.getOperator().getName()+")",
											toReducer, DriverStrategy.SORTED_GROUP_REDUCE, this.keyList);
		}
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
		if (this.ordering == null) {
			props.setGroupedFields(this.keys);
		} else {
			props.setOrdering(this.ordering);
		}
		return Collections.singletonList(props);
	}

	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
		if (gProps.getUniqueFieldCombination() != null && gProps.getUniqueFieldCombination().size() > 0 &&
				gProps.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED)
		{
			gProps.setAnyPartitioning(gProps.getUniqueFieldCombination().iterator().next().toFieldList());
		}
		gProps.clearUniqueFieldCombinations();
		return gProps;
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties lProps) {
		return lProps.clearUniqueFieldSets();
	}
}
