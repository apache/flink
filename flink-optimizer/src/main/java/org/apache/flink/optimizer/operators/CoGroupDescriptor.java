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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;

public class CoGroupDescriptor extends OperatorDescriptorDual {
	
	private final Ordering ordering1;		// ordering on the first input 
	private final Ordering ordering2;		// ordering on the second input 
	
	private Partitioner<?> customPartitioner;
	
	
	public CoGroupDescriptor(FieldList keys1, FieldList keys2) {
		this(keys1, keys2, null, null);
	}
	
	public CoGroupDescriptor(FieldList keys1, FieldList keys2, Ordering additionalOrdering1, Ordering additionalOrdering2) {
		super(keys1, keys2);
		
		// if we have an additional ordering, construct the ordering to have primarily the grouping fields
		if (additionalOrdering1 != null) {
			this.ordering1 = new Ordering();
			for (Integer key : this.keys1) {
				this.ordering1.appendOrdering(key, null, Order.ANY);
			}
		
			// and next the additional order fields
			for (int i = 0; i < additionalOrdering1.getNumberOfFields(); i++) {
				Integer field = additionalOrdering1.getFieldNumber(i);
				Order order = additionalOrdering1.getOrder(i);
				this.ordering1.appendOrdering(field, additionalOrdering1.getType(i), order);
			}
		} else {
			this.ordering1 = Utils.createOrdering(this.keys1);
		}
		
		// if we have an additional ordering, construct the ordering to have primarily the grouping fields
		if (additionalOrdering2 != null) {
			this.ordering2 = new Ordering();
			for (Integer key : this.keys2) {
				this.ordering2.appendOrdering(key, null, Order.ANY);
			}
		
			// and next the additional order fields
			for (int i = 0; i < additionalOrdering2.getNumberOfFields(); i++) {
				Integer field = additionalOrdering2.getFieldNumber(i);
				Order order = additionalOrdering2.getOrder(i);
				this.ordering2.appendOrdering(field, additionalOrdering2.getType(i), order);
			}
		} else {
			this.ordering2 = Utils.createOrdering(this.keys2);
		}
	}
	
	public void setCustomPartitioner(Partitioner<?> customPartitioner) {
		this.customPartitioner = customPartitioner;
	}
	
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.CO_GROUP;
	}

	@Override
	protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {

		if (this.customPartitioner == null) {

			// we accept compatible partitionings of any type
			RequestedGlobalProperties partitioned_left_any = new RequestedGlobalProperties();
			RequestedGlobalProperties partitioned_right_any = new RequestedGlobalProperties();
			partitioned_left_any.setAnyPartitioning(this.keys1);
			partitioned_right_any.setAnyPartitioning(this.keys2);

			// add strict hash partitioning of both inputs on their full key sets
			RequestedGlobalProperties partitioned_left_hash = new RequestedGlobalProperties();
			RequestedGlobalProperties partitioned_right_hash = new RequestedGlobalProperties();
			partitioned_left_hash.setHashPartitioned(this.keys1);
			partitioned_right_hash.setHashPartitioned(this.keys2);

			return Arrays.asList(new GlobalPropertiesPair(partitioned_left_any, partitioned_right_any),
					new GlobalPropertiesPair(partitioned_left_hash, partitioned_right_hash));
		}
		else {
			RequestedGlobalProperties partitioned_left = new RequestedGlobalProperties();
			partitioned_left.setCustomPartitioned(this.keys1, this.customPartitioner);

			RequestedGlobalProperties partitioned_right = new RequestedGlobalProperties();
			partitioned_right.setCustomPartitioned(this.keys2, this.customPartitioner);

			return Collections.singletonList(new GlobalPropertiesPair(partitioned_left, partitioned_right));
		}
	}
	
	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		RequestedLocalProperties sort1 = new RequestedLocalProperties(this.ordering1);
		RequestedLocalProperties sort2 = new RequestedLocalProperties(this.ordering2);
		return Collections.singletonList(new LocalPropertiesPair(sort1, sort2));
	}
	
	@Override
	public boolean areCompatible(RequestedGlobalProperties requested1, RequestedGlobalProperties requested2,
			GlobalProperties produced1, GlobalProperties produced2)
	{

		if(produced1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
				produced2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED) {

			// both are hash partitioned, check that partitioning fields are equivalently chosen
			return checkEquivalentFieldPositionsInKeyFields(
					produced1.getPartitioningFields(), produced2.getPartitioningFields());

		}
		else if(produced1.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
				produced2.getPartitioning() == PartitioningProperty.RANGE_PARTITIONED &&
				produced1.getDataDistribution() != null && produced2.getDataDistribution() != null) {

			return produced1.getPartitioningFields().size() == produced2.getPartitioningFields().size() &&
					checkSameOrdering(produced1, produced2, produced1.getPartitioningFields().size()) &&
					produced1.getDataDistribution().equals(produced2.getDataDistribution());
			
		}
		else if(produced1.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING &&
				produced2.getPartitioning() == PartitioningProperty.CUSTOM_PARTITIONING) {

			// both use a custom partitioner. Check that both keys are exactly as specified and that both the same partitioner
			return produced1.getPartitioningFields().isExactMatch(this.keys1) &&
					produced2.getPartitioningFields().isExactMatch(this.keys2) &&
					produced1.getCustomPartitioner() != null && produced2.getCustomPartitioner() != null &&
					produced1.getCustomPartitioner().equals(produced2.getCustomPartitioner());

		}
		else {

			// no other partitioning valid, incl. ANY_PARTITIONING.
			//   For co-groups we must ensure that both sides are exactly identically partitioned, ANY is not good enough.
			return false;
		}

	}
	
	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2) {
		int numRelevantFields = this.keys1.size();
		return checkSameOrdering(produced1, produced2, numRelevantFields);
	}

	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		boolean[] inputOrders = in1.getLocalProperties().getOrdering() == null ? null : in1.getLocalProperties().getOrdering().getFieldSortDirections();
		
		if (inputOrders == null || inputOrders.length < this.keys1.size()) {
			throw new CompilerException("BUG: The input strategy does not sufficiently describe the sort orders for a CoGroup operator.");
		} else if (inputOrders.length > this.keys1.size()) {
			boolean[] tmp = new boolean[this.keys1.size()];
			System.arraycopy(inputOrders, 0, tmp, 0, tmp.length);
			inputOrders = tmp;
		}
		
		return new DualInputPlanNode(node, "CoGroup ("+node.getOperator().getName()+")", in1, in2, DriverStrategy.CO_GROUP, this.keys1, this.keys2, inputOrders);
	}
	
	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
		GlobalProperties gp = GlobalProperties.combine(in1, in2);
		if (gp.getUniqueFieldCombination() != null && gp.getUniqueFieldCombination().size() > 0 &&
					gp.getPartitioning() == PartitioningProperty.RANDOM_PARTITIONED)
		{
			gp.setAnyPartitioning(gp.getUniqueFieldCombination().iterator().next().toFieldList());
		}
		gp.clearUniqueFieldCombinations();
		return gp;
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		LocalProperties comb = LocalProperties.combine(in1, in2);
		return comb.clearUniqueFieldSets();
	}

}
