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


package org.apache.flink.optimizer.dag;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.util.Preconditions;

/**
 * The optimizer's internal representation of a <i>Partition</i> operator node.
 */
public class PartitionNode extends SingleInputNode {

	private final List<OperatorDescriptorSingle> possibleProperties;
	
	public PartitionNode(PartitionOperatorBase<?> operator) {
		super(operator);
		
		OperatorDescriptorSingle descr = new PartitionDescriptor(
					this.getOperator().getPartitionMethod(), this.keys, operator.getOrdering(), operator.getCustomPartitioner(),
					operator.getDistribution());
		this.possibleProperties = Collections.singletonList(descr);
	}

	@Override
	public PartitionOperatorBase<?> getOperator() {
		return (PartitionOperatorBase<?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "Partition";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.possibleProperties;
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// partitioning does not change the number of records
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}
	
	@Override
	public SemanticProperties getSemanticProperties() {
		return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class PartitionDescriptor extends OperatorDescriptorSingle {

		private final PartitionMethod pMethod;
		private final Partitioner<?> customPartitioner;
		private final DataDistribution distribution;
		private final Ordering ordering;

		public PartitionDescriptor(PartitionMethod pMethod, FieldSet pKeys, Ordering ordering, Partitioner<?>
				customPartitioner, DataDistribution distribution) {
			super(pKeys);

			Preconditions.checkArgument(pMethod != PartitionMethod.RANGE
					|| pKeys.equals(new FieldSet(ordering.getFieldPositions())),
					"Partition keys must match the given ordering.");

			this.pMethod = pMethod;
			this.customPartitioner = customPartitioner;
			this.distribution = distribution;
			this.ordering = ordering;
		}
		
		@Override
		public DriverStrategy getStrategy() {
			return DriverStrategy.UNARY_NO_OP;
		}

		@Override
		public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
			return new SingleInputPlanNode(node, "Partition", in, DriverStrategy.UNARY_NO_OP);
		}

		@Override
		protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
			RequestedGlobalProperties rgps = new RequestedGlobalProperties();
			
			switch (this.pMethod) {
			case HASH:
				rgps.setHashPartitioned(this.keys);
				break;
			case REBALANCE:
				rgps.setForceRebalancing();
				break;
			case CUSTOM:
				rgps.setCustomPartitioned(this.keys, this.customPartitioner);
				break;
			case RANGE:
				rgps.setRangePartitioned(ordering, distribution);
				break;
			default:
				throw new IllegalArgumentException("Invalid partition method");
			}
			
			return Collections.singletonList(rgps);
		}

		@Override
		protected List<RequestedLocalProperties> createPossibleLocalProperties() {
			// partitioning does not require any local property.
			return Collections.singletonList(new RequestedLocalProperties());
		}
		
		@Override
		public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
			// the partition node is a no-operation operation, such that all global properties are preserved.
			return gProps;
		}
		
		@Override
		public LocalProperties computeLocalProperties(LocalProperties lProps) {
			// the partition node is a no-operation operation, such that all global properties are preserved.
			return lProps;
		}
	}
}
