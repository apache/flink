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

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.base.SortPartitionOperatorBase;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

import java.util.Collections;
import java.util.List;

/**
 * The optimizer's internal representation of a <i>SortPartition</i> operator node.
 */
public class SortPartitionNode extends SingleInputNode {

	private final List<OperatorDescriptorSingle> possibleProperties;

	public SortPartitionNode(SortPartitionOperatorBase<?> operator) {
		super(operator);
		
		OperatorDescriptorSingle descr = new SortPartitionDescriptor(operator.getPartitionOrdering());
		this.possibleProperties = Collections.singletonList(descr);
	}

	@Override
	public SortPartitionOperatorBase<?> getOperator() {
		return (SortPartitionOperatorBase<?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "Sort-Partition";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.possibleProperties;
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// sorting does not change the number of records
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		this.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
	}
	
	@Override
	public SemanticProperties getSemanticProperties() {
		return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class SortPartitionDescriptor extends OperatorDescriptorSingle {

		private Ordering partitionOrder;

		public SortPartitionDescriptor(Ordering partitionOrder) {
			this.partitionOrder = partitionOrder;
		}
		
		@Override
		public DriverStrategy getStrategy() {
			return DriverStrategy.UNARY_NO_OP;
		}

		@Override
		public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
			return new SingleInputPlanNode(node, "Sort-Partition", in, DriverStrategy.UNARY_NO_OP);
		}

		@Override
		protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
			// sort partition does not require any global property
			return Collections.singletonList(new RequestedGlobalProperties());
		}

		@Override
		protected List<RequestedLocalProperties> createPossibleLocalProperties() {
			// set partition order as required local property
			RequestedLocalProperties rlp = new RequestedLocalProperties();
			rlp.setOrdering(this.partitionOrder);

			return Collections.singletonList(rlp);
		}
		
		@Override
		public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
			// sort partition is a no-operation operation, such that all global properties are preserved.
			return gProps;
		}
		
		@Override
		public LocalProperties computeLocalProperties(LocalProperties lProps) {
			// sort partition is a no-operation operation, such that all global properties are preserved.
			return lProps;
		}
	}
}
