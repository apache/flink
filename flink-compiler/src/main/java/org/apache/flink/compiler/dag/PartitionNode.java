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


package org.apache.flink.compiler.dag;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.operators.base.PartitionOperatorBase;
import org.apache.flink.api.common.operators.base.PartitionOperatorBase.PartitionMethod;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.dataproperties.GlobalProperties;
import org.apache.flink.compiler.dataproperties.LocalProperties;
import org.apache.flink.compiler.dataproperties.RequestedGlobalProperties;
import org.apache.flink.compiler.dataproperties.RequestedLocalProperties;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

/**
 * The optimizer's internal representation of a <i>Partition</i> operator node.
 */
public class PartitionNode extends SingleInputNode {

	public PartitionNode(PartitionOperatorBase<?> operator) {
		super(operator);
	}

	@Override
	public PartitionOperatorBase<?> getPactContract() {
		return (PartitionOperatorBase<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Partition";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new PartitionDescriptor(this.getPactContract().getPartitionMethod(), this.keys));
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// partitioning does not change the number of records
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
	}
	
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		// Partition does not change any data
		return true;
	}
	
	
	public static class PartitionDescriptor extends OperatorDescriptorSingle {

		private final PartitionMethod pMethod;
		private final FieldSet pKeys;
		
		public PartitionDescriptor(PartitionMethod pMethod, FieldSet pKeys) {
			this.pMethod = pMethod;
			this.pKeys = pKeys;
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
				rgps.setHashPartitioned(pKeys.toFieldList());
				break;
			case REBALANCE:
				rgps.setForceRebalancing();
				break;
			case RANGE:
				throw new UnsupportedOperationException("Not yet supported");
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
