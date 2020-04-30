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

import org.apache.flink.api.common.operators.base.DeltaIterationBase.WorksetPlaceHolder;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.WorksetPlanNode;

/**
 * The optimizer's internal representation of the partial solution that is input to a bulk iteration.
 */
public class WorksetNode extends AbstractPartialSolutionNode {
	
	private final WorksetIterationNode iterationNode;
	
	
	public WorksetNode(WorksetPlaceHolder<?> psph, WorksetIterationNode iterationNode) {
		super(psph);
		this.iterationNode = iterationNode;
	}

	// --------------------------------------------------------------------------------------------
	
	public void setCandidateProperties(GlobalProperties gProps, LocalProperties lProps, Channel initialInput) {
		if (this.cachedPlans != null) {
			throw new IllegalStateException();
		} else {
			WorksetPlanNode wspn = new WorksetPlanNode(this, "Workset ("+this.getOperator().getName()+")", gProps, lProps, initialInput);
			this.cachedPlans = Collections.<PlanNode>singletonList(wspn);
		}
	}
	
	public WorksetPlanNode getCurrentWorksetPlanNode() {
		if (this.cachedPlans != null) {
			return (WorksetPlanNode) this.cachedPlans.get(0);
		} else {
			throw new IllegalStateException();
		}
	}
	
	public WorksetIterationNode getIterationNode() {
		return this.iterationNode;
	}
	
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		copyEstimates(this.iterationNode.getInitialWorksetPredecessorNode());
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract object for this data source node.
	 * 
	 * @return The contract.
	 */
	@Override
	public WorksetPlaceHolder<?> getOperator() {
		return (WorksetPlaceHolder<?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "Workset";
	}
	
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		DagConnection worksetInput = this.iterationNode.getSecondIncomingConnection();
		OptimizerNode worksetSource = worksetInput.getSource();
		
		addClosedBranches(worksetSource.closedBranchingNodes);
		List<UnclosedBranchDescriptor> fromInput = worksetSource.getBranchesForParent(worksetInput);
		this.openBranches = (fromInput == null || fromInput.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : fromInput;
	}
}
