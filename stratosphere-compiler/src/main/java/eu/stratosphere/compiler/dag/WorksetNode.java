/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.dag;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.base.DeltaIterationBase.WorksetPlaceHolder;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.WorksetPlanNode;

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
			WorksetPlanNode wspn = new WorksetPlanNode(this, "Workset ("+this.getPactContract().getName()+")", gProps, lProps, initialInput);
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
	public WorksetPlaceHolder<?> getPactContract() {
		return (WorksetPlaceHolder<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Workset";
	}
	
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		PactConnection worksetInput = this.iterationNode.getSecondIncomingConnection();
		OptimizerNode worksetSource = worksetInput.getSource();
		
		addClosedBranches(worksetSource.closedBranchingNodes);
		List<UnclosedBranchDescriptor> fromInput = worksetSource.getBranchesForParent(worksetInput);
		this.openBranches = (fromInput == null || fromInput.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : fromInput;
	}
}
