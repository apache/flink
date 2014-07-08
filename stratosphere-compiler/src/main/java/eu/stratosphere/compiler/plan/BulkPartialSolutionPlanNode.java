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

package eu.stratosphere.compiler.plan;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.Collections;
import java.util.HashMap;

import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.dag.BulkPartialSolutionNode;
import eu.stratosphere.compiler.dag.OptimizerNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * Plan candidate node for partial solution of a bulk iteration.
 */
public class BulkPartialSolutionPlanNode extends PlanNode {
	
	private static final Costs NO_COSTS = new Costs();
	
	private BulkIterationPlanNode containingIterationNode;
	
	private Channel initialInput;
	
	public Object postPassHelper;
	
	
	public BulkPartialSolutionPlanNode(BulkPartialSolutionNode template, String nodeName,
			GlobalProperties gProps, LocalProperties lProps,
			Channel initialInput)
	{
		super(template, nodeName, DriverStrategy.NONE);
		
		this.globalProps = gProps;
		this.localProps = lProps;
		this.initialInput = initialInput;
		
		// the partial solution does not cost anything
		this.nodeCosts = NO_COSTS;
		this.cumulativeCosts = NO_COSTS;
		
		if (initialInput.getSource().branchPlan != null && initialInput.getSource().branchPlan.size() > 0) {
			if (this.branchPlan == null) {
				this.branchPlan = new HashMap<OptimizerNode, PlanNode>();
			}
			
			this.branchPlan.putAll(initialInput.getSource().branchPlan);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public BulkPartialSolutionNode getPartialSolutionNode() {
		return (BulkPartialSolutionNode) this.template;
	}
	
	public BulkIterationPlanNode getContainingIterationNode() {
		return this.containingIterationNode;
	}
	
	public void setContainingIterationNode(BulkIterationPlanNode containingIterationNode) {
		this.containingIterationNode = containingIterationNode;
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}

	@Override
	public Iterable<PlanNode> getPredecessors() {
		return Collections.<PlanNode>emptyList();
	}

	@Override
	public Iterable<Channel> getInputs() {
		return Collections.<Channel>emptyList();
	}

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		}
		SourceAndDamReport res = this.initialInput.getSource().hasDamOnPathDownTo(source);
		if (res == FOUND_SOURCE_AND_DAM) {
			return FOUND_SOURCE_AND_DAM;
		}
		else if (res == FOUND_SOURCE) {
			return (this.initialInput.getLocalStrategy().dams() || 
					this.initialInput.getTempMode().breaksPipeline() ||
					getDriverStrategy().firstDam() == DamBehavior.FULL_DAM) ?
				FOUND_SOURCE_AND_DAM : FOUND_SOURCE;
		}
		else {
			return NOT_FOUND;
		}
	}
}
