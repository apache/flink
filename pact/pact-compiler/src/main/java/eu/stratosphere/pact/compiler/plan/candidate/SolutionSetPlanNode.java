/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.plan.candidate;

import static eu.stratosphere.pact.compiler.plan.candidate.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.pact.compiler.plan.candidate.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.Collections;
import java.util.Iterator;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.SolutionSetNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * Plan candidate node for partial solution of a bulk iteration.
 */
public class SolutionSetPlanNode extends PlanNode
{
	private WorksetIterationPlanNode containingIterationNode;
	
	public Object postPassHelper;
	
	
	public SolutionSetPlanNode(SolutionSetNode template, GlobalProperties gProps, LocalProperties lProps) {
		super(template, DriverStrategy.NONE);
		
		this.globalProps = gProps;
		this.localProps = lProps;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public SolutionSetNode getSolutionSetNode() {
		return (SolutionSetNode) this.template;
	}
	
	public WorksetIterationPlanNode getContainingIterationNode() {
		return this.containingIterationNode;
	}
	
	public void setContainingIterationNode(WorksetIterationPlanNode containingIterationNode) {
		this.containingIterationNode = containingIterationNode;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			visitor.postVisit(this);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		return Collections.<PlanNode>emptyList().iterator();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getInputs()
	 */
	@Override
	public Iterator<Channel> getInputs() {
		return Collections.<Channel>emptyList().iterator();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#hasDamOnPathDownTo(eu.stratosphere.pact.compiler.plan.candidate.PlanNode)
	 */
	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		} else {
			return NOT_FOUND;
		}
	}
}
