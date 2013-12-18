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
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.Collections;
import java.util.Iterator;

import eu.stratosphere.compiler.costs.Costs;
import eu.stratosphere.compiler.dag.BulkPartialSolutionNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * Plan candidate node for partial solution of a bulk iteration.
 */
public class BulkPartialSolutionPlanNode extends PlanNode {
	
	private static final Costs NO_COSTS = new Costs();
	
	private BulkIterationPlanNode containingIterationNode;
	
	public Object postPassHelper;
	
	
	public BulkPartialSolutionPlanNode(BulkPartialSolutionNode template, String nodeName, GlobalProperties gProps, LocalProperties lProps) {
		super(template, nodeName, DriverStrategy.NONE);
		
		this.globalProps = gProps;
		this.localProps = lProps;
		
		// the partial solution does not cost anything
		this.nodeCosts = NO_COSTS;
		this.cumulativeCosts = NO_COSTS;
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
	public Iterator<PlanNode> getPredecessors() {
		return Collections.<PlanNode>emptyList().iterator();
	}

	@Override
	public Iterator<Channel> getInputs() {
		return Collections.<Channel>emptyList().iterator();
	}

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		if (source == this) {
			return FOUND_SOURCE;
		} else {
			return NOT_FOUND;
		}
	}
}
