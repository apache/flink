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

import java.util.Iterator;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;


/**
 *
 *
 * @author Stephan Ewen
 */
public abstract class DualInputPlanNode extends PlanNode
{
	protected final Channel input1 = null;
	protected final Channel input2 = null;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.candidate.PlanNode}.
	 * Since nodes with multiple inputs may join branched plans, care must be taken not to double-count the costs of the subtree rooted
	 * at the last unjoined branch.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode == null) {
			return;
		}

		// TODO: Check this!
		// get the cumulative costs of the last joined branching node
		PlanNode lastCommonChild = this.input1.getSource().branchPlan.get(this.lastJoinedBranchNode);
		Costs douleCounted = lastCommonChild.getCumulativeCosts();
		getCumulativeCosts().subtractCosts(douleCounted);
		
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input1.getSource().accept(visitor);
			this.input2.getSource().accept(visitor);
			visitor.postVisit(this);
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		return new Iterator<PlanNode>() {
			private int hasLeft = 2;
			@Override
			public boolean hasNext() {
				return this.hasLeft > 0;
			}
			@Override
			public PlanNode next() {
				if (this.hasLeft == 2) {
					this.hasLeft = 1;
					return DualInputPlanNode.this.input1.getSource();
				} else if (this.hasLeft == 1) {
					this.hasLeft = 0;
					return DualInputPlanNode.this.input2.getSource();
				} else
					throw new NoSuchElementException();
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
