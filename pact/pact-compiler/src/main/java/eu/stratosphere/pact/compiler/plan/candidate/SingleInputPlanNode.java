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
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;


/**
 *
 *
 * @author Stephan Ewen
 */
public abstract class SingleInputPlanNode extends PlanNode
{
	protected final Channel input;
	
	// --------------------------------------------------------------------------------------------
	
	protected SingleInputPlanNode(Channel input) {
		this.input = input;
	}

	// --------------------------------------------------------------------------------------------
	
	/**
	 * This function overrides the standard behavior of computing costs in the {@link eu.stratosphere.pact.compiler.plan.candidate.PlanNode}.
	 * It adds a check for correct branch handling.
	 * 
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#setCosts(eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void setCosts(Costs nodeCosts) {
		super.setCosts(nodeCosts);
		// check, if this node has no branch beneath it, no double-counted cost then
		if (this.lastJoinedBranchNode != null) {
			throw new CompilerException("SingleInputPlanNode should not have a branch node");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		if (visitor.preVisit(this)) {
			this.input.getSource().accept(visitor);
			visitor.postVisit(this);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		return new Iterator<PlanNode>() {
			private boolean hasLeft = true;
			@Override
			public boolean hasNext() {
				return this.hasLeft;
			}
			@Override
			public PlanNode next() {
				if (this.hasLeft) {
					this.hasLeft = false;
					return SingleInputPlanNode.this.input.getSource();
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
