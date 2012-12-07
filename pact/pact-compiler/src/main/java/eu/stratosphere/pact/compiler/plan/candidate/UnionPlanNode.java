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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.UnionNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * 
 */
public class UnionPlanNode extends PlanNode
{
	private final ArrayList<Channel> inputs;
	
	/**
	 * @param template
	 */
	public UnionPlanNode(UnionNode template) {
		super(template, DriverStrategy.NONE);
		
		this.inputs = new ArrayList<Channel>(4);
		
		this.globalProps = new GlobalProperties();
		this.localProps = new LocalProperties();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Visitable#accept(eu.stratosphere.pact.common.plan.Visitor)
	 */
	@Override
	public void accept(Visitor<PlanNode> visitor) {
		visitor.preVisit(this);
		for (Channel c : this.inputs) {
			c.getSource().accept(visitor);
		}
		visitor.postVisit(this);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getInputs()
	 */
	@Override
	public Iterator<Channel> getInputs() {
		return Collections.unmodifiableList(this.inputs).iterator();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.candidate.PlanNode#getPredecessors()
	 */
	@Override
	public Iterator<PlanNode> getPredecessors() {
		final Iterator<Channel> channels = this.inputs.iterator();
		return new Iterator<PlanNode>() {

			@Override
			public boolean hasNext() {
				return channels.hasNext();
			}

			@Override
			public PlanNode next() {
				return channels.next().getSource();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
