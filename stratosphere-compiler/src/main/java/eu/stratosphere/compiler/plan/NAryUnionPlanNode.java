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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.compiler.dag.BinaryUnionNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * 
 */
public class NAryUnionPlanNode extends PlanNode {
	
	private final List<Channel> inputs;
	
	/**
	 * @param template
	 */
	public NAryUnionPlanNode(BinaryUnionNode template, List<Channel> inputs, GlobalProperties gProps) {
		super(template, "Union", DriverStrategy.NONE);
		
		this.inputs = inputs;
		this.globalProps = gProps;
		this.localProps = new LocalProperties();
	}

	@Override
	public void accept(Visitor<PlanNode> visitor) {
		visitor.preVisit(this);
		for (Channel c : this.inputs) {
			c.getSource().accept(visitor);
		}
		visitor.postVisit(this);
	}
	
	public List<Channel> getListOfInputs() {
		return this.inputs;
	}

	@Override
	public Iterator<Channel> getInputs() {
		return Collections.unmodifiableList(this.inputs).iterator();
	}

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

	@Override
	public SourceAndDamReport hasDamOnPathDownTo(PlanNode source) {
		throw new UnsupportedOperationException();
	}
}
