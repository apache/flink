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

import java.util.Collections;
import java.util.Iterator;

import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.DataSourceNode;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

import static eu.stratosphere.pact.compiler.plan.candidate.PlanNode.SourceAndDamReport.*;

/**
 * Plan candidate node for data flow sources that have no input and no special strategies.
 */
public class SourcePlanNode extends PlanNode
{
	private TypeSerializerFactory<?> serializer;
	
	/**
	 * Constructs a new source candidate node that uses <i>NONE</i> as its local strategy.
	 * 
	 * @param template The template optimizer node that this candidate is created for.
	 */
	public SourcePlanNode(DataSourceNode template) {
		super(template, DriverStrategy.NONE);
		
		this.globalProps = new GlobalProperties();
		this.localProps = new LocalProperties();
		updatePropertiesWithUniqueSets(template.getUniqueFields());
	}
	
	// --------------------------------------------------------------------------------------------
	
	public DataSourceNode getDataSourceNode() {
		return (DataSourceNode) this.template;
	}
	
	/**
	 * Gets the serializer from this PlanNode.
	 *
	 * @return The serializer.
	 */
	public TypeSerializerFactory<?> getSerializer() {
		return serializer;
	}
	
	/**
	 * Sets the serializer for this PlanNode.
	 *
	 * @param serializer The serializer to set.
	 */
	public void setSerializer(TypeSerializerFactory<?> serializer) {
		this.serializer = serializer;
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
