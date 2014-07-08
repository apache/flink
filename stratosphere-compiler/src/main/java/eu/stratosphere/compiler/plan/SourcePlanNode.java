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

import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.compiler.dag.DataSourceNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * Plan candidate node for data flow sources that have no input and no special strategies.
 */
public class SourcePlanNode extends PlanNode {
	
	private TypeSerializerFactory<?> serializer;
	
	/**
	 * Constructs a new source candidate node that uses <i>NONE</i> as its local strategy.
	 * 
	 * @param template The template optimizer node that this candidate is created for.
	 */
	public SourcePlanNode(DataSourceNode template, String nodeName) {
		super(template, nodeName, DriverStrategy.NONE);
		
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
		} else {
			return NOT_FOUND;
		}
	}
}
