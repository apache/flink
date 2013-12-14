/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.plan;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.operators.base.GenericMapContract;
import eu.stratosphere.pact.compiler.operators.MapDescriptor;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle;

/**
 * The optimizer's internal representation of a <i>Map</i> contract node.
 */
public class MapNode extends SingleInputNode {
	
	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract The map contract object.
	 */
	public MapNode(GenericMapContract<?> pactContract) {
		super(pactContract);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericMapContract<?> getPactContract() {
		return (GenericMapContract<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Map";
	}

	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		if (getPredecessorNode() != null)
			return getPredecessorNode().estimatedNumRecords;
		else
			return -1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.SingleInputNode#getPossibleProperties()
	 */
	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new MapDescriptor());
	}
}
