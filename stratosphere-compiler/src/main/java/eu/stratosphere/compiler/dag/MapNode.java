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

package eu.stratosphere.compiler.dag;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.compiler.operators.MapDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;

/**
 * The optimizer's internal representation of a <i>Map</i> contract node.
 */
public class MapNode extends SingleInputNode {
	
	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract The map contract object.
	 */
	public MapNode(MapOperatorBase<?> pactContract) {
		super(pactContract);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
	public MapOperatorBase<?> getPactContract() {
		return (MapOperatorBase<?>) super.getPactContract();
	}

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

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new MapDescriptor());
	}
}
