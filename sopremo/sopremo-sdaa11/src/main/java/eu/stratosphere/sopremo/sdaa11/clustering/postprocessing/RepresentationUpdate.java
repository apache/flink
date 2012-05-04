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
package eu.stratosphere.sopremo.sdaa11.clustering.postprocessing;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.sdaa11.clustering.json.JsonPoints;
import eu.stratosphere.sopremo.sdaa11.util.Ranking;
import eu.stratosphere.sopremo.sdaa11.util.ReverseRanking;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author skruse
 * 
 */
public class RepresentationUpdate extends
		ElementaryOperator<RepresentationUpdate> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5470882666744483986L;

	private final int representationCount = 10;

	/**
	 * Returns the representationCount.
	 * 
	 * @return the representationCount
	 */
	public int getRepresentationCount() {
		return this.representationCount;
	}

	public static class Implementation extends SopremoCoGroup {

		private final int representationCount = 10;

		private final Ranking<IJsonNode> nearestPoints = new Ranking<IJsonNode>(
				this.representationCount, IJsonNode.class);
		private final Ranking<IJsonNode> furthestPoints = new ReverseRanking<IJsonNode>(
				this.representationCount, IJsonNode.class);

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere
		 * .sopremo.type.IArrayNode, eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(final IArrayNode representationsNode,
				final IArrayNode pointsNode, final JsonCollector out) {
			nearestPoints.clear();
			furthestPoints.clear();
			
			if (representationsNode.size() != 1) {
				throw new IllegalStateException("Unexpected number of representations: "+representationsNode.size())
			}
			ObjectNode representationNode = (ObjectNode) representationsNode.get(0);
			
			for (IJsonNode memberNode : pointsNode) {
				ObjectNode pointNode = (ObjectNode) memberNode;
				JsonPoints.getValues(pointNode);
			}
				
		}

	}

}
