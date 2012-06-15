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
package eu.stratosphere.sopremo.sdaa11.clustering.main;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.sdaa11.clustering.json.ClusterNodes;
import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Takes whole clusters and separately outputs all of its points with the
 * respective cluster ID assigned.
 * 
 * @author skruse
 * 
 */
@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class ClusterDisassemble extends ElementaryOperator<ClusterDisassemble> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2038524229470187992L;

	public static class Implementation extends SopremoMap {

		private final ObjectNode outputNode = new ObjectNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(final IJsonNode node, final JsonCollector out) {

			System.out.println("Disassembling " + node);
			final ObjectNode clusterNode = (ObjectNode) node;

			final TextNode idNode = ClusterNodes.getId(clusterNode);
			final IArrayNode pointsNode = ClusterNodes.getPoints(clusterNode);

			for (final IJsonNode pointNode : pointsNode) {
				this.outputNode.putAll((IObjectNode) pointNode);
				PointNodes.assignCluster(this.outputNode, idNode);
				out.collect(this.outputNode);
			}

		}

	}

}
