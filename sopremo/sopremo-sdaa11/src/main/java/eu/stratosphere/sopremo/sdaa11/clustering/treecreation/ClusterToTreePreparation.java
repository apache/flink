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
package eu.stratosphere.sopremo.sdaa11.clustering.treecreation;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * This operator simply takes clusters, strips off all of its points
 * except for the clustroid and additionally adds a dummy reduce key.
 * 
 * @see TreeCreator
 * @see TreeAssembler
 * @author skruse
 *
 */
public class ClusterToTreePreparation extends ElementaryOperator<ClusterToTreePreparation> {
	
	private static final long serialVersionUID = -5035298968776097883L;

	public static class Implementation extends SopremoMap {

		ObjectNode outputNode = new ObjectNode();
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(IJsonNode value, JsonCollector out) {
			ObjectNode clusterNode = (ObjectNode) value;
			
			// TODO: check whether it is better to just modify the incoming node
			outputNode.put(TreeAssembler.DUMMY_KEY, TreeAssembler.DUMMY_NODE);
			outputNode.put("id", clusterNode.get("id"));
			outputNode.put("clustroid", clusterNode.get("clustroid"));
			
			out.collect(outputNode);
		}
		
	}

}
