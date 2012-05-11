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
package eu.stratosphere.sopremo.sdaa11.clustering.json;

import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class RepresentationNodes {

	public static final String ID = "id";
	public static final String CLUSTROID = "clstrd";

	public static void write(final ObjectNode clusterNode,
			final TextNode idNode, final ObjectNode clustroidNode) {
		clusterNode.put(ID, idNode).put(CLUSTROID, clustroidNode);
	}

	public static TextNode getId(final ObjectNode clusterNode) {
		return (TextNode) clusterNode.get(ID);
	}

	public static ObjectNode getClustroid(final ObjectNode clusterNode) {
		return (ObjectNode) clusterNode.get(CLUSTROID);
	}

}
