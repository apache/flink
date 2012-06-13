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

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class PointNodes {

	public static final String CLUSTER_ID = "c_id";
	public static final String ID = "id";
	public static final String VALUES = "values";
	public static final String ROWSUM = "rowsum";

	public static void write(final ObjectNode pointNode, final TextNode idNode,
			final IArrayNode valuesNode, final IntNode rowsumNode) {
		pointNode.clear();
		pointNode.put(ID, idNode);
		pointNode.put(VALUES, valuesNode);
		pointNode.put(ROWSUM, rowsumNode);
	}

	public static void assignCluster(final ObjectNode pointNode,
			final TextNode clusterIdNode) {
		pointNode.put(CLUSTER_ID, clusterIdNode);
	}

	public static IArrayNode getValues(final ObjectNode pointNode) {
		return (ArrayNode) pointNode.get(VALUES);
	}

	public static TextNode getId(final ObjectNode pointNode) {
		return (TextNode) pointNode.get(ID);
	}

	public static IntNode getRowsum(final ObjectNode pointNode) {
		return (IntNode) pointNode.get(ROWSUM);
	}

	public static TextNode getAssignedCluster(final ObjectNode pointNode) {
		return (TextNode) pointNode.get(CLUSTER_ID);
	}

}
