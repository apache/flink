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
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.tree.ClusterTree;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 *
 */
public class PointMapper extends ElementaryOperator<PointMapper> {
	
	private static final long serialVersionUID = -1539853388756701551L;
	
	public static final String SCHEMA_CLUSTER_ID = "clusterId";
	public static final String SCHEMA_POINT = "point";
	
	public static final int TREE_INPUT_INDEX = 0;
	public static final int POINT_INPUT_INDEX = 1;

	public static class Implementation extends SopremoCross {

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void cross(IJsonNode treeNode, IJsonNode pointNode,
				JsonCollector out) {
			ClusterTree tree = new ClusterTree();
			tree.read(treeNode);
			
			Point point = new Point();
			point.read(pointNode);
			
			String clusterId = tree.findIdOfClusterNextTo(point);
			
			ObjectNode taggedPoint = new ObjectNode();
			taggedPoint.put(SCHEMA_CLUSTER_ID, new TextNode(clusterId));
			taggedPoint.put(SCHEMA_POINT, pointNode);
			
			out.collect(taggedPoint);
		}
		
	}

}
