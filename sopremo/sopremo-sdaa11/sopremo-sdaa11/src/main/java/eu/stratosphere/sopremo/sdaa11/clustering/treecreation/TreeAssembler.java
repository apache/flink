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

import java.util.Arrays;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.sdaa11.clustering.tree.ClusterTree;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 *
 */
public class TreeAssembler extends ElementaryOperator<TreeAssembler> {
	
	private static final long serialVersionUID = -1439186245691893155L;
	
	public static final IntNode DUMMY_NODE = new IntNode(0);
	
	public static final String DUMMY_KEY = "dummy";
	
	public static final int DEFAULT_TREE_WIDTH = 10;

	/**
	 * Specifies the degree of the created tree.
	 */
	private int treeWidth = DEFAULT_TREE_WIDTH;

	/**
	 * Returns the treeWidth.
	 * 
	 * @return the treeWidth
	 */
	public int getTreeWidth() {
		return treeWidth;
	}

	/**
	 * Sets the treeWidth to the specified value.
	 *
	 * @param treeWidth the treeWidth to set
	 */
	public void setTreeWidth(int treeWidth) {
	
		this.treeWidth = treeWidth;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions()
	 */
	@Override
	public Iterable<? extends EvaluationExpression> getKeyExpressions() {
		return Arrays.asList(new ObjectAccess(DUMMY_KEY));
	}

	public static class Implementation extends SopremoReduce {

		/**
		 * @see TreeAssembler#treeWidth
		 */
		private int treeWidth;
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void reduce(IArrayNode values, JsonCollector out) {
			ClusterTree tree = new ClusterTree(treeWidth);

			for (IJsonNode value : values) {
				ObjectNode clusterDescriptorNode = (ObjectNode) value;
				String clusterName = ((TextNode) clusterDescriptorNode.get("id")).getJavaValue();
				Point clustroid = new Point();
				clustroid.read(clusterDescriptorNode.get("clustroid"));
				tree.add(clustroid, clusterName);
			}
			
			TextNode serialization = new TextNode(SopremoUtil.objectToString(tree));
			ObjectNode output = new ObjectNode();
			output.put("tree", tree.write(null));
			out.collect(output);		
		}
		
	}
	
}
