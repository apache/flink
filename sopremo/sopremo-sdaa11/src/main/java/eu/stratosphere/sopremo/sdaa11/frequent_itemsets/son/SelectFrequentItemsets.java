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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.sdaa11.util.JsonNodePool;
import eu.stratosphere.sopremo.sdaa11.util.JsonNodePool.TextNodeFactory;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class SelectFrequentItemsets extends
		ElementaryOperator<SelectFrequentItemsets> {

	private static final long serialVersionUID = 7424999307415466657L;
	
	private int minSupport = 0;
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions(int)
	 */
	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(int inputIndex) {
		if (inputIndex != 0) {
			throw new IllegalArgumentException("Illegal input index: "+inputIndex);
		}
		return Arrays.asList(new ObjectAccess(FrequentItemsetNodes.ITEMS));
	}

	/**
	 * Returns the minSupport.
	 * 
	 * @return the minSupport
	 */
	public int getMinSupport() {
		return minSupport;
	}

	/**
	 * Sets the minSupport to the specified value.
	 *
	 * @param minSupport the minSupport to set
	 */
	public void setMinSupport(int minSupport) {
		this.minSupport = minSupport;
	}
	
	public static class Implementation extends SopremoReduce {
		
		int minSupport;
		
		private ObjectNode fisNode = new ObjectNode();
		private IntNode supportNode = new IntNode();
		private IArrayNode itemsNode = new ArrayNode();
		private JsonNodePool<TextNode> itemNodePool = new JsonNodePool<TextNode>(new TextNodeFactory());

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void reduce(IArrayNode values, JsonCollector out) {
			int support = values.size();
			if (support >= minSupport) {
				ObjectNode value = (ObjectNode) values.get(0);
				supportNode.setValue(support);
				itemsNode = FrequentItemsetNodes.getItems(value);
				FrequentItemsetNodes.write(fisNode, itemsNode, supportNode);
				out.collect(fisNode);
			}
		}
		
	}

}
