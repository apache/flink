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

import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.BasketNodes;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class FindUnaryFrequentItemSets extends
		ElementaryOperator<FindUnaryFrequentItemSets> {

	private static final long serialVersionUID = 1400135657979934016L;

	private int minSupport = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions(int)
	 */
	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(
			final int inputIndex) {
		if (inputIndex != 0)
			throw new IllegalArgumentException("Illegal input index: "
					+ inputIndex);
		return Arrays.asList(new ObjectAccess(AnnotatorNodes.ANNOTATION));
	}

	/**
	 * Returns the minSupport.
	 * 
	 * @return the minSupport
	 */
	public int getMinSupport() {
		return this.minSupport;
	}

	/**
	 * Sets the minSupport to the specified value.
	 * 
	 * @param minSupport
	 *            the minSupport to set
	 */
	public void setMinSupport(final int minSupport) {
		this.minSupport = minSupport;
	}

	public static class Implementation extends SopremoReduce {

		int minSupport;

		private final ObjectNode outputNode = new ObjectNode();
		private final IArrayNode itemsNode = new ArrayNode();
		private final TextNode outputTextNode = new TextNode();
		private final IntNode supportNode = new IntNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoReduce#reduce(eu.stratosphere
		 * .sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void reduce(final IArrayNode values, final JsonCollector out) {
			final Object2IntOpenHashMap<String> counts = new Object2IntOpenHashMap<String>();
			counts.defaultReturnValue(0);
			for (final IJsonNode value : values) {
				final ObjectNode basket = (ObjectNode) AnnotatorNodes
						.getAnnotatee((ObjectNode) value);
				final IArrayNode items = BasketNodes.getItems(basket);
				for (final IJsonNode item : items) {
					final String itemValue = ((TextNode) item).getTextValue();
					final int count = counts.getInt(itemValue);
					counts.put(itemValue, count + 1);
				}
			}

			for (final Entry<String> entry : counts.object2IntEntrySet())
				if (entry.getIntValue() >= this.minSupport) {
					this.outputTextNode.setValue(entry.getKey());
					this.itemsNode.clear();
					this.itemsNode.add(this.outputTextNode);
					this.supportNode.setValue(entry.getIntValue());
					this.outputNode.clear();
					FrequentItemsetNodes.write(this.outputNode, this.itemsNode,
							this.supportNode);
					out.collect(this.outputNode);
				}
		}

	}

}
