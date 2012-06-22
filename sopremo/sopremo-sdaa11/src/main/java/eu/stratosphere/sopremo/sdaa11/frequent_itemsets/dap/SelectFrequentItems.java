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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import eu.stratosphere.sopremo.DegreeOfParallelism;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.BasketNodes;
import eu.stratosphere.sopremo.sdaa11.json.AnnotatorNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
@DegreeOfParallelism(value = 1)
@InputCardinality(value = 1)
@OutputCardinality(value = 1)
public class SelectFrequentItems extends
		ElementaryOperator<SelectFrequentItems> {

	private static final long serialVersionUID = -5564714871881347227L;

	private int minSupport = 1;

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

	public static class Implementation extends SopremoMap {

		int minSupport;

		private final Object2IntMap<String> counts;
		private final ObjectNode outputNode = new ObjectNode();
		private final IArrayNode basketNode = new ArrayNode();
		private final TextNode itemNode = new TextNode();

		public Implementation() {
			this.counts = new Object2IntOpenHashMap<String>();
			this.counts.defaultReturnValue(0);
			this.basketNode.add(this.itemNode);
			FrequentItemsetNodes.write(this.outputNode, this.basketNode);
			AnnotatorNodes.flatAnnotate(this.outputNode);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoMap#map(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void map(final IJsonNode value, final JsonCollector out) {
			final ObjectNode basketNode = (ObjectNode) value;
			final IArrayNode itemsNode = BasketNodes.getItems(basketNode);
			for (final IJsonNode itemsNodeMember : itemsNode) {
				final String item = ((TextNode) itemsNodeMember).getTextValue();
				final int newCount = this.counts.getInt(item) + 1;
				if (newCount <= this.minSupport)
					this.counts.put(item, newCount);
				if (newCount == this.minSupport) {
					this.itemNode.setValue(item);
					out.collect(this.outputNode);
				}
			}
		}

	}

}
