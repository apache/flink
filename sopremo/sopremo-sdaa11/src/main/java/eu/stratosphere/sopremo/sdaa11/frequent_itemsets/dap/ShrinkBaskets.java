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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Set;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.dap.json.FrequentItemsetListNodes;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.BasketNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Retains only those items from baskets that exist in a frequent item set. If a
 * basket cannot contain frequent item sets of higher item set sizes it might
 * even be filtered.<br>
 * <br>
 * Inputs:
 * <ol>
 * <li>Baskets</li>
 * <li>Frequent item set list <i>(expecting exactly one list!)</i></li>
 * </ol>
 * Outputs:
 * <ol>
 * <li>Shrinked baskets</li>
 * </ol>
 * 
 * @author skruse
 * 
 */
@InputCardinality(value = 2)
@OutputCardinality(value = 1)
public class ShrinkBaskets extends ElementaryOperator<ShrinkBaskets> {

	private static final long serialVersionUID = -7543342863585696395L;

	public static class Implementation extends SopremoCross {

		private final Set<String> frequentItems = new ObjectOpenHashSet<String>();
		private final ObjectNode outputNode = new ObjectNode();
		private final ArrayNode itemsOutputNode = new ArrayNode();

		public Implementation() {
			BasketNodes.write(this.outputNode, this.itemsOutputNode);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void cross(final IJsonNode basketValue,
				final IJsonNode fisListValue, final JsonCollector out) {
			this.frequentItems.clear();
			int fisSize = -1;
			final IArrayNode fisListNode = FrequentItemsetListNodes
					.getFrequentItemsets((ObjectNode) fisListValue);
			for (final IJsonNode itemsValue : fisListNode) {
				final IArrayNode itemsNode = (IArrayNode) itemsValue;
				if (fisSize < 0)
					fisSize = itemsNode.size();
				for (final IJsonNode fisNodeMember : itemsNode)
					this.frequentItems.add(((TextNode) fisNodeMember)
							.getTextValue());
			}
			this.itemsOutputNode.clear();
			for (final IJsonNode itemValue : BasketNodes
					.getItems((ObjectNode) basketValue)) {
				final TextNode itemNode = (TextNode) itemValue;
				if (this.frequentItems.contains(itemNode.getTextValue()))
					this.itemsOutputNode.add(itemNode);
			}
			if (this.itemsOutputNode.size() > fisSize)
				out.collect(this.outputNode);
		}

	}
}
