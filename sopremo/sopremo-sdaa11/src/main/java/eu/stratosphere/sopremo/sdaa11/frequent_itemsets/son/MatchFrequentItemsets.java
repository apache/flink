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

import java.util.Iterator;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.BasketNodes;
import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.FrequentItemsetNodes;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
@InputCardinality(min = 2, max = 2)
public class MatchFrequentItemsets extends
		ElementaryOperator<MatchFrequentItemsets> {

	private static final long serialVersionUID = -1684243615303990964L;

	public static class Implementation extends SopremoCross {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.pact.SopremoCross#cross(eu.stratosphere.sopremo
		 * .type.IJsonNode, eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void cross(final IJsonNode fisNode,
				final IJsonNode basketNode, final JsonCollector out) {
			final IArrayNode fisItems = FrequentItemsetNodes
					.getItems((ObjectNode) fisNode);
			final IArrayNode basketItems = BasketNodes
					.getItems((ObjectNode) basketNode);
			if (this.isIncluded(fisItems, basketItems))
				out.collect(fisNode);

		}

		private boolean isIncluded(final IArrayNode fisItems,
				final IArrayNode basketItems) {
			final Iterator<IJsonNode> fisIterator = fisItems.iterator();
			String fisItem = ((TextNode) fisIterator.next()).getTextValue();
			for (final IJsonNode basketItemNode : basketItems) {
				final String basketItem = ((TextNode) basketItemNode)
						.getTextValue();
				final int diff = fisItem.compareTo(basketItem);
				if (diff < 0)
					return false;
				else if (diff == 0) {
					if (!fisIterator.hasNext())
						return true;
					fisItem = ((TextNode) fisIterator.next()).getTextValue();
				}
			}
			return false;
		}

	}

}
