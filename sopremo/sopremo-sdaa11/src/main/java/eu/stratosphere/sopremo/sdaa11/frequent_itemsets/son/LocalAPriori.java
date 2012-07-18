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

import java.util.ArrayList;
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
import eu.stratosphere.sopremo.sdaa11.util.JsonNodePool;
import eu.stratosphere.sopremo.sdaa11.util.JsonNodePool.TextNodeFactory;
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
public class LocalAPriori extends ElementaryOperator<LocalAPriori> {

	private static final long serialVersionUID = 1400135657979934016L;

	private int minSupport = 0;
	private int maxSetSize = 3;

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

	/**
	 * Returns the maxSetSize.
	 * 
	 * @return the maxSetSize
	 */
	public int getMaxSetSize() {
		return this.maxSetSize;
	}

	/**
	 * Sets the maxSetSize to the specified value.
	 * 
	 * @param maxSetSize
	 *            the maxSetSize to set
	 */
	public void setMaxSetSize(final int maxSetSize) {
		this.maxSetSize = maxSetSize;
	}

	public static class Implementation extends SopremoReduce {

		int minSupport;
		int maxSetSize;

		private final ObjectNode fisNode = new ObjectNode();
		private final IArrayNode itemsNode = new ArrayNode();
		private final JsonNodePool<TextNode> itemNodePool = new JsonNodePool<TextNode>(
				new TextNodeFactory());
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

			List<FrequentItemset> fis = this.findUnaryFrequentItemSets(values);
			this.emit(fis, out);
			int setSize = 1;
			while (setSize < this.maxSetSize) {
				setSize++;
				final List<FrequentItemset> candidates = FrequentItemset
						.generateCandidates(fis);
				fis = this.findFrequentItemsets(candidates, values);
				if (fis.isEmpty())
					break;
				this.emit(fis, out);
			}

		}

		/**
		 * @param fis
		 * @param out
		 */
		private void emit(final List<FrequentItemset> allFis,
				final JsonCollector out) {
			for (final FrequentItemset fis : allFis) {
				this.itemsNode.clear();
				for (final String item : fis.getItems()) {
					final TextNode itemNode = this.itemNodePool.getNode();
					itemNode.setValue(item);
					this.itemsNode.add(itemNode);
				}
				this.supportNode.setValue(fis.getSupport());
				FrequentItemsetNodes.write(this.fisNode, this.itemsNode,
						this.supportNode);
				out.collect(this.fisNode);
			}
		}

		/**
		 * @param candidates
		 * @param values
		 * @return
		 */
		private List<FrequentItemset> findFrequentItemsets(
				final List<FrequentItemset> candidates, final IArrayNode values) {
			final Object2IntOpenHashMap<FrequentItemset> counts = new Object2IntOpenHashMap<FrequentItemset>();
			counts.defaultReturnValue(0);

			for (final IJsonNode value : values) {
				final IArrayNode items = this.extractItems(value);
				for (final FrequentItemset fis : candidates)
					if (fis.isIncludedIn(items)) {
						final int count = counts.getInt(fis);
						counts.put(fis, count + 1);
					}
			}

			return this.retainFrequentItemsets(counts);
		}

		/**
		 * @param values
		 * @return
		 */
		private List<FrequentItemset> findUnaryFrequentItemSets(
				final IArrayNode values) {

			final Object2IntOpenHashMap<String> counts = new Object2IntOpenHashMap<String>();
			counts.defaultReturnValue(0);
			for (final IJsonNode value : values) {
				final IArrayNode items = this.extractItems(value);
				for (final IJsonNode item : items) {
					final String itemValue = ((TextNode) item).getTextValue();
					final int count = counts.getInt(itemValue);
					counts.put(itemValue, count + 1);
				}
			}

			return this.createFrequentItemsets(counts);
		}

		/**
		 * @param counts
		 * @return
		 */
		private List<FrequentItemset> createFrequentItemsets(
				final Object2IntOpenHashMap<String> counts) {
			final List<FrequentItemset> fis = new ArrayList<FrequentItemset>();
			for (final Entry<String> entry : counts.object2IntEntrySet()) {
				final int support = entry.getIntValue();
				if (support >= this.minSupport) {
					final String item = entry.getKey();
					fis.add(new FrequentItemset(new String[] { item }, entry
							.getIntValue()));
				}
			}
			return fis;
		}

		private List<FrequentItemset> retainFrequentItemsets(
				final Object2IntOpenHashMap<FrequentItemset> counts) {
			final List<FrequentItemset> retainedFis = new ArrayList<FrequentItemset>();
			for (final Entry<FrequentItemset> entry : counts
					.object2IntEntrySet()) {
				final int support = entry.getIntValue();
				if (support >= this.minSupport) {
					final FrequentItemset fis = entry.getKey();
					fis.setSupport(entry.getIntValue());
					retainedFis.add(fis);
				}
			}
			return retainedFis;
		}

		/**
		 * @param value
		 * @return
		 */
		private IArrayNode extractItems(final IJsonNode value) {
			final ObjectNode basket = (ObjectNode) AnnotatorNodes
					.getAnnotatee((ObjectNode) value);
			final IArrayNode items = BasketNodes.getItems(basket);
			return items;
		}
	}

}
