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

import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

public class FrequentItemset {

	public static List<FrequentItemset> generateCandidates(
			final List<FrequentItemset> fis) {

		// Do sanity check: All fis should have the same size.
		int fisSize = -1;
		final Object2IntOpenHashMap<FrequentItemset> counts = new Object2IntOpenHashMap<FrequentItemset>();
		for (int i = 0; i < fis.size() - 1; i++) {
			final FrequentItemset firstFi = fis.get(i);
			if (fisSize < 0)
				fisSize = firstFi.items.length;
			for (int j = i + 1; j < fis.size(); j++) {
				final FrequentItemset secondFi = fis.get(j);
				if (secondFi.items.length != fisSize)
					throw new IllegalArgumentException(
							"Given frequent itemsets differ in length.");
				final FrequentItemset candidate = firstFi.union(secondFi);
				if (candidate != null) {
					final int count = counts.getInt(candidate);
					counts.put(candidate, count + 1);
				}
			}
		}
		// i.e. the number of all pairs
		final int neededOccurrences = (fisSize + 1) * fisSize / 2;
		final List<FrequentItemset> result = new ArrayList<FrequentItemset>();
		for (final Entry<FrequentItemset> entry : counts.object2IntEntrySet())
			if (entry.getIntValue() >= neededOccurrences)
				result.add(entry.getKey());
		return result;
	}

	public boolean isIncludedIn(final IArrayNode items) {
		int i = 0;
		for (final IJsonNode itemNode : items) {
			final String item = ((TextNode) itemNode).getTextValue();
			final int diff = this.items[i].compareTo(item);
			if (diff < 0)
				return false;
			else if (diff == 0) {
				i++;
				if (i >= this.items.length)
					return true;
			}
		}
		return false;
	}

	private final String[] items;
	private int support;

	/**
	 * Initializes FrequentItemset.
	 */
	public FrequentItemset(final String[] items, final int support) {
		this.items = items;
		this.support = support;
	}

	/**
	 * Generates a candidate frequent itemset by unioning the items of the given
	 * itemsets' items. They should differ in only one element. If not so,
	 * <code>null</code> will be returned instead.
	 */
	public FrequentItemset union(final FrequentItemset otherFis) {
		if (this.items.length != otherFis.items.length)
			throw new IllegalArgumentException(
					"Frequent itemsets differ in length.");

		int thisIndex, otherIndex, newIndex;
		thisIndex = otherIndex = newIndex = 0;
		final String[] newFisItems = new String[this.items.length + 1];

		while (thisIndex < this.items.length
				&& otherIndex < otherFis.items.length) {
			if (newIndex >= newFisItems.length)
				return null;
			final String thisItem = this.items[thisIndex];
			final String otherItem = otherFis.items[otherIndex];
			final int diff = thisItem.compareTo(otherItem);
			if (diff <= 0) {
				newFisItems[newIndex] = thisItem;
				thisIndex++;
			}
			if (diff >= 0) {
				newFisItems[newIndex] = otherItem;
				otherIndex++;
			}
			newIndex++;
		}

		while (thisIndex < this.items.length && newIndex < newFisItems.length)
			newFisItems[newIndex++] = this.items[thisIndex++];
		while (otherIndex < otherFis.items.length
				&& newIndex < newFisItems.length)
			newFisItems[newIndex++] = otherFis.items[otherIndex++];

		// This is the case if the given items are equal.
		if (newIndex < this.items.length || thisIndex < this.items.length
				|| otherIndex < otherFis.items.length)
			return null;

		return new FrequentItemset(newFisItems, 0);
	}

	/**
	 * Returns the items.
	 * 
	 * @return the items
	 */
	public String[] getItems() {
		return this.items;
	}

	/**
	 * Returns the support.
	 * 
	 * @return the support
	 */
	public int getSupport() {
		return this.support;
	}

	public void setSupport(final int support) {
		this.support = support;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(this.items) + this.support;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj instanceof FrequentItemset))
			return false;
		final FrequentItemset other = (FrequentItemset) obj;
		return this.support == other.support
				&& Arrays.equals(this.items, other.items);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FIS[items=" + Arrays.toString(this.items) + ";support="
				+ this.support + "]";
	}

}