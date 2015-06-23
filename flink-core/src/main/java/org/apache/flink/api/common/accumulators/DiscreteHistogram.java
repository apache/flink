/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.accumulators;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A Histogram accumulator designed for Discrete valued data.
 * --{@link #count(double)}
 * 		Compute number of elements belonging to a given category
 * --{@link #entropy()}
 * 		Computes the entropy(in bits) of the data set represented by this histogram
 * --{@link #gini()}
 * 		Computes the Gini impurity of the data set represented by this histogram
 */
public class DiscreteHistogram implements Accumulator<Double, TreeMap<Double, Integer>> {

	private static double logConversion = Math.log(2);
	protected TreeMap<Double, Integer> treeMap = new TreeMap<Double, Integer>();
	protected long counter = 0;

	/**
	 * Consider using {@link #add(double)} for primitive double values to get better performance.
	 */
	@Override
	public void add(Double value) {
		add(value, 1);
	}

	public void add(double value) {
		add(value, 1);
	}

	@Override
	public TreeMap<Double, Integer> getLocalValue() {
		return treeMap;
	}

	@Override
	public void merge(Accumulator<Double, TreeMap<Double, Integer>> other) {
		// Merge the values into this map
		fill(other.getLocalValue().entrySet());
	}

	@Override
	public void resetLocal() {
		counter = 0;
		treeMap.clear();
	}

	@Override
	public String toString() {
		return "Discrete Histogram:: Total: " + counter + ", Map: " + treeMap.toString();
	}

	@Override
	public Accumulator<Double, TreeMap<Double, Integer>> clone() {
		DiscreteHistogram result = new DiscreteHistogram();
		result.treeMap = new TreeMap<Double, Integer>(treeMap);
		result.counter = counter;
		return result;
	}

	/**
	 * Get the total number of items added to this histogram.
	 * This is preserved across merge operations.
	 *
	 * @return Total number of items added to the histogram
	 */
	public long getTotal() {
		return counter;
	}

	/**
	 * Get the current size of the {@link #treeMap}
	 *
	 * @return Size of the {@link #treeMap}
	 */
	public int getSize() {
		return treeMap.size();
	}

	/**
	 * Add a new value to the histogram along with the associated count of items
	 *
	 * @param value Value to be added
	 * @param count Number of items associated with value
	 */
	private void add(double value, int count) {
		counter += count;
		Integer current = treeMap.get(value);
		Integer newValue = (current != null ? current : 0) + count;
		treeMap.put(value, newValue);
	}

	/**
	 * Directly loads values and their associated counts from the given set.
	 *
	 * @param entries Set of values and their associated counts.
	 */
	void fill(Set<Map.Entry<Double, Integer>> entries) {
		for (Map.Entry<Double, Integer> entry : entries) {
			if (entry.getValue() <= 0) {
				throw new IllegalArgumentException("Negative counters are not allowed: " + entry);
			}
		}
		for (Map.Entry<Double, Integer> entry : entries) {
			add(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Returns the number of elements belonging to the given category
	 *
	 * @param category Category
	 * @return Elements belonging to category
	 */
	public int count(double category) {
		Integer val = treeMap.get(category);
		return val != null ? val : 0;
	}

	/**
	 * Computes the Entropy(in bits) of the data set represented by this histogram
	 *
	 * @return Entropy(in bits) of the data set
	 */
	public double entropy() {
		long total = getTotal();
		double result = 0.0;
		for (double classCount : treeMap.values()) {
			result -= classCount * Math.log(classCount / total);
		}
		return result / (logConversion * total);
	}

	/**
	 * Computes the Gini impurity of the data set represented by this histogram
	 *
	 * @return Gini impurity of the data set
	 */
	public double gini() {
		long total = getTotal();
		double result = 0.0;
		for (double classCount : treeMap.values()) {
			result += Math.pow(classCount, 2);
		}
		return 1.0 - result / Math.pow(total, 2);
	}
}
