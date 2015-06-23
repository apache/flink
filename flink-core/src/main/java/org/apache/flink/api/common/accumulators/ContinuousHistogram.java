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

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import static java.lang.Double.MAX_VALUE;

/**
 * A Histogram accumulator designed for Continuous valued data.
 * It supports:
 * -- {@link #quantile(double)}
 * 		Computes a quantile of the data
 * -- {@link #count(double)}
 *		Computes number of items less than the given value in the data
 * -- {@link #min()}
 * 		Minimum value added to this histogram
 * -- {@link #max()}
 * 		Maximum value added to this histogram
 * -- {@link #mean()}
 * 		Mean of the values added to this histogram
 * -- {@link #variance()}
 * 		Variance of valued added to this histogram
 *
 * <p>
 * A continuous histogram stores values in bins in sorted order and keeps their associated
 * number of items. It is assumed that the items associated with every bin are scattered around
 * it, half to the right and half to the left.
 * <p>
 * bin counters:  m_1    m_2    m_3    m_4    m_5    m_6
 *                10     12     5      10     4      6
 *                |  5   |  6   |  2.5 |  5   |  2   |
 *             5  |  +   |  +   |   +  |  +   |  +   |  3
 *                |  6   |  2.5 |   5  |  2   |  3   |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * bin index:     1      2      3      4      5      6
 * bin values:    v_1 <  v_2 <  v_3 <  v_4 <  v_5 <  v_6
 * <p>
 * The number of items between v_i and v_(i+1) is directly proportional to the area of
 * trapezoid (v_i, v_(i+1), m_(i+1), m_i)
 * <p>
 * Adapted from Ben-Haim and Yom-Tov's
 * <a href="http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf">Streaming Decision Tree Algorithm's histogram</a>
 */
public class ContinuousHistogram implements Accumulator<Double, TreeMap<Double, Integer>> {

	protected TreeMap<Double, Integer> treeMap = new TreeMap<Double, Integer>();

	protected long counter = 0;

	private int bin;

	private double lower;

	private double upper;

	protected double sum;

	protected double sumSquare;

	private PriorityQueue<KeyDiff> diffQueue;

	private HashMap<Double, KeyProps> keyUpdateTimes;

	private long timestamp;

	/**
	 * Creates a new Continuous histogram with the given number of bins
	 * Bins represents the number of values the histogram stores to approximate the continuous
	 * data set. The higher this value, the more we move towards an exact representation of data.
	 *
	 * @param numBins Number of bins in the histogram
	 */
	public ContinuousHistogram(int numBins) {
		if (numBins <= 0) {
			throw new IllegalArgumentException("Number of bins must be greater than zero");
		}
		bin = numBins;
		lower = MAX_VALUE;
		upper = -MAX_VALUE;
		diffQueue = new PriorityQueue<>();
		keyUpdateTimes = new HashMap<>();
		timestamp = 0;
		sum = 0;
		sumSquare = 0;
	}

	/**
	 * Consider using {@link #add(double)} for primitive double values to get better performance.
	 */
	@Override
	public void add(Double value) {
		add(value.doubleValue());
	}

	public void add(double value) {
		lower = Math.min(lower, value);
		upper = Math.max(upper, value);

		sum += value;
		sumSquare += Math.pow(value, 2);

		addValue(value, 1);
		if (getSize() > bin) {
			mergeBins();
		}
	}

	@Override
	public TreeMap<Double, Integer> getLocalValue() {
		return this.treeMap;
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
	 * Return the minimum value added to this histogram
	 *
	 * @return minimum value added
	 */
	public double min() {
		return lower;
	}

	/**
	 * Return the maximum value added to this histogram
	 *
	 * @return maximum value added
	 */
	public double max() {
		return upper;
	}

	/**
	 * Return the mean of values added to this histogram
	 *
	 * @return mean of added values
	 */
	public double mean() {
		return sum / getTotal();
	}

	/**
	 * Return the variance of valued added to this histogram
	 *
	 * @return variance of added values
	 */
	public double variance() {
		return sumSquare / getTotal() - Math.pow(mean(), 2);
	}

	/**
	 * Get the current size of the {@link #treeMap}
	 *
	 * @return Size of the {@link #treeMap}
	 */
	public int getSize() {
		return treeMap.size();
	}

	@Override
	public void resetLocal() {
		treeMap.clear();
		counter = 0;
		lower = MAX_VALUE;
		upper = -MAX_VALUE;
		diffQueue.clear();
		keyUpdateTimes.clear();
		sum = 0;
		sumSquare = 0;
	}

	@Override
	public void merge(Accumulator<Double, TreeMap<Double, Integer>> other) {
		if (other instanceof ContinuousHistogram) {
			ContinuousHistogram continuousHistogram = (ContinuousHistogram) other;
			lower = Math.min(lower, continuousHistogram.min());
			upper = Math.max(upper, continuousHistogram.max());
			sum += continuousHistogram.sum;
			sumSquare += continuousHistogram.sumSquare;

			for (Map.Entry<Double, Integer> entry: other.getLocalValue().entrySet()) {
				addValue(entry.getKey(), entry.getValue());
			}

			while (getSize() > bin) {
				mergeBins();
			}
		} else {
			throw new RuntimeException("Only a Continuous Histogram can be merged with a Continuous Histogram");
		}
	}

	/**
	 * Merges the given other histogram into this histogram, with the number of bins in the
	 * merged histogram being {@code numBins}.
	 *
	 * @param other   Histogram to be merged
	 * @param numBins Bins in the merged histogram
	 */
	public void merge(Accumulator<Double, TreeMap<Double, Integer>> other, int numBins) {
		bin = numBins;
		merge(other);
	}

	@Override
	public Accumulator<Double, TreeMap<Double, Integer>> clone() {
		ContinuousHistogram result = new ContinuousHistogram(bin);
		result.treeMap = new TreeMap<>(treeMap);
		result.counter = counter;
		result.lower = lower;
		result.upper = upper;
		result.sum = sum;
		result.sumSquare = sumSquare;
		// initialize all differences and key update times for the new histogram
		result.computeDiffs();
		return result;
	}

	void fill(Set<Map.Entry<Double, Integer>> entries) {
		for (Map.Entry<Double, Integer> entry : entries) {
			if (entry.getValue() <= 0) {
				throw new IllegalArgumentException("Negative counters are not allowed: " + entry);
			}
		}

		for (Map.Entry<Double, Integer> entry : entries) {
			lower = Math.min(lower, entry.getKey());
			upper = Math.max(upper, entry.getKey());
			sum += entry.getKey() * entry.getValue();
			sumSquare += entry.getValue() * Math.pow(entry.getKey(), 2);
			addValue(entry.getKey(), entry.getValue());
		}

		while (getSize() > bin) {
			mergeBins();
		}
	}

	/**
	 * Adds a new value to the histogram along with an associated count.
	 *
	 * @param value Value to be added
	 * @param count Associated count to this value
	 */
	private void addValue(double value, int count) {
		// Add to the map.
		counter += count;
		Integer current = treeMap.get(value);
		Integer newValue = (current != null ? current : 0) + count;
		treeMap.put(value, newValue);

		// in case a new bin was created, we need to compute the differences and reset lower and
		// higher of existing keys
		if (current == null) {
			Double higherValue = treeMap.higherKey(value);
			Double lowerValue = treeMap.lowerKey(value);

			// new key available. Increment the timestamp
			timestamp++;
			keyUpdateTimes.put(value, new KeyProps(timestamp, higherValue, lowerValue));

			if (lowerValue != null) {
				// set value as the higher key of the immediately lower key
				keyUpdateTimes.get(lowerValue).higher = value;

				// new difference available
				diffQueue.offer(new KeyDiff(lowerValue, value, keyUpdateTimes.get(lowerValue).timestamp, timestamp));
			}

			if (higherValue != null) {
				// set value as the lower key of the immediately higher key
				keyUpdateTimes.get(higherValue).lower = value;

				// new difference available
				diffQueue.offer(new KeyDiff(value, higherValue, timestamp, keyUpdateTimes.get(higherValue).timestamp));
			}
		}
	}

	/**
	 * Merges the closest two bins in the histogram.
	 * This method is used to bring the histogram to the allowed number of bins after an add or
	 * merge operation.
	 */
	private void mergeBins() {
		// we only come to merge when we've added something and we're over the allowed number of
		// bins
		// if both the keys which this difference corresponds to, exist in the keyUpdateTimes
		// right now, and timestamps of both keys match, we keep it, otherwise, throw it away.
		KeyDiff minDiff = diffQueue.poll();
		if (!(keyUpdateTimes.containsKey(minDiff.key)
				&& keyUpdateTimes.get(minDiff.key).timestamp == minDiff.timestamp1
				&& keyUpdateTimes.containsKey(minDiff.nextKey)
				&& keyUpdateTimes.get(minDiff.nextKey).timestamp == minDiff.timestamp2)) {
			mergeBins();
			return;
		}

		int counter1 = treeMap.get(minDiff.key);
		int counter2 = treeMap.get(minDiff.nextKey);
		int newCount = counter1 + counter2;
		double newValue = (minDiff.key * counter1 + minDiff.nextKey * counter2) / newCount;

		// remove the existing keys
		treeMap.remove(minDiff.key);
		treeMap.remove(minDiff.nextKey);
		Double lowerEntry = keyUpdateTimes.remove(minDiff.key).lower;
		Double higherEntry = keyUpdateTimes.remove(minDiff.nextKey).higher;

		// put a new key
		timestamp++;
		treeMap.put(newValue, newCount);
		keyUpdateTimes.put(newValue, new KeyProps(timestamp, higherEntry, lowerEntry));

		// new differences are available.
		if (higherEntry != null) {
			// reset the lower key of the higher key nextKey
			keyUpdateTimes.get(higherEntry).lower = newValue;
			diffQueue.offer(new KeyDiff(newValue, higherEntry, timestamp, keyUpdateTimes.get(higherEntry).timestamp));
		}
		if (lowerEntry != null) {
			// reset the higher key of the lower key of key
			keyUpdateTimes.get(lowerEntry).higher = newValue;
			diffQueue.offer(new KeyDiff(lowerEntry, newValue, keyUpdateTimes.get(lowerEntry).timestamp, timestamp));
		}
	}

	/**
	 * Returns the qth quantile of the data
	 *
	 * @return q<sup>th</sup> quantile of data
	 */
	public double quantile(double q) {
		if (treeMap.isEmpty()) {
			return upper;
		} else {
			if (q < 0 || q > 1) {
				throw new IllegalArgumentException("q must be lie in between 0 and 1");
			} else {
				long wantedSum = Math.round(q * getTotal());

				// <------ wantedSum ---------------->
				// <- count (x) ---->
				//                  <-- left over --->
				//                                                      | m_y
				//                                m_b |                 |
				//              m_x |                 |                 |
				//                  |                 |                 |
				//               ___|_________________|_________________|_____
				//                 x                b                  y
				//
				// m_b is on the straight line connecting m_x and m_y
				// (m_x + m_y) / 2 elements correspond to the area of trapezoid (x, y, m_y, m_x)
				// A = Area(x, y, m_y, m_x) = (m_x + m_y) * (y - x)/2
				// Area of the trapezoid (x, b, m_b, m_x) is
				// A1 = (m_x + m_b) * (b - x)/2
				// we need that leftOver = A1 * (m_x + m_y) / 2 * A
				// this leads to solving a quadratic equation in t = b - x
				// (m_y - m_x).t^2 + 2.m_x.(y - x).t - 2.leftOver.(y - x)^2

				Iterator<Map.Entry<Double, Integer>> entries = treeMap.entrySet().iterator();
				double currentSum = 0;
				Map.Entry<Double, Integer> currentEntry = new AbstractMap.SimpleEntry<>(lowerVal(), 0);
				Map.Entry<Double, Integer> nextEntry = null;
				double leftOver = -1;

				while (entries.hasNext()) {
					nextEntry = entries.next();
					double newSum = currentSum + currentEntry.getValue() / 2.0 + nextEntry.getValue() / 2.0;
					if (currentSum <= wantedSum && newSum > wantedSum) {
						leftOver = wantedSum - currentSum;
						break;
					} else {
						currentEntry = nextEntry;
						currentSum = newSum;
					}
				}

				if (leftOver == -1) {
					// we have to go further than the last key
					leftOver = wantedSum - currentSum;
					nextEntry = new AbstractMap.SimpleEntry<>(upperVal(), 0);
				}

				// now solve the quadratic
				double alpha = nextEntry.getValue() - currentEntry.getValue();
				double beta = 2 * currentEntry.getValue() * (nextEntry.getKey() - currentEntry.getKey());
				double gamma = -2 * leftOver * Math.pow(nextEntry.getKey() - currentEntry.getKey(), 2);
				if (alpha == 0) {
					return currentEntry.getKey() + (-gamma / beta);
				} else {
					return currentEntry.getKey() + (-beta + Math.sqrt(Math.pow(beta, 2) - 4 * alpha * gamma)) / (2 * alpha);
				}
			}
		}
	}

	/**
	 * Returns the number of items less than the given value b.
	 *
	 * @return Approximate number of items in the data which are less than b
	 */
	public long count(double b) {
		if (b < lowerVal()) {
			return 0;
		} else if (b > upperVal()) {
			return getTotal();
		} else {
			// Suppose i is the index of bin with value x just less than or equal to b.
			// ALL the items till (i-1) are definitely less than b
			// Also, HALF of the items for bin i are less than x
			// Now, there are (m_x + m_y) / 2 items between bins i and i + 1.
			// Here m_y is value of bin i + 1
			//
			//          m_b    | m_y
			//           |     |
			// m_x |     |     |
			//     |     |     |
			//  ___|_____|_____|_______________
			//    x     b     y
			//
			// m_b is on the straight line connecting m_x and m_y
			// These (m_x + m_y) / 2 elements correspond to the area of trapezoid
			// A = Area(x, y, m_y, m_x) = (m_x + m_y) * (y - x)/2
			// Area of the trapezoid (x, b, m_b, m_x) is
			// A1 = (m_x + m_b) * (b - x)/2
			// count(b) is simply A1 * (m_x + m_y) / 2 * A
			// = (m_x + m_b) * (b - x) / (2 * (y - x))


			Map.Entry<Double, Integer> higherEntry = treeMap.higherEntry(b);
			Map.Entry<Double, Integer> lowerEntry = treeMap.floorEntry(b);
			if (higherEntry == null) {
				// b is more than the last key but less than an upper bound.
				higherEntry = new AbstractMap.SimpleEntry<>(upperVal(), 0);
			} else if (lowerEntry == null) {
				lowerEntry = new AbstractMap.SimpleEntry<>(lowerVal(), 0);
			}

			// now sum up to one key before the lower key
			double result = 0;

			for (double value : treeMap.headMap(lowerEntry.getKey(), false).values()) {
				result += value;
			}

			// add half of lower key, i.e., x
			result += lowerEntry.getValue() / 2.0;
			// now add a fraction of values between x and y
			double slope = (higherEntry.getValue() - lowerEntry.getValue()) / (higherEntry.getKey() - lowerEntry.getKey());
			double m_b = lowerEntry.getValue() + slope * (b - lowerEntry.getKey());
			result += (lowerEntry.getValue() + m_b) * (b - lowerEntry.getKey()) / (2 * (higherEntry.getKey() - lowerEntry.getKey()));
			return Math.round(result);
		}
	}

	/**
	 * Computes all the differences again by re-initializing the relevant fields.
	 */
	private void computeDiffs() {
		diffQueue.clear();
		keyUpdateTimes.clear();
		timestamp = 0;
		Iterator<Map.Entry<Double, Integer>> iterator = treeMap.entrySet().iterator();
		Double key = iterator.next().getKey();
		Double lowerEntry = null;
		while (iterator.hasNext()) {
			Double nextKey = iterator.next().getKey();
			diffQueue.offer(new KeyDiff(key, nextKey, timestamp, timestamp));
			keyUpdateTimes.put(key, new KeyProps(timestamp, nextKey, lowerEntry));
			lowerEntry = key;
			key = nextKey;
		}
		keyUpdateTimes.put(key, new KeyProps(timestamp, null, lowerEntry));
	}

	/**
	 * Returns an approximate lower bound on the items in the histogram.
	 * Since the items at index 0 bin are evenly distributed around it, we shift the minimum back
	 * by the average bin sizes
	 */
	private double lowerVal() {
		return lower - (upper - lower) / bin;
	}

	/**
	 * Returns an approximate upper bound on the items in the histogram.
	 * Since the items in the last bin are evenly distributed around it, we shift the maximum
	 * further by the average bin sizes
	 */
	private double upperVal() {
		return upper + (upper - lower) / bin;
	}

	/**
	 * A class to maintain the differences of consecutive keys at any time.
	 * Any time we want to consider this difference for merging purposes, we need to make sure
	 * that both the keys {@link #key} and {@link #nextKey} are present in {@link #keyUpdateTimes}.
	 * The update timestamps of both keys must also match the respective timestamps stored here.
	 */
	private final class KeyDiff implements Comparable<KeyDiff> {
		public double key;
		public double nextKey;
		public long timestamp1;
		public long timestamp2;

		/**
		 * @param key        Key
		 * @param nextKey    Key next to {@link #key}
		 * @param timestamp1 Timestamp of {@link #key} when this difference was computed
		 * @param timestamp2 Timestamp of {@link #nextKey} when this difference was computed
		 */
		public KeyDiff(double key, double nextKey, long timestamp1, long timestamp2) {
			this.key = key;
			this.nextKey = nextKey;
			this.timestamp1 = timestamp1;
			this.timestamp2 = timestamp2;
		}

		/**
		 * Gets the difference associated with this {@link KeyDiff}
		 *
		 * @return {@link #nextKey} - {@link #key}
		 */
		public double getDiff() {
			return nextKey - key;
		}

		@Override
		/**
		 * Comparator to another {@link KeyDiff}.
		 *
		 * @param other The {@link keyDiff} to be compared to this
		 * @return 1 if other.getDiff is higher, -1 if it is lower otherwise 0
		 */
		public int compareTo(KeyDiff other) {
			double diff = getDiff() - other.getDiff();
			return (int) Math.signum(diff);
		}

		@Override
		public String toString() {
			return key + " " + nextKey + " " + timestamp1 + " " + timestamp2;
		}
	}

	/**
	 * Defines a key properties class
	 * This stores a timestamp when the key was inserted into {@link #treeMap}, and what the
	 * immediately lower and higher values at any point in time.
	 */
	private final class KeyProps {
		public long timestamp;
		public Double higher;
		public Double lower;

		/**
		 * @param timestamp Time at which this key was inserted into {@link #treeMap}
		 * @param higher    Immediately higher key at the time of insertion
		 * @param lower     Immediately lower key at the time of insertion
		 */
		public KeyProps(long timestamp, Double higher, Double lower) {
			this.timestamp = timestamp;
			this.lower = lower;
			this.higher = higher;
		}

		@Override
		public String toString() {
			return timestamp + " " + higher + " " + lower;
		}
	}
}