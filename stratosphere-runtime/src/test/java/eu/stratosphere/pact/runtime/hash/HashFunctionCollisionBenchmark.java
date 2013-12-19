/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.runtime.hash.MultiLevelHashTester.BucketBoundaries;
import eu.stratosphere.pact.runtime.hash.util.LastBitsToRange;
import eu.stratosphere.pact.runtime.hash.util.RandomIterator;
import eu.stratosphere.pact.runtime.hash.util.RangeCalculator;
import eu.stratosphere.pact.runtime.hash.util.RangeIterator;
import eu.stratosphere.pact.runtime.hash.util.StepRangeIterator;

/**
 * Test distribution of hash function for multiple levels
 * 
 * 
 */
public class HashFunctionCollisionBenchmark {

	private static final Log LOG = LogFactory.getLog(HashFunctionCollisionBenchmark.class);

	private static final long SEED = 561349061987311L;

	@Test
	public void testStepSeventeen() {

		// Define numbers of buckets on each level
		RangeCalculator[] rangeCalculators = { 
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 0
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 1
				new LastBitsToRange(10) }; // 2^10=1024 Buckets on level 2

		Iterator<Integer> importIterator = new StepRangeIterator(-30000000,
				30000000, 17);

		MultiLevelHashTester ht = new MultiLevelHashTester(importIterator,
				rangeCalculators);

		BucketBoundaries[] boundaries = {
				new BucketBoundaries(3000, 3700, 5, 0.01),
				new BucketBoundaries(0, 20,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.0001),
				new BucketBoundaries(0, 3,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.000001) };

		LOG.debug("Start Step Seventeen hash test");
		ht.runTest(boundaries);
		LOG.debug("End Step Seventeen hash test");
	}

	@Test
	public void testThreeLevel() {

		// Define numbers of buckets on each level
		RangeCalculator[] rangeCalculators = { 
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 0
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 1
				new LastBitsToRange(10) }; // 2^10=1024 Buckets on level 2

		Iterator<Integer> importIterator = new RangeIterator(-1000000, 1000000);

		MultiLevelHashTester ht = new MultiLevelHashTester(importIterator,
				rangeCalculators);

		BucketBoundaries[] boundaries = {
				new BucketBoundaries(1800, 2110, 5, 0.01),
				new BucketBoundaries(0, 15,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.0001),
				new BucketBoundaries(0, 2,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.000001) };

		LOG.debug("Start Three Level hash test");
		ht.runTest(boundaries);
		LOG.debug("End Three Level hash test");
	}

	@Test
	public void testRandom() {

		// Define numbers of buckets on each level
		RangeCalculator[] rangeCalculators = { 
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 0
				new LastBitsToRange(10), // 2^10=1024 Buckets on level 1
				new LastBitsToRange(10) }; // 2^10=1024 Buckets on level 2

		Iterator<Integer> importIterator = new RandomIterator(SEED, 2000000);

		MultiLevelHashTester ht = new MultiLevelHashTester(importIterator,
				rangeCalculators);

		BucketBoundaries[] boundaries = {
				new BucketBoundaries(1800, 2110, 5, 0.01),
				new BucketBoundaries(0, 15,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.0001),
				new BucketBoundaries(0, 2,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.000001) };

		LOG.debug("Start Random hash test");
		ht.runTest(boundaries);
		LOG.debug("End Random hash test");
	}

	@Test
	public void testTwoLevel() {

		// Define numbers of buckets on each level
		RangeCalculator[] rangeCalculators = { 
				new LastBitsToRange(12),	// 2^12=4096 Buckets on level 0
				new LastBitsToRange(12) };	// 2^12=4096 Buckets on level 1

		Iterator<Integer> importIterator = new RangeIterator(-1000000, 1000000);

		MultiLevelHashTester ht = new MultiLevelHashTester(importIterator,
				rangeCalculators);

		BucketBoundaries[] boundaries = {
				new BucketBoundaries(400, 600, 5, 0.01),
				new BucketBoundaries(0, 4,
						BucketBoundaries.MAX_EMPTY_UNBOUNDED, 0.0001) };

		LOG.debug("Start Two Level hash test");
		ht.runTest(boundaries);
		LOG.debug("End Two Level hash test");
	}

}

class MultiLevelHashTester {

	private static final Log LOG = LogFactory.getLog(MultiLevelHashTester.class);

	private final int maxLevel;
	private final Iterator<Integer> importIterator;
	private final RangeCalculator[] rangeCalculators;
	private final HashMap<Integer, Object> rootMap = new HashMap<Integer, Object>();
	private final ArrayList<SortedMap<Integer, Integer>> bucketSizesPerLevel;

	/**
	 * 
	 * @param hashFunction
	 *            HashFunction to be tested
	 * @param importIterator
	 *            Iterator over values to be used in test run
	 * @param rangeCalculators
	 *            For each level a range calculator which defines how to map
	 *            from hash to bucket
	 */
	public MultiLevelHashTester(Iterator<Integer> importIterator,
			RangeCalculator[] rangeCalculators) {
		this.importIterator = importIterator;
		this.rangeCalculators = rangeCalculators;
		this.maxLevel = rangeCalculators.length;
		this.bucketSizesPerLevel = new ArrayList<SortedMap<Integer, Integer>>(
				maxLevel);

		for (int i = 0; i < maxLevel; i++) {
			bucketSizesPerLevel.add(i, new TreeMap<Integer, Integer>());
		}
	}

	/**
	 * Run the test by: - Adding values from iterator to map - Creating
	 * histogram over bucket sizes per level - Printing histogram informations
	 * 
	 * @param boundaries
	 *            Expected results for each level
	 */
	public void runTest(BucketBoundaries[] boundaries) {
		addValues();
		collectStatistics(rootMap, 0);
		if (LOG.isDebugEnabled() == true) {
			printStatistics();
		}
		checkBoundaries(boundaries);
	}

	private void checkBoundaries(BucketBoundaries[] boundaries) {
		for (int level = 0; level < boundaries.length; level++) {
			int lowerBound = boundaries[level].getLowerBound();
			int upperBound = boundaries[level].getUpperBound();
			int bucketCountInLevel = 0;
			int bucketCountOutOfRange = 0;

			SortedMap<Integer, Integer> levelMap = bucketSizesPerLevel
					.get(level);
			Iterator<Integer> bucketSizeIterator = levelMap.keySet().iterator();

			while (bucketSizeIterator.hasNext()) {
				int bucketSize = bucketSizeIterator.next();
				if (bucketSize != 0) {
					int countForBucketSize = levelMap.get(bucketSize);
					bucketCountInLevel += countForBucketSize;
					if (lowerBound > bucketSize || upperBound < bucketSize) {
						bucketCountOutOfRange += countForBucketSize;
					}

				}
			}
			double bucketsOutOfRange = (double) bucketCountOutOfRange
					/ (double) bucketCountInLevel;
			double maxBucketsOutOfRange = boundaries[level]
					.getPercentOutOfRange();
			Assert.assertTrue("More than " + (maxBucketsOutOfRange * 100)
					+ "% of buckets out of range in level " + level,
					bucketsOutOfRange <= maxBucketsOutOfRange);

			int maxEmpty = boundaries[level].getMaxEmpty();
			Assert.assertTrue(
					"More than " + maxEmpty + " empty buckets in level "
							+ level,
					(maxEmpty == BucketBoundaries.MAX_EMPTY_UNBOUNDED)
							|| (levelMap.get(0) <= boundaries[level]
									.getMaxEmpty()));
		}
	}

	/**
	 * Find for each value the right bucket on the deepest level and increase
	 * its count
	 */
	@SuppressWarnings("unchecked")
	private void addValues() {

		while (importIterator.hasNext()) {
			int nextValue = importIterator.next();

			HashMap<Integer, Object> mapForCurrentLevel = rootMap;

			for (int i = 0; i < maxLevel - 1; i++) {
				int hashValue = MutableHashTable.hash(nextValue, i);
				int bucket = rangeCalculators[i].getBucket(hashValue);
				Object nextObject = mapForCurrentLevel.get(bucket);
				if (nextObject == null) {
					HashMap<Integer, Object> mapForNextLevel = new HashMap<Integer, Object>();
					mapForCurrentLevel.put(bucket, mapForNextLevel);
					mapForCurrentLevel = mapForNextLevel;

				} else {
					mapForCurrentLevel = (HashMap<Integer, Object>) nextObject;
				}
			}

			int lastHashValue = MutableHashTable.hash(nextValue, maxLevel - 1);
			int deepestBucketNr = rangeCalculators[maxLevel - 1]
					.getBucket(lastHashValue);
			Object countOnDeepestLevel = mapForCurrentLevel
					.get(deepestBucketNr);
			if (countOnDeepestLevel == null) {
				mapForCurrentLevel.put(deepestBucketNr, 1);
			} else {
				mapForCurrentLevel.put(deepestBucketNr,
						((Integer) countOnDeepestLevel) + 1);
			}

		}
	}

	private void printStatistics() {
		for (int level = 0; level < maxLevel; level++) {
			int bucketCountInLevel = 0;

			SortedMap<Integer, Integer> levelMap = bucketSizesPerLevel
					.get(level);
			Iterator<Integer> bucketSizeIterator = levelMap.keySet().iterator();

			LOG.debug("Statistics for level: " + level);
			LOG.debug("----------------------------------------------");
			LOG.debug("");
			LOG.debug("Bucket Size |      Count");
			LOG.debug("------------------------");

			int i = 0;
			while (bucketSizeIterator.hasNext()) {
				int bucketSize = bucketSizeIterator.next();
				if (bucketSize != 0) {
					int countForBucketSize = levelMap.get(bucketSize);
					bucketCountInLevel += countForBucketSize;
					Formatter formatter = new Formatter();
					formatter.format(" %10d | %10d", bucketSize, countForBucketSize);

					if (levelMap.size() < 20 || i < 3 || i >= (levelMap.size() - 3)) {
						LOG.debug(formatter.out());
					} else if (levelMap.size() / 2 == i) {
						LOG.debug("         .. |         ..");
						LOG.debug(formatter.out());
						LOG.debug("         .. |         ..");
					}
					i++;
					formatter.close();
				}
			}

			LOG.debug("");
			LOG.debug("Number of non-empty buckets in level: "
					+ bucketCountInLevel);
			LOG.debug("Number of empty buckets in level    : "
					+ levelMap.get(0));
			LOG.debug("Number of different bucket sizes    : "
					+ (levelMap.size() - 1));
			LOG.debug("");
			LOG.debug("");
			LOG.debug("");
		}
	}

	/**
	 * Create histogram over bucket sizes
	 * 
	 * @param map
	 *            Map to be analyzed
	 * @param level
	 *            Level on which the map is located in
	 * @return The total count of hashed values in the map
	 */
	private int collectStatistics(HashMap<Integer, Object> map, int level) {
		SortedMap<Integer, Integer> bucketSizesForLevel = bucketSizesPerLevel
				.get(level);

		Iterator<Object> bucketIterator = map.values().iterator();
		int bucketCount = 0;
		int totalValueCount = 0;

		while (bucketIterator.hasNext()) {
			bucketCount++;

			Integer hashValuesInBucket;
			// If we are already on the deepest level, get the count in the
			// bucket, otherwise
			// recursively examine the subtree
			if (level == maxLevel - 1) {
				hashValuesInBucket = (Integer) bucketIterator.next();
			} else {
				@SuppressWarnings("unchecked")
				HashMap<Integer, Object> nextMap = (HashMap<Integer, Object>) bucketIterator
						.next();
				hashValuesInBucket = collectStatistics(nextMap, level + 1);
			}
			totalValueCount += hashValuesInBucket;
			Integer countOfBucketSizes = bucketSizesForLevel
					.get(hashValuesInBucket);
			if (countOfBucketSizes == null) {
				countOfBucketSizes = 1;
			} else {
				countOfBucketSizes += 1;
			}
			bucketSizesForLevel.put(hashValuesInBucket, countOfBucketSizes);
		}

		Integer countOfEmptyBuckets = bucketSizesForLevel.get(0);
		if (countOfEmptyBuckets == null) {
			countOfEmptyBuckets = rangeCalculators[level].getBucketCount()
					- bucketCount;
		} else {
			countOfEmptyBuckets += rangeCalculators[level].getBucketCount()
					- bucketCount;
		}
		bucketSizesForLevel.put(0, countOfEmptyBuckets);

		return totalValueCount;
	}

	/**
	 * Expected results for bucket sizes per level
	 * 
	 *
	 */
	static class BucketBoundaries {

		public static final int MAX_EMPTY_UNBOUNDED = -1;
		private int lowerBound;
		private int upperBound;
		private int maxEmpty;
		private double percentOutOfRange;

		/**
		 * 
		 * 
		 * @param lowerBound Lower bound for bucket sizes
		 * @param upperBound Upper bound for bucket sizes
		 * @param maxEmpty Maximum number of empty buckets
		 * @param percentOutOfRange Maximum percentage of buckets out of range
		 */
		public BucketBoundaries(int lowerBound, int upperBound, int maxEmpty,
				double percentOutOfRange) {
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
			this.maxEmpty = maxEmpty;
			this.percentOutOfRange = percentOutOfRange;
		}

		/**
		 * 
		 * @return Lower bound for bucket sizes
		 */
		public int getLowerBound() {
			return lowerBound;
		}
		
		/**
		 * 
		 * @return Upper bound for bucket sizes
		 */
		public int getUpperBound() {
			return upperBound;
		}

		/**
		 * 
		 * @return Maximum number of empty buckets
		 */
		public int getMaxEmpty() {
			return maxEmpty;
		}

		/**
		 * 
		 * @return Maximum percentage of buckets out of range
		 */
		public double getPercentOutOfRange() {
			return percentOutOfRange;
		}
	}
}
