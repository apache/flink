package eu.stratosphere.pact.runtime.hash;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;


public class HashTester {

	private final int maxLevel;
	private final HashFunction hashFunction;
	private final Iterator<Integer> importIterator;
	private final RangeCalculator[] rangeCalculators;
	private final HashMap<Integer, Object> rootMap = new HashMap<Integer, Object>();
	private final ArrayList<SortedMap<Integer, Integer>> bucketSizesPerLevel; 
	
	
	/**
	 *
	 * @param hashFunction HashFunction to be tested
	 * @param importIterator Iterator over values to be used in test run
	 * @param rangeCalculators For each level a range calculator which 
	 * 							defines how to map from hash to bucket
	 */
	public HashTester(HashFunction hashFunction, Iterator<Integer> importIterator, 
			RangeCalculator[] rangeCalculators)
	{
		this.hashFunction = hashFunction;
		this.importIterator = importIterator;
		this.rangeCalculators = rangeCalculators;
		this.maxLevel = rangeCalculators.length;
		this.bucketSizesPerLevel = new ArrayList<SortedMap<Integer,Integer>>(maxLevel);
		
		for (int i = 0; i < maxLevel; i++)
		{
			bucketSizesPerLevel.add(i, new TreeMap<Integer,Integer>());
		}
	}
	
	/**
	 * Run the test by:
	 * 		- Adding values from iterator to map
	 * 		- Creating histogram over bucketsizes per level
	 * 		- Printing histogram informations
	 * @param boundaries 
	 */
	public void runTest(BucketBoundaries[] boundaries)
	{
		addValues();
		collectStatistics(rootMap, 0);
		printStatistics();
		checkBounadiers(boundaries);
	}
	
	private void checkBounadiers(BucketBoundaries[] boundaries) {
		for (int level = 0; level < boundaries.length; level++)
		{
			int lowerBound = boundaries[level].getLowerBound();
			int upperBound = boundaries[level].getUpperBound();
			int bucketCountInLevel = 0;
			int bucketCountOutOfRange = 0;
			
			SortedMap<Integer, Integer> levelMap = bucketSizesPerLevel.get(level);
			Iterator<Integer> bucketSizeIterator = levelMap.keySet().iterator();

			
			while (bucketSizeIterator.hasNext())
			{
				int bucketSize = bucketSizeIterator.next();
				if (bucketSize != 0)
				{
					int countForBucketSize = levelMap.get(bucketSize);
					bucketCountInLevel += countForBucketSize;
					if (lowerBound > bucketSize || upperBound < bucketSize)
					{
						bucketCountOutOfRange += countForBucketSize;
					}
					
				}
			}
			double bucketsOutOfRange = (double) bucketCountOutOfRange / (double)bucketCountInLevel;
			double maxBucketsOutOfRange = boundaries[level].getPercentOutOfRange();
			Assert.assertTrue("More than " + maxBucketsOutOfRange + "% of buckets out of range in level " + level,
				bucketsOutOfRange <= maxBucketsOutOfRange);

			Assert.assertTrue("More than "+ boundaries[level].getMaxEmpty() + " empty buckets in level "+ level,
				levelMap.get(0) <= boundaries[level].getMaxEmpty());
		}
	}

	/**
	 * Find for each value the right bucket on the deepest level and increase its count
	 */
	@SuppressWarnings("unchecked")
	private void addValues()
	{

		while (importIterator.hasNext())
		{
			int nextValue = importIterator.next();
			
			HashMap<Integer, Object> mapForCurrentLevel = rootMap;
			
			for (int i = 0; i < maxLevel - 1; i++)
			{
				int hashValue = hashFunction.hash(nextValue, i);
				int bucket = rangeCalculators[i].getBucket(hashValue);
				Object nextObject = mapForCurrentLevel.get(bucket);
				if (nextObject == null)
				{
					HashMap<Integer, Object> mapForNextLevel = new HashMap<Integer, Object>();
					mapForCurrentLevel.put(bucket, mapForNextLevel);
					mapForCurrentLevel = mapForNextLevel;
					
				}
				else
				{
					mapForCurrentLevel = (HashMap<Integer, Object>) nextObject;
				}
			}
			
			int lastHashValue = hashFunction.hash(nextValue, maxLevel - 1);
			int deepestBucketNr = rangeCalculators[maxLevel - 1].getBucket(lastHashValue);
			Object countOnDeepestLevel = mapForCurrentLevel.get(deepestBucketNr);
			if (countOnDeepestLevel == null)
			{
				mapForCurrentLevel.put(deepestBucketNr, 1);
			}
			else
			{
				mapForCurrentLevel.put(deepestBucketNr, ((Integer)countOnDeepestLevel) + 1);
			}
			
		}
	}
	
	
	private void printStatistics()
	{
		for (int level = 0; level < maxLevel; level++)
		{
			int bucketCountInLevel = 0;
			
			SortedMap<Integer, Integer> levelMap = bucketSizesPerLevel.get(level);
			Iterator<Integer> bucketSizeIterator = levelMap.keySet().iterator();
			
			System.out.println("Statistics for level: "+ level);
			System.out.println("----------------------------------------------");
			System.out.println("");
			System.out.println("Bucket Size |      Count");
			System.out.println("------------------------");
			
			int i = 0;
			while (bucketSizeIterator.hasNext())
			{
				int bucketSize = bucketSizeIterator.next();
				if (bucketSize != 0)
				{
					int countForBucketSize = levelMap.get(bucketSize);
					bucketCountInLevel += countForBucketSize;
					
					if (levelMap.size() < 20 || i < 3 || i >= (levelMap.size() -3))
					{
						System.out.printf(" %10d | %10d\n",bucketSize,countForBucketSize);
					} else if (levelMap.size() / 2 == i)
					{
						System.out.println("         .. |         ..");
						System.out.printf(" %10d | %10d\n",bucketSize,countForBucketSize);
						System.out.println("         .. |         ..");
					}
					i++;
				}
			}
			
			System.out.println("");
			System.out.println("Number of non-empty buckets in level: " + bucketCountInLevel);
			System.out.println("Number of empty buckets in level    : " + levelMap.get(0));
			System.out.println("Number of different bucket sizes    : " + (levelMap.size()-1));
			System.out.println("");
			System.out.println("");
			System.out.println("");
		}
	}
	
	/**
	 * Create histogram over bucket sizes
	 * 
	 * @param map Map to be analyzed
	 * @param level Level on which the map is located in
	 * @return The total count of hashed values in the map 
	 */
	private int collectStatistics(HashMap<Integer, Object> map, int level)
	{
		SortedMap<Integer, Integer> bucketSizesForLevel = bucketSizesPerLevel.get(level);

		Iterator<Object> bucketIterator = map.values().iterator();
		int bucketCount = 0;
		int totalValueCount = 0;
		
		while (bucketIterator.hasNext())
		{
			bucketCount++;
			
			Integer hashValuesInBucket;
			//If we are already on the deepest level, get the count in the bucket, otherwise
			//recursively examine the subtree 
			if (level == maxLevel-1)
			{
				hashValuesInBucket = (Integer) bucketIterator.next();	
			}
			else
			{
				@SuppressWarnings("unchecked")
				HashMap<Integer, Object> nextMap = (HashMap<Integer, Object>) bucketIterator.next();
				hashValuesInBucket = collectStatistics(nextMap, level+1);
			}
			totalValueCount += hashValuesInBucket;
			Integer countOfBucketSizes = bucketSizesForLevel.get(hashValuesInBucket);
			if (countOfBucketSizes == null)
			{
				countOfBucketSizes = 1;
			}
			else
			{
				countOfBucketSizes += 1;
			}
			bucketSizesForLevel.put(hashValuesInBucket, countOfBucketSizes);
		}
		
		Integer countOfEmptyBuckets = bucketSizesForLevel.get(0);
		if (countOfEmptyBuckets == null)
		{
			countOfEmptyBuckets = rangeCalculators[level].getBucketCount() - bucketCount;
		}
		else
		{
			countOfEmptyBuckets += rangeCalculators[level].getBucketCount() - bucketCount;
		}
		bucketSizesForLevel.put(0, countOfEmptyBuckets);
		
		return totalValueCount;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		RangeCalculator[] rt = {new LastBitsToRange(8), 
				new LastBitsToRange(3), 
				new LastBitsToRange(3)};
		HashTester ht = new HashTester(new RotateHashFunction(),
				new StepRangeIterator(-300000, 300000, 19),
				rt);
		BucketBoundaries[] boundaries = {new BucketBoundaries(90, 150, 5, 0.05), 
		new BucketBoundaries(0, 30, 5, 0.1),
		new BucketBoundaries(0, 10, 5000, 0.1)};
		ht.runTest(boundaries);
	}
	
	static class BucketBoundaries
	{
		private int lowerBound;
		private int upperBound;
		private int maxEmpty;
		private double percentOutOfRange;
		public BucketBoundaries(int lowerBound, int upperBound, int maxEmpty, double percentOutOfRange) {
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
			this.maxEmpty = maxEmpty;
			this.percentOutOfRange = percentOutOfRange;
		}

		public int getLowerBound() {
			return lowerBound;
		}
		public int getUpperBound() {
			return upperBound;
		}
		public int getMaxEmpty() {
			return maxEmpty;
		}
		public double getPercentOutOfRange() {
			return percentOutOfRange;
		}
	}

}
