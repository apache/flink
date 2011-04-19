package eu.stratosphere.pact.runtime.hash;

public class LastBitsToRange implements RangeCalculator {

	public final int mask;
	public final int bucketCount;
	
	public LastBitsToRange(int numberOfLastBits)
	{
		bucketCount = (int)Math.pow(2, numberOfLastBits);
		mask =  bucketCount - 1;
	}
	
	@Override
	public int getBucket(int hash) {
		return hash & mask;
	}
	
	public int getBucketCount()
	{
		return bucketCount;
	}

}
