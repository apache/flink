package eu.stratosphere.pact.runtime.hash;

public interface RangeCalculator {

	public int getBucket(int hash);
	
	public int getBucketCount();
	
}
