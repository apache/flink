package eu.stratosphere.pact.runtime.hash;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import eu.stratosphere.pact.runtime.hash.HashTester.BucketBoundaries;

public class MultiLevelHashTest {

	private static final Log LOG = LogFactory.getLog(MultiLevelHashTest.class);

	
	@Test
	public void testStepSeventeen() {
		
		//Define numbers of buckets on each level 
		RangeCalculator[] rangeCalculators = {new LastBitsToRange(8), //2^8=256 Buckets on level 0 
			new LastBitsToRange(3), 					//2^3=8 Buckets on level 1
			new LastBitsToRange(3)};					//2^3=8 Buckets on level 2
		
		
		
		Iterator<Integer> importIterator = new StepRangeIterator(-300000, 300000, 17);
		
		
		HashFunction hashFunction = new RotateHashFunction();
		
		
		HashTester ht = new HashTester(hashFunction ,
			importIterator,
			rangeCalculators);
		
		
		BucketBoundaries[] boundaries = {new BucketBoundaries(90, 150, 5, 0.05), 
		new BucketBoundaries(0, 30, 5, 0.1),
		new BucketBoundaries(0, 10, 5000, 0.1)};
		
		
		ht.runTest(boundaries);
	}
}
