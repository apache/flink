package eu.stratosphere.pact.example.relational.util;

import org.eclipse.jdt.internal.core.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class IntTupleDataInFormatTest {

	@Test
	public void testReadLineKeyValuePairOfPactIntegerTupleByteArray() {
		
		String[] testTuples = {
			"1|attribute1|attribute2|3|attribute4|5|",
			"2|3|",
			"3|attribute1|attribute2|",
			"-1|attr1|attr2|",
			"-2|attribute1|attribute2|",
			Integer.MAX_VALUE+"|attr1|attr2|attr3|attr4|",
			Integer.MIN_VALUE+"|attr1|attr2|attr3|attr4|"
		};
		
		int[] expectedKeys = {
			1,2,3,-1,-2,Integer.MAX_VALUE,Integer.MIN_VALUE
		};
		
		int[] expectedAttrCnt = {6,2,3,3,3,5,5};

		IntTupleDataInFormat inFormat = new IntTupleDataInFormat();
		KeyValuePair<PactInteger, Tuple> pair = new KeyValuePair<PactInteger, Tuple>();		
		
		for(int i = 0; i < testTuples.length; i++) {
			
			String testTuple = testTuples[i];
			
			inFormat.readLine(pair, testTuple.getBytes());
			
			Assert.isTrue(pair.getKey().getValue() == expectedKeys[i], "Expected Key: "+expectedKeys[i]+" != Returned Key: "+pair.getKey().getValue());
			Assert.isTrue(pair.getValue().getNumberOfColumns() == expectedAttrCnt[i], "Expected Attr Cnt: "+expectedAttrCnt[i]+" != Returned Attr Cnt: "+pair.getValue().getNumberOfColumns());
		}

	}
	
	

}
