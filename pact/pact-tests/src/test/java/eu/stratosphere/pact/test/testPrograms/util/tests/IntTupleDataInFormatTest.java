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

package eu.stratosphere.pact.test.testPrograms.util.tests;

import org.eclipse.jdt.internal.core.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.test.testPrograms.util.IntTupleDataInFormat;
import eu.stratosphere.pact.test.testPrograms.util.Tuple;

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
		PactRecord rec = new PactRecord();	
		
		for(int i = 0; i < testTuples.length; i++) {
			
			byte[] tupleBytes = testTuples[i].getBytes();
			
			inFormat.readRecord(rec, tupleBytes, tupleBytes.length);
			
			Assert.isTrue(rec.getField(0, PactInteger.class).equals(new PactInteger(expectedKeys[i])) , "Expected Key: "+expectedKeys[i]+" != Returned Key: "+rec.getField(0, PactInteger.class));
			Assert.isTrue(rec.getField(1, Tuple.class).getNumberOfColumns() == expectedAttrCnt[i], "Expected Attr Cnt: "+expectedAttrCnt[i]+" != Returned Attr Cnt: "+rec.getField(1, Tuple.class));
		}

	}
	
	

}
