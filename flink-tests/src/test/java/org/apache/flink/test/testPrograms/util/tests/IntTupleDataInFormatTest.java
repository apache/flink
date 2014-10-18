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


package org.apache.flink.test.testPrograms.util.tests;

import org.apache.flink.test.recordJobs.util.IntTupleDataInFormat;
import org.apache.flink.test.recordJobs.util.Tuple;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.junit.Assert;
import org.junit.Test;

public class IntTupleDataInFormatTest
{
	@Test
	public void testReadLineKeyValuePairOfIntValueTupleByteArray() {
		
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
		Record rec = new Record();	
		
		for(int i = 0; i < testTuples.length; i++) {
			
			byte[] tupleBytes = testTuples[i].getBytes();
			
			inFormat.readRecord(rec, tupleBytes, 0, tupleBytes.length);
			
			Assert.assertTrue("Expected Key: "+expectedKeys[i]+" != Returned Key: "+rec.getField(0, IntValue.class), rec.getField(0, IntValue.class).equals(new IntValue(expectedKeys[i])));
			Assert.assertTrue("Expected Attr Cnt: "+expectedAttrCnt[i]+" != Returned Attr Cnt: "+rec.getField(1, Tuple.class), rec.getField(1, Tuple.class).getNumberOfColumns() == expectedAttrCnt[i]);
		}
	}
}
