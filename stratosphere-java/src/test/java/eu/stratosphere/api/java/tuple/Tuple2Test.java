/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.api.java.tuple;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class Tuple2Test {
	
	private Tuple2<String, Integer> classUnderTestTuple2;

	@Before
	public void setUp() throws Exception {
		classUnderTestTuple2 = new Tuple2<String, Integer>(new String("Test case"), 25);
	}

	@Test
	public void testSwapValues() {
		// Tuple 2 is instantiated with String and Integer
		String stringBeforeSwap = classUnderTestTuple2.f0;
		Integer intBeforeSwap = classUnderTestTuple2.f1;
		
		// Swap the values
		classUnderTestTuple2.swapValues();
		
		// Check if values are really swapped and is really the same (Object.equals);
		
		// Assert when not the same
		// Use overloaded equals method to check for equality. Especially important for String
		if(!classUnderTestTuple2.getField(0).equals(intBeforeSwap)) {
			Assert.fail();
		}
		
		if(!classUnderTestTuple2.getField(1).equals(stringBeforeSwap)) {
			Assert.fail();
		}
		
	}

}
