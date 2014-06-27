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

import org.junit.Test;

public class Tuple2Test {
	

	@Test
	public void testSwapValues() {
		Tuple2<String, Integer> fullTuple2 = new Tuple2<String, Integer>(new String("Test case"), 25);
		// Swapped tuple for comparison
		Tuple2<Integer, String> swappedTuple2 = fullTuple2.swapValues();
		
		Assert.failNotEquals("Must be equal", swappedTuple2.f1, fullTuple2.f0);
		Assert.failNotEquals("Must be equal", swappedTuple2.f0, fullTuple2.f1);
		
		// Assert when not the same
		// Use overloaded equals method to check for equality. Especially important for String.
		if(!swappedTuple2.f1.equals(fullTuple2.f0)) {
			Assert.fail("Swapped values should be the same");
		}
		
		if(!swappedTuple2.f0.equals(fullTuple2.f1)) {
			Assert.fail("Swapped values should be the same");
		}
		
	}

}
