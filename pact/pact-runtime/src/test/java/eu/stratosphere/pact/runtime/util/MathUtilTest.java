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

package eu.stratosphere.pact.runtime.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class MathUtilTest
{
	@Test
	public void testLog2Computation()
	{
		assertEquals(0, MathUtils.log2floor(1));
		assertEquals(1, MathUtils.log2floor(2));
		assertEquals(1, MathUtils.log2floor(3));
		assertEquals(2, MathUtils.log2floor(4));
		assertEquals(2, MathUtils.log2floor(5));
		assertEquals(2, MathUtils.log2floor(7));
		assertEquals(3, MathUtils.log2floor(8));
		assertEquals(3, MathUtils.log2floor(9));
		assertEquals(4, MathUtils.log2floor(16));
		assertEquals(4, MathUtils.log2floor(17));
		assertEquals(13, MathUtils.log2floor((0x1 << 13) + 1));
		assertEquals(30, MathUtils.log2floor(Integer.MAX_VALUE));
		assertEquals(31, MathUtils.log2floor(-1));
		
		try {
			MathUtils.log2floor(0);
			fail();
		}
		catch (ArithmeticException aex) {}
	}
}
