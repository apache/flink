/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
	
	@Test
	public void testRoundDownToPowerOf2()
	{
		assertEquals(0, MathUtils.roundDownToPowerOf2(0));
		assertEquals(1, MathUtils.roundDownToPowerOf2(1));
		assertEquals(2, MathUtils.roundDownToPowerOf2(2));
		assertEquals(2, MathUtils.roundDownToPowerOf2(3));
		assertEquals(4, MathUtils.roundDownToPowerOf2(4));
		assertEquals(4, MathUtils.roundDownToPowerOf2(5));
		assertEquals(4, MathUtils.roundDownToPowerOf2(6));
		assertEquals(4, MathUtils.roundDownToPowerOf2(7));
		assertEquals(8, MathUtils.roundDownToPowerOf2(8));
		assertEquals(8, MathUtils.roundDownToPowerOf2(9));
		assertEquals(8, MathUtils.roundDownToPowerOf2(15));
		assertEquals(16, MathUtils.roundDownToPowerOf2(16));
		assertEquals(16, MathUtils.roundDownToPowerOf2(17));
		assertEquals(16, MathUtils.roundDownToPowerOf2(31));
		assertEquals(32, MathUtils.roundDownToPowerOf2(32));
		assertEquals(32, MathUtils.roundDownToPowerOf2(33));
		assertEquals(32, MathUtils.roundDownToPowerOf2(42));
		assertEquals(32, MathUtils.roundDownToPowerOf2(63));
		assertEquals(64, MathUtils.roundDownToPowerOf2(64));
		assertEquals(64, MathUtils.roundDownToPowerOf2(125));
		assertEquals(16384, MathUtils.roundDownToPowerOf2(25654));
		assertEquals(33554432, MathUtils.roundDownToPowerOf2(34366363));
		assertEquals(33554432, MathUtils.roundDownToPowerOf2(63463463));
		assertEquals(1073741824, MathUtils.roundDownToPowerOf2(1852987883));
		assertEquals(1073741824, MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE));
	}
}
