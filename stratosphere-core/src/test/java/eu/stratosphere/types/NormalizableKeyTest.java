/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.types;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;

public class NormalizableKeyTest {

	@Test
	public void testIntValue() {
		IntValue int0 = new IntValue(10);
		IntValue int1 = new IntValue(10);
		IntValue int2 = new IntValue(-10);
		IntValue int3 = new IntValue(255);
		IntValue int4 = new IntValue(Integer.MAX_VALUE);
		IntValue int5 = new IntValue(Integer.MAX_VALUE & 0xff800000);
		IntValue int6 = new IntValue(Integer.MIN_VALUE);
		IntValue int7 = new IntValue(Integer.MIN_VALUE & 0xff800000);
		
		for (int length = 2; length <= 4; length++) {
			assertNormalizableKey(int0, int1, length);
			assertNormalizableKey(int0, int2, length);
			assertNormalizableKey(int0, int3, length);
			assertNormalizableKey(int0, int4, length);
			assertNormalizableKey(int0, int5, length);
			assertNormalizableKey(int0, int6, length);
			assertNormalizableKey(int0, int7, length);
			assertNormalizableKey(int4, int5, length);
			assertNormalizableKey(int6, int7, length);
		}
	}
	
	@Test
	public void testLongValue() {
		LongValue long0 = new LongValue(10);
		LongValue long1 = new LongValue(10);
		LongValue long2 = new LongValue(-10);
		LongValue long3 = new LongValue(255);
		LongValue long4 = new LongValue(Long.MAX_VALUE);
		LongValue long5 = new LongValue(Long.MAX_VALUE & 0xff80000000000000L);
		LongValue long6 = new LongValue(Long.MIN_VALUE);
		LongValue long7 = new LongValue(Long.MIN_VALUE & 0xff80000000000000L);
		
		for (int length = 2; length <= 8; length++) {
			assertNormalizableKey(long0, long1, length);
			assertNormalizableKey(long0, long2, length);
			assertNormalizableKey(long0, long3, length);
			assertNormalizableKey(long0, long4, length);
			assertNormalizableKey(long0, long5, length);
			assertNormalizableKey(long0, long6, length);
			assertNormalizableKey(long0, long7, length);
			assertNormalizableKey(long4, long5, length);
			assertNormalizableKey(long6, long7, length);
		}
	}

	

	@Test
	public void testStringValue() {
		StringValue string0 = new StringValue("This is a test");
		StringValue string1 = new StringValue("This is a test with some longer String");
		StringValue string2 = new StringValue("This is a tesa");
		StringValue string3 = new StringValue("This");
		StringValue string4 = new StringValue("Ünlaut ßtring µ avec é y ¢");
		
		for (int length = 5; length <= 15; length+=10) {
			assertNormalizableKey(string0, string1, length);
			assertNormalizableKey(string0, string2, length);
			assertNormalizableKey(string0, string3, length);
			assertNormalizableKey(string0, string4, length);
		}
	}
	
	@Test
	public void testPactNull() {
		
		final NullValue pn1 = new NullValue();
		final NullValue pn2 = new NullValue();
		
		assertNormalizableKey(pn1, pn2, 0);
	}
	
	@Test
	public void testPactChar() {
		
		final CharValue c1 = new CharValue((char) 0);
		final CharValue c2 = new CharValue((char) 1);
		final CharValue c3 = new CharValue((char) 0xff);
		final CharValue c4 = new CharValue(Character.MAX_VALUE);
		final CharValue c5 = new CharValue((char) (Character.MAX_VALUE + (char) 1));
		final CharValue c6 = new CharValue(Character.MAX_HIGH_SURROGATE);
		final CharValue c7 = new CharValue(Character.MAX_LOW_SURROGATE);
		final CharValue c8 = new CharValue(Character.MAX_SURROGATE);
		
		CharValue[] allChars = new CharValue[] {
			c1, c2, c3, c4, c5, c6, c7, c8 };
		
		for (int i = 0; i < 5; i++) {
			// self checks
			for (int x = 0; x < allChars.length; x++) {
				for (int y = 0; y < allChars.length; y++) {
					assertNormalizableKey(allChars[x], allChars[y], i);
				}
			}
		}
	}
	
	private void assertNormalizableKey(NormalizableKey key1, NormalizableKey key2, int len) {
		
		byte[] normalizedKeys = new byte[2*len];
		MemorySegment wrapper = new MemorySegment(normalizedKeys);
		
		key1.copyNormalizedKey(wrapper, 0, len);
		key2.copyNormalizedKey(wrapper, len, len);
		
		for (int i = 0; i < len; i++) {
			int comp;
			int normKey1 = normalizedKeys[i] & 0xFF;
			int normKey2 = normalizedKeys[len + i] & 0xFF;
			
			if ((comp = (normKey1 - normKey2)) != 0) {
				if (Math.signum(key1.compareTo(key2)) != Math.signum(comp)) {
					Assert.fail("Normalized key comparison differs from actual key comparision");
				}
				return;
			}
		}
		if (key1.compareTo(key2) != 0 && key1.getMaxNormalizedKeyLen() <= len) {
			Assert.fail("Normalized key was not able to distinguish keys, " +
					"although it should as the length of it sufficies to uniquely identify them");
		}
	}

}
