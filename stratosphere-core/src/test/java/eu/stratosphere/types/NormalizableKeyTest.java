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

package eu.stratosphere.types;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.NormalizableKey;
import eu.stratosphere.types.PactCharacter;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.types.PactNull;
import eu.stratosphere.types.PactString;

public class NormalizableKeyTest {

	@Test
	public void testPactInteger() {
		PactInteger int0 = new PactInteger(10);
		PactInteger int1 = new PactInteger(10);
		PactInteger int2 = new PactInteger(-10);
		PactInteger int3 = new PactInteger(255);
		PactInteger int4 = new PactInteger(Integer.MAX_VALUE);
		PactInteger int5 = new PactInteger(Integer.MAX_VALUE & 0xff800000);
		PactInteger int6 = new PactInteger(Integer.MIN_VALUE);
		PactInteger int7 = new PactInteger(Integer.MIN_VALUE & 0xff800000);
		
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
	public void testPactLong() {
		PactLong long0 = new PactLong(10);
		PactLong long1 = new PactLong(10);
		PactLong long2 = new PactLong(-10);
		PactLong long3 = new PactLong(255);
		PactLong long4 = new PactLong(Long.MAX_VALUE);
		PactLong long5 = new PactLong(Long.MAX_VALUE & 0xff80000000000000L);
		PactLong long6 = new PactLong(Long.MIN_VALUE);
		PactLong long7 = new PactLong(Long.MIN_VALUE & 0xff80000000000000L);
		
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
	public void testPactString() {
		PactString string0 = new PactString("This is a test");
		PactString string1 = new PactString("This is a test with some longer String");
		PactString string2 = new PactString("This is a tesa");
		PactString string3 = new PactString("This");
		PactString string4 = new PactString("Ünlaut ßtring µ avec é y ¢");
		
		for (int length = 5; length <= 15; length+=10) {
			assertNormalizableKey(string0, string1, length);
			assertNormalizableKey(string0, string2, length);
			assertNormalizableKey(string0, string3, length);
			assertNormalizableKey(string0, string4, length);
		}
	}
	
	@Test
	public void testPactNull() {
		
		final PactNull pn1 = new PactNull();
		final PactNull pn2 = new PactNull();
		
		assertNormalizableKey(pn1, pn2, 0);
	}
	
	@Test
	public void testPactChar() {
		
		final PactCharacter c1 = new PactCharacter((char) 0);
		final PactCharacter c2 = new PactCharacter((char) 1);
		final PactCharacter c3 = new PactCharacter((char) 0xff);
		final PactCharacter c4 = new PactCharacter(Character.MAX_VALUE);
		final PactCharacter c5 = new PactCharacter((char) (Character.MAX_VALUE + (char) 1));
		final PactCharacter c6 = new PactCharacter(Character.MAX_HIGH_SURROGATE);
		final PactCharacter c7 = new PactCharacter(Character.MAX_LOW_SURROGATE);
		final PactCharacter c8 = new PactCharacter(Character.MAX_SURROGATE);
		
		PactCharacter[] allChars = new PactCharacter[] {
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
