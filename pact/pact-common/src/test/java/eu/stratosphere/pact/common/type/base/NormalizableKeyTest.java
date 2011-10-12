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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class NormalizableKeyTest {

	private DataOutputStream mOut;

	private DataInputStream mIn;

	@Before
	public void setup() {
		try {
			PipedInputStream input = new PipedInputStream(1000);
			mIn = new DataInputStream(input);
			mOut = new DataOutputStream(new PipedOutputStream(input));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPactInteger() {
		PactInteger int0 = new PactInteger(10);
		// test value retrieval
		Assert.assertEquals(10, int0.getValue());
		// test value comparison
		PactInteger int1 = new PactInteger(10);
		PactInteger int2 = new PactInteger(-10);
		PactInteger int3 = new PactInteger(255);
		for (int length = 2; length <= 4; length++) {
			assertNormalizableKey(int0, int1, length);
			assertNormalizableKey(int0, int2, length);
			assertNormalizableKey(int0, int3, length);
		}
		
		
		// test stream output and retrieval
		try {
			int0.write(mOut);
			int2.write(mOut);
			int3.write(mOut);
			PactInteger int1n = new PactInteger();
			PactInteger int2n = new PactInteger();
			PactInteger int3n = new PactInteger();
			int1n.read(mIn);
			int2n.read(mIn);
			int3n.read(mIn);
			for (int length = 2; length <= 4; length++) {
				assertNormalizableKey(int0, int1n, length);
				assertNormalizableKey(int0, int2n, length);
				assertNormalizableKey(int0, int3n, length);
			}
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void testPactLong() {
		PactLong long0 = new PactLong(10);
		// test value retrieval
		Assert.assertEquals(10, long0.getValue());
		// test value comparison
		PactLong long1 = new PactLong(10);
		PactLong long2 = new PactLong(-10);
		PactLong long3 = new PactLong(255);
		for (int length = 2; length <= 8; length++) {
			assertNormalizableKey(long0, long1, length);
			assertNormalizableKey(long0, long2, length);
			assertNormalizableKey(long0, long3, length);
		}
		
		
		// test stream output and retrieval
		try {
			long0.write(mOut);
			long2.write(mOut);
			long3.write(mOut);
			PactLong long1n = new PactLong();
			PactLong long2n = new PactLong();
			PactLong long3n = new PactLong();
			long1n.read(mIn);
			long2n.read(mIn);
			long3n.read(mIn);
			for (int length = 2; length <= 8; length++) {
				assertNormalizableKey(long0, long1n, length);
				assertNormalizableKey(long0, long2n, length);
				assertNormalizableKey(long0, long3n, length);
			}
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
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
	
		// test stream out/input
		try {
			string0.write(mOut);
			string4.write(mOut);
			string2.write(mOut);
			string3.write(mOut);
			PactString string1n = new PactString();
			PactString string2n = new PactString();
			PactString string3n = new PactString();
			PactString string4n = new PactString();
			string1n.read(mIn);
			string4n.read(mIn);
			string2n.read(mIn);
			string3n.read(mIn);
			
			for (int length = 5; length <= 15; length+=10) {
				assertNormalizableKey(string0, string1n, length);			
				assertNormalizableKey(string0, string2n, length);
				assertNormalizableKey(string0, string3n, length);
				assertNormalizableKey(string0, string4n, length);
			}
			
			string2.setValue("This");
			assertNormalizableKey(string2, string3, 32);
			
			
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
	}
	
	@Test
	public void testPactNull() {
		
		final PactNull pn1 = new PactNull();
		final PactNull pn2 = new PactNull();
		
		
		assertNormalizableKey(pn1, pn2, 0);
		
		// test serialization
		final PactNull pn = new PactNull();
		
		try {
			pn.write(mOut);
			pn.read(mIn);
			assertNormalizableKey(pn, pn1, 0);
		}
		catch (IOException ioex) {
			Assert.fail("An exception occurred in the testcase: " + ioex.getMessage());
		}
	}
	
	private void assertNormalizableKey(Key key1, Key key2, int len) {
		
		byte[] normalizedKeys = new byte[2*len]; 
		
		((NormalizableKey)key1).copyNormalizedKey(normalizedKeys, 0, len);
		((NormalizableKey)key2).copyNormalizedKey(normalizedKeys, len, len);
		
		for (int i = 0; i < len; i++) {
			int comp;
			if ((comp = (normalizedKeys[i] - normalizedKeys[len+i])) != 0) {
				if (Math.signum(key1.compareTo(key2)) != Math.signum(comp)) {
					Assert.fail("Normalized key comparison differs from actual key comparision");
				}
				return;
			}
		}
		if (key1.compareTo(key2) != 0 && ((NormalizableKey)key1).getMaxNormalizedKeyLen() <= len) {
			Assert.fail("Normalized key was not able to distinguish keys, " +
					"although it should as the length of it sufficies to uniquely identify them");
		}
		
	}

}
