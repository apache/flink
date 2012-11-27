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

package eu.stratosphere.pact.common.type.base;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class PrimitiveDataTypeTest {

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
		PactInteger int3 = new PactInteger(20);
		Assert.assertEquals(int0.compareTo(int0), 0);
		Assert.assertEquals(int0.compareTo(int1), 0);
		Assert.assertEquals(int0.compareTo(int2), 1);
		Assert.assertEquals(int0.compareTo(int3), -1);
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
			Assert.assertEquals(int0.compareTo(int1n), 0);
			Assert.assertEquals(int0.getValue(), int1n.getValue());
			Assert.assertEquals(int2.compareTo(int2n), 0);
			Assert.assertEquals(int2.getValue(), int2n.getValue());
			Assert.assertEquals(int3.compareTo(int3n), 0);
			Assert.assertEquals(int3.getValue(), int3n.getValue());
		}
		catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testPactDouble() {
		PactDouble double0 = new PactDouble(10.2);
		// test value retrieval
		Assert.assertEquals(10.2, double0.getValue());
		// test value comparison
		PactDouble double1 = new PactDouble(10.2);
		PactDouble double2 = new PactDouble(-10.5);
		PactDouble double3 = new PactDouble(20.2);
		Assert.assertEquals(double0.compareTo(double0), 0);
		Assert.assertEquals(double0.compareTo(double1), 0);
		Assert.assertEquals(double0.compareTo(double2), 1);
		Assert.assertEquals(double0.compareTo(double3), -1);
		// test stream output and retrieval
		try {
			double0.write(mOut);
			double2.write(mOut);
			double3.write(mOut);
			PactDouble double1n = new PactDouble();
			PactDouble double2n = new PactDouble();
			PactDouble double3n = new PactDouble();
			double1n.read(mIn);
			double2n.read(mIn);
			double3n.read(mIn);
			Assert.assertEquals(double0.compareTo(double1n), 0);
			Assert.assertEquals(double0.getValue(), double1n.getValue());
			Assert.assertEquals(double2.compareTo(double2n), 0);
			Assert.assertEquals(double2.getValue(), double2n.getValue());
			Assert.assertEquals(double3.compareTo(double3n), 0);
			Assert.assertEquals(double3.getValue(), double3n.getValue());
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
	}

	@Test
	public void testPactString() {
		PactString string0 = new PactString("This is a test");
		PactString stringThis = new PactString("This");
		PactString stringIsA = new PactString("is a");
		// test value retrieval
		Assert.assertEquals("This is a test", string0.toString());
		// test value comparison
		PactString string1 = new PactString("This is a test");
		PactString string2 = new PactString("This is a tesa");
		PactString string3 = new PactString("This is a tesz");
		PactString string4 = new PactString("Ünlaut ßtring µ avec é y ¢");
		CharSequence chars5 = string1.subSequence(0, 4);
		PactString string5 = (PactString) chars5;
		PactString string6 = (PactString) string0.subSequence(0, string0.length());
		PactString string7 = (PactString) string0.subSequence(5, 9);
		PactString string8 = (PactString) string0.subSequence(0, 0);
		Assert.assertTrue(string0.compareTo(string0) == 0);
		Assert.assertTrue(string0.compareTo(string1) == 0);
		Assert.assertTrue(string0.compareTo(string2) > 0);
		Assert.assertTrue(string0.compareTo(string3) < 0);
		Assert.assertTrue(stringThis.equals(chars5));
		Assert.assertTrue(stringThis.compareTo(string5) == 0);
		Assert.assertTrue(string0.compareTo(string6) == 0);
		Assert.assertTrue(stringIsA.compareTo(string7) == 0);
		string7.setValue("This is a test");
		Assert.assertTrue(stringIsA.compareTo(string7) > 0);
		Assert.assertTrue(string0.compareTo(string7) == 0);
		string7.setValue("is a");
		Assert.assertTrue(stringIsA.compareTo(string7) == 0);
		Assert.assertTrue(string0.compareTo(string7) < 0);
		Assert.assertEquals(stringIsA.hashCode(), string7.hashCode());
		Assert.assertEquals(string7.length(), 4);
		Assert.assertEquals("is a", string7.getValue());
		Assert.assertEquals(string8.length(), 0);
		Assert.assertEquals("", string8.getValue());
		Assert.assertEquals('s', string7.charAt(1));
		try {
			string7.charAt(5);
			Assert.fail("Exception should have been thrown when accessing characters out of bounds.");
		} catch (IndexOutOfBoundsException iOOBE) {}
		
		// test stream out/input
		try {
			string0.write(mOut);
			string4.write(mOut);
			string2.write(mOut);
			string3.write(mOut);
			string7.write(mOut);
			PactString string1n = new PactString();
			PactString string2n = new PactString();
			PactString string3n = new PactString();
			PactString string4n = new PactString();
			PactString string7n = new PactString();
			string1n.read(mIn);
			string4n.read(mIn);
			string2n.read(mIn);
			string3n.read(mIn);
			string7n.read(mIn);
			Assert.assertEquals(string0.compareTo(string1n), 0);
			Assert.assertEquals(string0.toString(), string1n.toString());
			Assert.assertEquals(string4.compareTo(string4n), 0);
			Assert.assertEquals(string4.toString(), string4n.toString());
			Assert.assertEquals(string2.compareTo(string2n), 0);
			Assert.assertEquals(string2.toString(), string2n.toString());
			Assert.assertEquals(string3.compareTo(string3n), 0);
			Assert.assertEquals(string3.toString(), string3n.toString());
			Assert.assertEquals(string7.compareTo(string7n), 0);
			Assert.assertEquals(string7.toString(), string7n.toString());
			try {
				string7n.charAt(5);
				Assert.fail("Exception should have been thrown when accessing characters out of bounds.");
			} catch (IndexOutOfBoundsException iOOBE) {}
			
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
	}
	
	@Test
	public void testPactNull() {
		
		final PactNull pn1 = new PactNull();
		final PactNull pn2 = new PactNull();
		
		Assert.assertEquals("PactNull not equal to other PactNulls.", pn1, pn2);
		Assert.assertEquals("PactNull not equal to other PactNulls.", pn2, pn1);
		
		Assert.assertFalse("PactNull equal to other null.", pn1.equals(null));
		
		// test serialization
		final PactNull pn = new PactNull();
		final int numWrites = 13;
		
		try {
			// write it multiple times
			for (int i = 0; i < numWrites; i++) {
				pn.write(mOut);
			}
			
			// read it multiple times
			for (int i = 0; i < numWrites; i++) {
				pn.read(mIn);
			}
			
			Assert.assertEquals("Reading PactNull does not consume the same data as was written.", mIn.available(), 0);
		}
		catch (IOException ioex) {
			Assert.fail("An exception occurred in the testcase: " + ioex.getMessage());
		}
	}

}
