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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import eu.stratosphere.core.memory.InputViewDataInputStreamWrapper;
import eu.stratosphere.core.memory.OutputViewDataOutputStreamWrapper;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

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
	public void testIntValue() {
		IntValue int0 = new IntValue(10);
		// test value retrieval
		Assert.assertEquals(10, int0.getValue());
		// test value comparison
		IntValue int1 = new IntValue(10);
		IntValue int2 = new IntValue(-10);
		IntValue int3 = new IntValue(20);
		Assert.assertEquals(int0.compareTo(int0), 0);
		Assert.assertEquals(int0.compareTo(int1), 0);
		Assert.assertEquals(int0.compareTo(int2), 1);
		Assert.assertEquals(int0.compareTo(int3), -1);
		// test stream output and retrieval
		try {
			int0.write(new OutputViewDataOutputStreamWrapper(mOut));
			int2.write(new OutputViewDataOutputStreamWrapper(mOut));
			int3.write(new OutputViewDataOutputStreamWrapper(mOut));
			IntValue int1n = new IntValue();
			IntValue int2n = new IntValue();
			IntValue int3n = new IntValue();
			int1n.read(new InputViewDataInputStreamWrapper(mIn));
			int2n.read(new InputViewDataInputStreamWrapper(mIn));
			int3n.read(new InputViewDataInputStreamWrapper(mIn));
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
	public void testDoubleValue() {
		DoubleValue double0 = new DoubleValue(10.2);
		// test value retrieval
		Assert.assertEquals(10.2, double0.getValue());
		// test value comparison
		DoubleValue double1 = new DoubleValue(10.2);
		DoubleValue double2 = new DoubleValue(-10.5);
		DoubleValue double3 = new DoubleValue(20.2);
		Assert.assertEquals(double0.compareTo(double0), 0);
		Assert.assertEquals(double0.compareTo(double1), 0);
		Assert.assertEquals(double0.compareTo(double2), 1);
		Assert.assertEquals(double0.compareTo(double3), -1);
		// test stream output and retrieval
		try {
			double0.write(new OutputViewDataOutputStreamWrapper(mOut));
			double2.write(new OutputViewDataOutputStreamWrapper(mOut));
			double3.write(new OutputViewDataOutputStreamWrapper(mOut));
			DoubleValue double1n = new DoubleValue();
			DoubleValue double2n = new DoubleValue();
			DoubleValue double3n = new DoubleValue();
			double1n.read(new InputViewDataInputStreamWrapper(mIn));
			double2n.read(new InputViewDataInputStreamWrapper(mIn));
			double3n.read(new InputViewDataInputStreamWrapper(mIn));
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
	public void testStringValue() {
		StringValue string0 = new StringValue("This is a test");
		StringValue stringThis = new StringValue("This");
		StringValue stringIsA = new StringValue("is a");
		// test value retrieval
		Assert.assertEquals("This is a test", string0.toString());
		// test value comparison
		StringValue string1 = new StringValue("This is a test");
		StringValue string2 = new StringValue("This is a tesa");
		StringValue string3 = new StringValue("This is a tesz");
		StringValue string4 = new StringValue("Ünlaut ßtring µ avec é y ¢");
		CharSequence chars5 = string1.subSequence(0, 4);
		StringValue string5 = (StringValue) chars5;
		StringValue string6 = (StringValue) string0.subSequence(0, string0.length());
		StringValue string7 = (StringValue) string0.subSequence(5, 9);
		StringValue string8 = (StringValue) string0.subSequence(0, 0);
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
			string0.write(new OutputViewDataOutputStreamWrapper(mOut));
			string4.write(new OutputViewDataOutputStreamWrapper(mOut));
			string2.write(new OutputViewDataOutputStreamWrapper(mOut));
			string3.write(new OutputViewDataOutputStreamWrapper(mOut));
			string7.write(new OutputViewDataOutputStreamWrapper(mOut));
			StringValue string1n = new StringValue();
			StringValue string2n = new StringValue();
			StringValue string3n = new StringValue();
			StringValue string4n = new StringValue();
			StringValue string7n = new StringValue();
			string1n.read(new InputViewDataInputStreamWrapper(mIn));
			string4n.read(new InputViewDataInputStreamWrapper(mIn));
			string2n.read(new InputViewDataInputStreamWrapper(mIn));
			string3n.read(new InputViewDataInputStreamWrapper(mIn));
			string7n.read(new InputViewDataInputStreamWrapper(mIn));
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
		
		final NullValue pn1 = new NullValue();
		final NullValue pn2 = new NullValue();
		
		Assert.assertEquals("PactNull not equal to other PactNulls.", pn1, pn2);
		Assert.assertEquals("PactNull not equal to other PactNulls.", pn2, pn1);
		
		Assert.assertFalse("PactNull equal to other null.", pn1.equals(null));
		
		// test serialization
		final NullValue pn = new NullValue();
		final int numWrites = 13;
		
		try {
			// write it multiple times
			for (int i = 0; i < numWrites; i++) {
				pn.write(new OutputViewDataOutputStreamWrapper(mOut));
			}
			
			// read it multiple times
			for (int i = 0; i < numWrites; i++) {
				pn.read(new InputViewDataInputStreamWrapper(mIn));
			}
			
			Assert.assertEquals("Reading PactNull does not consume the same data as was written.", mIn.available(), 0);
		}
		catch (IOException ioex) {
			Assert.fail("An exception occurred in the testcase: " + ioex.getMessage());
		}
	}

}
