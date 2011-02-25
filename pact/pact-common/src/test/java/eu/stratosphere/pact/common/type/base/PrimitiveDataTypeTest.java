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
		// test value retrieval
		Assert.assertEquals("This is a test", string0.toString());
		// test value comparison
		PactString string1 = new PactString("This is a test");
		PactString string2 = new PactString("This is a tesa");
		PactString string3 = new PactString("This is a tesz");
		Assert.assertTrue(string0.compareTo(string0) == 0);
		Assert.assertTrue(string0.compareTo(string1) == 0);
		Assert.assertTrue(string0.compareTo(string2) > 0);
		Assert.assertTrue(string0.compareTo(string3) < 0);

		// test stream out/input
		try {
			string0.write(mOut);
			string2.write(mOut);
			string3.write(mOut);
			PactString string1n = new PactString();
			PactString string2n = new PactString();
			PactString string3n = new PactString();
			string1n.read(mIn);
			string2n.read(mIn);
			string3n.read(mIn);
			Assert.assertEquals(string0.compareTo(string1n), 0);
			Assert.assertEquals(string0.toString(), string1n.toString());
			Assert.assertEquals(string2.compareTo(string2n), 0);
			Assert.assertEquals(string2.toString(), string2n.toString());
			Assert.assertEquals(string3.compareTo(string3n), 0);
			Assert.assertEquals(string3.toString(), string3n.toString());
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
	}

}
