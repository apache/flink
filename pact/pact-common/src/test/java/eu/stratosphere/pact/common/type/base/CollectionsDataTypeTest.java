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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactMap;
import eu.stratosphere.pact.common.type.base.PactPair;
import eu.stratosphere.pact.common.type.base.PactString;

public class CollectionsDataTypeTest {
	private DataOutputStream out;

	private DataInputStream in;

	@Before
	public void setup() {
		try {
			PipedInputStream input = new PipedInputStream(1000);
			in = new DataInputStream(input);
			out = new DataOutputStream(new PipedOutputStream(input));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPactPair() {
		NfIntStringPair pair1 = new NfIntStringPair();

		pair1.setFirst(new PactInteger(10));
		pair1.setSecond(new PactString("This is a string"));

		// test data retrieval
		Assert.assertEquals(pair1.getFirst(), new PactInteger(10));
		Assert.assertEquals(pair1.getSecond(), new PactString("This is a string"));

		// test serialization
		try {
			NfIntStringPair mPairActual = new NfIntStringPair();

			pair1.write(out);
			mPairActual.read(in);

			Assert.assertEquals(pair1, mPairActual);
		} catch (IOException e) {
			Assert.fail("Unexpected IOException");
		}

		// test comparison
		NfIntStringPair pair2 = new NfIntStringPair();
		NfIntStringPair pair3 = new NfIntStringPair();
		NfIntStringPair pair4 = new NfIntStringPair();
		NfIntStringPair pair5 = new NfIntStringPair();
		NfIntStringPair pair6 = new NfIntStringPair();

		pair2.setFirst(new PactInteger(10));
		pair2.setSecond(new PactString("This is a string"));

		pair3.setFirst(new PactInteger(5));
		pair3.setSecond(new PactString("This is a string"));

		pair4.setFirst(new PactInteger(15));
		pair4.setSecond(new PactString("This is a string"));

		pair5.setFirst(new PactInteger(10));
		pair5.setSecond(new PactString("This is a strina"));

		pair6.setFirst(new PactInteger(10));
		pair6.setSecond(new PactString("This is a strinz"));

		Assert.assertTrue(pair1.compareTo(pair2) == 0);
		Assert.assertTrue(pair1.compareTo(pair3) > 0);
		Assert.assertTrue(pair1.compareTo(pair4) < 0);
		Assert.assertTrue(pair1.compareTo(pair5) > 0);
		Assert.assertTrue(pair1.compareTo(pair6) < 0);

		Assert.assertTrue(pair1.equals(pair2));
		Assert.assertFalse(pair1.equals(pair3));
		Assert.assertFalse(pair1.equals(pair4));
		Assert.assertFalse(pair1.equals(pair5));
		Assert.assertFalse(pair1.equals(pair6));

		// test incorrect comparison
		NfDoubleStringPair mPair7 = new NfDoubleStringPair();
		mPair7.setFirst(new PactDouble(2.3));

		try {
			pair1.compareTo(mPair7);
			Assert.fail();
		} catch (Exception e) {
			Assert.assertTrue(e instanceof ClassCastException);
		}

		// test sorting
		NfIntStringPair[] pairs = new NfIntStringPair[5];

		pairs[0] = pair1;
		pairs[1] = pair2;
		pairs[2] = pair3;
		pairs[3] = pair4;
		pairs[4] = pair5;

		Arrays.sort(pairs);

		NfIntStringPair p1, p2;

		for (int i = 1; i < 5; i++) {
			p1 = pairs[i - 1];
			p2 = pairs[i];

			Assert.assertTrue(p1.compareTo(p2) <= 0);
		}

		// test hashing
		HashSet<NfIntStringPair> pairSet = new HashSet<NfIntStringPair>();

		Assert.assertTrue(pairSet.add(pair2));
		Assert.assertTrue(pairSet.add(pair3));
		Assert.assertTrue(pairSet.add(pair4));
		Assert.assertTrue(pairSet.add(pair5));
		Assert.assertTrue(pairSet.add(pair6));
		Assert.assertFalse(pairSet.add(pair1));

		Assert.assertTrue(pairSet.containsAll(Arrays.asList(pairs)));
	}

	@Test
	public void testPactMap() {
		NfIntStringMap map0 = new NfIntStringMap();
		map0.put(new PactInteger(10), new PactString("20"));

		// test data retrieval
		for (Entry<PactInteger, PactString> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getValue(), new PactString("20"));
			Assert.assertEquals(entry.getKey(), new PactInteger(10));
		}

		// test data overwriting
		map0.put(new PactInteger(10), new PactString("10"));
		for (Entry<PactInteger, PactString> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getValue(), new PactString("10"));
			Assert.assertEquals(entry.getKey(), new PactInteger(10));
		}

		// now test data retrieval of multiple values
		map0.put(new PactInteger(20), new PactString("20"));
		map0.put(new PactInteger(30), new PactString("30"));
		map0.put(new PactInteger(40), new PactString("40"));

		// construct an inverted map
		NfStringIntMap mapinv = new NfStringIntMap();
		for (Entry<PactInteger, PactString> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getKey().getValue(), Integer.parseInt(entry.getValue().toString()));
			mapinv.put(entry.getValue(), entry.getKey());
		}

		for (Entry<PactString, PactInteger> entry : mapinv.entrySet()) {
			Assert.assertEquals(entry.getValue().getValue(), Integer.parseInt(entry.getKey().toString()));
		}

		// now test data transfer
		NfIntStringMap nMap = new NfIntStringMap();
		try {
			map0.write(out);
			nMap.read(in);
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
		for (Entry<PactInteger, PactString> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getKey().getValue(), Integer.parseInt(entry.getValue().toString()));
		}
	}

	@Test
	public void testPactList() {
		NfStringList list = new NfStringList();
		list.add(new PactString("Hello!"));

		for (PactString value : list) {
			Assert.assertEquals(value, new PactString("Hello!"));
		}

		list.add(new PactString("Hello2!"));
		list.add(new PactString("Hello3!"));
		list.add(new PactString("Hello4!"));

		Assert.assertTrue(list.equals(list));

		// test data transfer
		NfStringList mList2 = new NfStringList();
		try {
			list.write(out);
			mList2.read(in);
		} catch (Exception e) {
			Assert.assertTrue(false);
		}
		Assert.assertTrue(list.equals(mList2));
	}

	private class NfIntStringPair extends PactPair<PactInteger, PactString> {
	}

	private class NfDoubleStringPair extends PactPair<PactDouble, PactString> {
	}

	private class NfStringList extends PactList<PactString> {
	}

	private class NfIntStringMap extends PactMap<PactInteger, PactString> {
	}

	private class NfStringIntMap extends PactMap<PactString, PactInteger> {
	}
}
