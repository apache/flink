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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

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
	public void testPair() {
		NfIntStringPair pair1 = new NfIntStringPair();

		pair1.setFirst(new IntValue(10));
		pair1.setSecond(new StringValue("This is a string"));

		// test data retrieval
		Assert.assertEquals(pair1.getFirst(), new IntValue(10));
		Assert.assertEquals(pair1.getSecond(), new StringValue("This is a string"));

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

		pair2.setFirst(new IntValue(10));
		pair2.setSecond(new StringValue("This is a string"));

		pair3.setFirst(new IntValue(5));
		pair3.setSecond(new StringValue("This is a string"));

		pair4.setFirst(new IntValue(15));
		pair4.setSecond(new StringValue("This is a string"));

		pair5.setFirst(new IntValue(10));
		pair5.setSecond(new StringValue("This is a strina"));

		pair6.setFirst(new IntValue(10));
		pair6.setSecond(new StringValue("This is a strinz"));

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
		mPair7.setFirst(new DoubleValue(2.3));

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
		map0.put(new IntValue(10), new StringValue("20"));

		// test data retrieval
		for (Entry<IntValue, StringValue> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getValue(), new StringValue("20"));
			Assert.assertEquals(entry.getKey(), new IntValue(10));
		}

		// test data overwriting
		map0.put(new IntValue(10), new StringValue("10"));
		for (Entry<IntValue, StringValue> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getValue(), new StringValue("10"));
			Assert.assertEquals(entry.getKey(), new IntValue(10));
		}

		// now test data retrieval of multiple values
		map0.put(new IntValue(20), new StringValue("20"));
		map0.put(new IntValue(30), new StringValue("30"));
		map0.put(new IntValue(40), new StringValue("40"));

		// construct an inverted map
		NfStringIntMap mapinv = new NfStringIntMap();
		for (Entry<IntValue, StringValue> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getKey().getValue(), Integer.parseInt(entry.getValue().toString()));
			mapinv.put(entry.getValue(), entry.getKey());
		}

		for (Entry<StringValue, IntValue> entry : mapinv.entrySet()) {
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
		for (Entry<IntValue, StringValue> entry : map0.entrySet()) {
			Assert.assertEquals(entry.getKey().getValue(), Integer.parseInt(entry.getValue().toString()));
		}
	}

	@Test
	public void testPactList() {
		NfStringList list = new NfStringList();
		list.add(new StringValue("Hello!"));

		for (StringValue value : list) {
			Assert.assertEquals(value, new StringValue("Hello!"));
		}

		list.add(new StringValue("Hello2!"));
		list.add(new StringValue("Hello3!"));
		list.add(new StringValue("Hello4!"));

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

	private class NfIntStringPair extends Pair<IntValue, StringValue> {
		private static final long serialVersionUID = 1L;
	}

	private class NfDoubleStringPair extends Pair<DoubleValue, StringValue> {
		private static final long serialVersionUID = 1L;
	}

	private class NfStringList extends ListValue<StringValue> {
		private static final long serialVersionUID = 1L;
	}

	private class NfIntStringMap extends MapValue<IntValue, StringValue> {
		private static final long serialVersionUID = 1L;
	}

	private class NfStringIntMap extends MapValue<StringValue, IntValue> {
		private static final long serialVersionUID = 1L;
	}
}
