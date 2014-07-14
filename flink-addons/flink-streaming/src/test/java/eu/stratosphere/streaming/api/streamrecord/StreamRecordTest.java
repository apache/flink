/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.api.streamrecord;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.tuple.Tuple9;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

public class StreamRecordTest {

	@Test
	public void singleRecordSetGetTest() {
		StreamRecord record = new StreamRecord(
				new Tuple9<String, Integer, Long, Boolean, Double, Byte, Character, Float, Short>(
						"Stratosphere", 1, 2L, true, 3.5, (byte) 0xa, 'a', 0.1f, (short) 42));

		assertEquals(9, record.getNumOfFields());
		assertEquals(1, record.getNumOfTuples());

		assertEquals("Stratosphere", record.getString(0));
		assertEquals((Integer) 1, record.getInteger(1));
		assertEquals((Long) 2L, record.getLong(2));
		assertEquals(true, record.getBoolean(3));
		assertEquals((Double) 3.5, record.getDouble(4));
		assertEquals((Byte) (byte) 0xa, record.getByte(5));
		assertEquals((Character) 'a', record.getCharacter(6));
		assertEquals((Float) 0.1f, record.getFloat(7));
		assertEquals((Short) (short) 42, record.getShort(8));

		Tuple9<String, Integer, Long, Boolean, Double, Byte, Character, Float, Short> tuple = new Tuple9<String, Integer, Long, Boolean, Double, Byte, Character, Float, Short>();

		record.getTupleInto(tuple);

		assertEquals("Stratosphere", tuple.getField(0));
		assertEquals((Integer) 1, tuple.getField(1));
		assertEquals((Long) 2L, tuple.getField(2));
		assertEquals(true, tuple.getField(3));
		assertEquals((Double) 3.5, tuple.getField(4));
		assertEquals((Byte) (byte) 0xa, tuple.getField(5));
		assertEquals((Character) 'a', tuple.getField(6));
		assertEquals((Float) 0.1f, tuple.getField(7));
		assertEquals((Short) (short) 42, tuple.getField(8));

		record.setString(0, "Streaming");
		record.setInteger(1, 2);
		record.setLong(2, 3L);
		record.setBoolean(3, false);
		record.setDouble(4, 4.5);
		record.setByte(5, (byte) 0xb);
		record.setCharacter(6, 'b');
		record.setFloat(7, 0.2f);
		record.setShort(8, (short) 69);

		assertEquals("Streaming", record.getString(0));
		assertEquals((Integer) 2, record.getInteger(1));
		assertEquals((Long) 3L, record.getLong(2));
		assertEquals(false, record.getBoolean(3));
		assertEquals((Double) 4.5, record.getDouble(4));
		assertEquals((Byte) (byte) 0xb, record.getByte(5));
		assertEquals((Character) 'b', record.getCharacter(6));
		assertEquals((Float) 0.2f, record.getFloat(7));
		assertEquals((Short) (short) 69, record.getShort(8));

		record.setString(0, 0, "");
		record.setInteger(0, 1, 0);
		record.setLong(0, 2, 0L);
		record.setBoolean(0, 3, false);
		record.setDouble(0, 4, 0.);
		record.setByte(0, 5, (byte) 0x0);
		record.setCharacter(0, 6, '\0');
		record.setFloat(0, 7, 0.f);
		record.setShort(0, 8, (short) 0);

		assertEquals("", record.getString(0, 0));
		assertEquals((Integer) 0, record.getInteger(0, 1));
		assertEquals((Long) 0L, record.getLong(0, 2));
		assertEquals(false, record.getBoolean(0, 3));
		assertEquals((Double) 0., record.getDouble(0, 4));
		assertEquals((Byte) (byte) 0x0, record.getByte(0, 5));
		assertEquals((Character) '\0', record.getCharacter(0, 6));
		assertEquals((Float) 0.f, record.getFloat(0, 7));
		assertEquals((Short) (short) 0, record.getShort(0, 8));

	}

	@Test
	public void batchRecordSetGetTest() {
		StreamRecord record = new StreamRecord(5, 2);

		Tuple5<String, Integer, Long, Boolean, Double> tuple = new Tuple5<String, Integer, Long, Boolean, Double>(
				"Stratosphere", 1, 2L, true, 3.5);

		record.addTuple(StreamRecord.copyTuple(tuple));

		tuple.setField("", 0);
		tuple.setField(0, 1);
		tuple.setField(0L, 2);
		tuple.setField(false, 3);
		tuple.setField(0., 4);

		record.addTuple(tuple);
		try {
			record.addTuple(new Tuple1<String>("4"));
			fail();
		} catch (TupleSizeMismatchException e) {
		}

		assertEquals(5, record.getNumOfFields());
		assertEquals(2, record.getNumOfTuples());

		assertEquals("Stratosphere", record.getString(0, 0));
		assertEquals((Integer) 1, record.getInteger(0, 1));
		assertEquals((Long) 2L, record.getLong(0, 2));
		assertEquals(true, record.getBoolean(0, 3));
		assertEquals((Double) 3.5, record.getDouble(0, 4));

		assertEquals("", record.getString(1, 0));
		assertEquals((Integer) 0, record.getInteger(1, 1));
		assertEquals((Long) 0L, record.getLong(1, 2));
		assertEquals(false, record.getBoolean(1, 3));
		assertEquals((Double) 0., record.getDouble(1, 4));

		record.setTuple(new Tuple5<String, Integer, Long, Boolean, Double>("", 0, 0L, false, 0.));

		assertEquals(5, record.getNumOfFields());
		assertEquals(2, record.getNumOfTuples());

		assertEquals("", record.getString(0, 0));
		assertEquals((Integer) 0, record.getInteger(0, 1));
		assertEquals((Long) 0L, record.getLong(0, 2));
		assertEquals(false, record.getBoolean(0, 3));
		assertEquals((Double) 0., record.getDouble(0, 4));

		record.setTuple(1, new Tuple5<String, Integer, Long, Boolean, Double>("Stratosphere", 1,
				2L, true, 3.5));

		assertEquals("Stratosphere", record.getString(1, 0));
		assertEquals((Integer) 1, record.getInteger(1, 1));
		assertEquals((Long) 2L, record.getLong(1, 2));
		assertEquals(true, record.getBoolean(1, 3));
		assertEquals((Double) 3.5, record.getDouble(1, 4));

		record.removeTuple(1);

		assertEquals(1, record.getNumOfTuples());

		assertEquals("", record.getString(0, 0));
		assertEquals((Integer) 0, record.getInteger(0, 1));
		assertEquals((Long) 0L, record.getLong(0, 2));
		assertEquals(false, record.getBoolean(0, 3));
		assertEquals((Double) 0., record.getDouble(0, 4));

		record.addTuple(0, new Tuple5<String, Integer, Long, Boolean, Double>("Stratosphere", 1,
				2L, true, 3.5));

		assertEquals(2, record.getNumOfTuples());

		assertEquals("Stratosphere", record.getString(0, 0));
		assertEquals((Integer) 1, record.getInteger(0, 1));
		assertEquals((Long) 2L, record.getLong(0, 2));
		assertEquals(true, record.getBoolean(0, 3));
		assertEquals((Double) 3.5, record.getDouble(0, 4));

	}

	@Test
	public void copyTest() throws IOException {
		StreamRecord a = new StreamRecord(new Tuple1<String>("Big"));
		a.setId(0);
		StreamRecord b = a.copy();
		assertTrue(a.getField(0).equals(b.getField(0)));
		assertTrue(a.getId().equals(b.getId()));
		b.setId(2);
		b.setTuple(new Tuple1<String>("Data"));
		assertFalse(a.getId().equals(b.getId()));
		assertFalse(a.getField(0).equals(b.getField(0)));
		final int ITERATION = 10000;

		StreamRecord c = new StreamRecord(new Tuple1<String>("Big"));

		long t = System.nanoTime();
		for (int i = 0; i < ITERATION; i++) {
			c.copySerialized();
		}
		long t2 = System.nanoTime() - t;
		System.out.println("Serialized copy:\t" + t2 + " ns");

		t = System.nanoTime();
		for (int i = 0; i < ITERATION; i++) {
			c.copy();
		}
		t2 = System.nanoTime() - t;
		System.out.println("Copy:\t" + t2 + " ns");

	}

	@Test
	public void exceptionTest() {
		StreamRecord a = new StreamRecord(new Tuple1<String>("Big"));
		try {
			a.setTuple(4, new Tuple1<String>("Data"));
			fail();
		} catch (NoSuchTupleException e) {
		}

		try {
			a.setTuple(new Tuple2<String, String>("Data", "Stratosphere"));
			fail();
		} catch (TupleSizeMismatchException e) {
		}

		StreamRecord b = new StreamRecord();
		try {
			b.addTuple(new Tuple2<String, String>("Data", "Stratosphere"));
			fail();
		} catch (TupleSizeMismatchException e) {
		}

		try {
			a.getField(3);
			fail();
		} catch (NoSuchFieldException e) {
		}

		try {
			a.getBoolean(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}

		try {
			a.getByte(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getCharacter(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getDouble(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getFloat(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getInteger(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getLong(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}
		try {
			a.getShort(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}

		StreamRecord c = new StreamRecord(new Tuple1<Integer>(1));
		try {
			c.getString(0);
			fail();
		} catch (FieldTypeMismatchException e) {
		}

	}

	@Test
	public void writeReadTest() {
		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);

		int num = 42;
		String str = "above clouds";
		Integer[] intArray = new Integer[] { 1, 2 };
		Tuple3<Integer, String, Integer[]> tuple1 = new Tuple3<Integer, String, Integer[]>(num,
				str, intArray);
		Tuple3<Integer, String, Integer[]> tuple2 = new Tuple3<Integer, String, Integer[]>(1, "",
				new Integer[] { 1, 2 });
		StreamRecord rec = new StreamRecord(tuple1);
		rec.addTuple(tuple2);

		try {
			rec.write(out);
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(buff.toByteArray()));

			StreamRecord newRec = new StreamRecord();
			newRec.read(in);

			assertEquals(2, newRec.getNumOfTuples());

			@SuppressWarnings("unchecked")
			Tuple3<Integer, String, Integer[]> tupleOut1 = (Tuple3<Integer, String, Integer[]>) newRec
					.getTuple(0);

			assertEquals(tupleOut1.getField(0), 42);
			assertEquals(str, tupleOut1.getField(1));
			assertArrayEquals(intArray, (Integer[]) tupleOut1.getField(2));

			@SuppressWarnings("unchecked")
			Tuple3<Integer, String, Integer[]> tupleOut2 = (Tuple3<Integer, String, Integer[]>) newRec
					.getTuple(1);
			assertEquals(tupleOut2.getField(0), 1);
			assertEquals("", tupleOut2.getField(1));
			assertArrayEquals(new Integer[] { 1, 2 }, (Integer[]) tupleOut2.getField(2));

		} catch (IOException e) {
			fail();
			e.printStackTrace();
		}

	}

	@Test
	public void tupleCopyTest() {
		Tuple3<String, Integer, Double[]> t1 = new Tuple3<String, Integer, Double[]>("a", 1,
				new Double[] { 4.2 });

		@SuppressWarnings("rawtypes")
		Tuple3 t2 = (Tuple3) StreamRecord.copyTuple(t1);

		assertEquals("a", t2.getField(0));
		assertEquals(1, t2.getField(1));
		assertArrayEquals(new Double[] { 4.2 }, (Double[]) t2.getField(2));

		t1.setField(2, 1);
		assertEquals(1, t2.getField(1));
		assertEquals(2, t1.getField(1));

		t1.setField(new Double[] { 3.14 }, 2);
		assertArrayEquals(new Double[] { 3.14 }, (Double[]) t1.getField(2));
		assertArrayEquals(new Double[] { 4.2 }, (Double[]) t2.getField(2));

		assertEquals(t1.getField(0).getClass(), t2.getField(0).getClass());
		assertEquals(t1.getField(1).getClass(), t2.getField(1).getClass());
	}

	@Test
	public void tupleArraySerializationTest() throws IOException {
		Tuple9<Boolean[], Byte[], Character[], Double[], Float[], Integer[], Long[], Short[], String[]> t1 = new Tuple9<Boolean[], Byte[], Character[], Double[], Float[], Integer[], Long[], Short[], String[]>(
				new Boolean[] { true }, new Byte[] { 12 }, new Character[] { 'a' },
				new Double[] { 12.5 }, new Float[] { 13.5f }, new Integer[] { 1234 },
				new Long[] { 12345678900l }, new Short[] { 12345 }, new String[] { "something" });

		StreamRecord s1 = new StreamRecord(t1);
		StreamRecord s2 = s1.copySerialized();

		@SuppressWarnings("rawtypes")
		Tuple9 t2 = (Tuple9) s2.getTuple();

		assertArrayEquals(new Boolean[] { true }, (Boolean[]) t2.getField(0));
		assertArrayEquals(new Byte[] { 12 }, (Byte[]) t2.getField(1));
		assertArrayEquals(new Character[] { 'a' }, (Character[]) t2.getField(2));
		assertArrayEquals(new Double[] { 12.5 }, (Double[]) t2.getField(3));
		assertArrayEquals(new Float[] { 13.5f }, (Float[]) t2.getField(4));
		assertArrayEquals(new Integer[] { 1234 }, (Integer[]) t2.getField(5));
		assertArrayEquals(new Long[] { 12345678900l }, (Long[]) t2.getField(6));
		assertArrayEquals(new Short[] { 12345 }, (Short[]) t2.getField(7));
		assertArrayEquals(new String[] { "something" }, (String[]) t2.getField(8));

		assertEquals(t1.getField(0).getClass(), t2.getField(0).getClass());
		assertEquals(t1.getField(1).getClass(), t2.getField(1).getClass());
		assertEquals(t1.getField(2).getClass(), t2.getField(2).getClass());
		assertEquals(t1.getField(3).getClass(), t2.getField(3).getClass());
		assertEquals(t1.getField(4).getClass(), t2.getField(4).getClass());
		assertEquals(t1.getField(5).getClass(), t2.getField(5).getClass());
		assertEquals(t1.getField(6).getClass(), t2.getField(6).getClass());
		assertEquals(t1.getField(7).getClass(), t2.getField(7).getClass());
		assertEquals(t1.getField(8).getClass(), t2.getField(8).getClass());
	}

	// TODO:measure performance of different serialization logics
	@Test
	public void typeCopyTest() throws NoSuchTupleException, IOException {
		StreamRecord rec = new StreamRecord(
				new Tuple9<Boolean, Byte, Character, Double, Float, Integer, Long, Short, String>(
						(Boolean) true, (Byte) (byte) 12, (Character) 'a', (Double) 12.5,
						(Float) (float) 13.5, (Integer) 1234, (Long) 12345678900l,
						(Short) (short) 12345, "something"));

		ByteArrayOutputStream buff3 = new ByteArrayOutputStream();
		DataOutputStream out3 = new DataOutputStream(buff3);
		for (int i = 0; i < 1000; i++) {
			out3.write(rec.tupleTypesToByteArray(rec.getTuple()));
		}

	}

	@Test
	public void typeArrayCopyTest() throws NoSuchTupleException, IOException {
		StreamRecord rec = new StreamRecord(
				new Tuple9<Boolean[], Byte[], Character[], Double[], Float[], Integer[], Long[], Short[], String[]>(
						new Boolean[] { true }, new Byte[] { 12 }, new Character[] { 'a' },
						new Double[] { 12.5 }, new Float[] { 13.5f }, new Integer[] { 1234 },
						new Long[] { 12345678900l }, new Short[] { 12345 },
						new String[] { "something" }));

		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(buff);
		for (int i = 0; i < 10000; i++) {
			out.write(rec.tupleTypesToByteArray(rec.getTuple()));
		}
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buff.toByteArray()));
		StreamRecord rec2 = new StreamRecord();
		Long start = System.nanoTime();
		for (int i = 0; i < 10000; i++) {
			byte[] byteTypes = new byte[9];
			in.read(byteTypes);
			TypeInformation<?>[] basicTypes = rec2.tupleTypesFromByteArray(byteTypes);
			@SuppressWarnings("unused")
			TupleTypeInfo<Tuple> typeInfo = new TupleTypeInfo<Tuple>(basicTypes);
		}
		System.out.println("Type copy with ByteArray:\t" + (System.nanoTime() - start) + " ns");

		start = System.nanoTime();

		byte[] byteTypes = rec.tupleTypesToByteArray(rec.getTuple());
		Tuple t = rec.getTuple();

		start = System.nanoTime();
		for (int i = 0; i < 10000; i++) {
			// rec2.tupleBasicTypesFromByteArray(byteTypes, 9);
			TypeInformation<?>[] basicTypes = rec2.tupleTypesFromByteArray(byteTypes);
			@SuppressWarnings("unused")
			TupleTypeInfo<Tuple> typeInfo = new TupleTypeInfo<Tuple>(basicTypes);
		}
		System.out.println("Write with infoArray:\t\t" + (System.nanoTime() - start) + " ns");
		start = System.nanoTime();
		for (int i = 0; i < 10000; i++) {
			@SuppressWarnings("unused")
			TupleTypeInfo<Tuple> typeInfo = (TupleTypeInfo<Tuple>) TypeExtractor.getForObject(t);
		}
		System.out.println("Write with extract:\t\t" + (System.nanoTime() - start) + " ns");
	}

	@Test
	public void batchIteratorTest() {

		List<Tuple> tupleList = new LinkedList<Tuple>();
		tupleList.add(new Tuple1<String>("Big"));
		tupleList.add(new Tuple1<String>("Data"));

		StreamRecord a = new StreamRecord(tupleList);

		assertEquals(2, a.getNumOfTuples());
		assertEquals(1, a.getNumOfFields());

		for (Tuple t : a.getBatchIterable()) {
			System.out.println(t);
		}

		OLSMultipleLinearRegression ols = new OLSMultipleLinearRegression();
		ols.newSampleData(new double[] { 1.0, 2.0 }, new double[][] { { 1, 2 }, { 3, 4 } });
		System.out.println(Arrays.toString(ols.estimateRegressionParameters()));

	}

	@Test
	public void typeExtractTest() throws IOException, ClassNotFoundException {

		ByteArrayOutputStream buff = new ByteArrayOutputStream();
		ObjectOutputStream out = new ObjectOutputStream(buff);

		MyGeneric<?> g = new MyGeneric2();
		out.writeObject(g);

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buff.toByteArray()));

		MyGeneric<?> f = (MyGeneric<?>) in.readObject();

		TypeInformation<?> ti = TypeExtractor.createTypeInfo(MyGeneric.class, f.getClass(), 0,
				null, null);

		System.out.println("Type info: " + ti);

	}
}
