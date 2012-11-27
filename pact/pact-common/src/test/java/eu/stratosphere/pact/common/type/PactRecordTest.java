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

package eu.stratosphere.pact.common.type;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactRecordTest {
	
	private static final long SEED = 354144423270432543L;
	private final Random rand = new Random(PactRecordTest.SEED);
	
	private DataInputStream in;
	private DataOutputStream out;
	
	// Couple of test values
	private final PactString origVal1 = new PactString("Hello World!");
	private final PactDouble origVal2 = new PactDouble(Math.PI);
	private final PactInteger origVal3 = new PactInteger(1337);
	
	

	@Before
	public void setUp() throws Exception
	{
		PipedInputStream pipedInput = new PipedInputStream(1024*1024);
		this.in = new DataInputStream(pipedInput);
		this.out = new DataOutputStream(new PipedOutputStream(pipedInput));
	}
	
	@Test
	public void testEmptyRecordSerialization()
	{
		try {
			// test deserialize into self
			PactRecord empty = new PactRecord();
			empty.write(this.out);
			empty.read(this.in);
			Assert.assertTrue("Deserialized Empty record is not another empty record.", empty.getNumFields() == 0);
			
			// test deserialize into new
			empty = new PactRecord();
			empty.write(this.out);
			empty = new PactRecord();
			empty.read(this.in);
			Assert.assertTrue("Deserialized Empty record is not another empty record.", empty.getNumFields() == 0);
			
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

	@Test
	public void testAddField() {
		try {
			// Add a value to an empty record
			PactRecord record = new PactRecord();
			assertTrue(record.getNumFields() == 0);
			record.addField(this.origVal1);
			assertTrue(record.getNumFields() == 1);
			assertTrue(origVal1.getValue().equals(record.getField(0, PactString.class).getValue()));
			
			// Add 100 random integers to the record
			record = new PactRecord();
			for (int i = 0; i < 100; i++) {
				PactInteger orig = new PactInteger(this.rand.nextInt());
				record.addField(orig);
				PactInteger rec = record.getField(i, PactInteger.class);
				
				assertTrue(record.getNumFields() == i + 1);
				assertTrue(orig.getValue() == rec.getValue());
			}
			
			// Add 3 values of different type to the record
			record = new PactRecord(this.origVal1, this.origVal2);
			record.addField(this.origVal3);
			
			assertTrue(record.getNumFields() == 3);
			
			PactString recVal1 = record.getField(0, PactString.class);
			PactDouble recVal2 = record.getField(1, PactDouble.class);
			PactInteger recVal3 = record.getField(2, PactInteger.class);
			
			assertTrue("The value of the first field has changed", recVal1.equals(this.origVal1));
			assertTrue("The value of the second field changed", recVal2.equals(this.origVal2));
			assertTrue("The value of the third field has changed", recVal3.equals(this.origVal3));
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

//	@Test
//	public void testInsertField() {
//		PactRecord record = null;
//		int oldLen = 0;
//
//		// Create filled record and insert in the middle
//		record = new PactRecord(this.origVal1, this.origVal3);
//		record.insertField(1, this.origVal2);
//
//		assertTrue(record.getNumFields() == 3);
//
//		PactString recVal1 = record.getField(0, PactString.class);
//		PactDouble recVal2 = record.getField(1, PactDouble.class);
//		PactInteger recVal3 = record.getField(2, PactInteger.class);
//
//		assertTrue(recVal1.getValue().equals(this.origVal1.getValue()));
//		assertTrue(recVal2.getValue() == this.origVal2.getValue());
//		assertTrue(recVal3.getValue() == this.origVal3.getValue());
//
//		record = this.generateFilledDenseRecord(100);
//
//		// Insert field at the first position of the record
//		oldLen = record.getNumFields();
//		record.insertField(0, this.origVal1);
//		assertTrue(record.getNumFields() == oldLen + 1);
//		assertTrue(this.origVal1.equals(record.getField(0, PactString.class)));
//
//		// Insert field at the end of the record
//		oldLen = record.getNumFields();
//		record.insertField(oldLen, this.origVal2);
//		assertTrue(record.getNumFields() == oldLen + 1);
//		assertTrue(this.origVal2 == record.getField(oldLen, PactDouble.class));
//
//		// Insert several random fields into the record
//		for (int i = 0; i < 100; i++) {
//			int pos = rand.nextInt(record.getNumFields());
//			PactInteger val = new PactInteger(rand.nextInt());
//			record.insertField(pos, val);
//			assertTrue(val.getValue() == record.getField(pos, PactInteger.class).getValue());
//		}
//	}

//	@Test
//	public void testRemoveField() {		
//		PactRecord record = null;
//		int oldLen = 0;
//
//		// Create filled record and remove field from the middle
//		record = new PactRecord(this.origVal1, this.origVal2);
//		record.addField(this.origVal3);
//		record.removeField(1);
//
//		assertTrue(record.getNumFields() == 2);
//
//		PactString recVal1 = record.getField(0, PactString.class);
//		PactInteger recVal2 = record.getField(1, PactInteger.class);
//
//		assertTrue(recVal1.getValue().equals(this.origVal1.getValue()));
//		assertTrue(recVal2.getValue() == this.origVal3.getValue());
//
//		record = this.generateFilledDenseRecord(100);
//
//		// Remove field from the first position of the record
//		oldLen = record.getNumFields();
//		record.removeField(0);
//		assertTrue(record.getNumFields() == oldLen - 1);
//
//		// Remove field from the end of the record
//		oldLen = record.getNumFields();
//		record.removeField(oldLen - 1);
//		assertTrue(record.getNumFields() == oldLen - 1);
//
//		// Insert several random fields into the record
//		record = this.generateFilledDenseRecord(100);
//
//		for (int i = 0; i < 100; i++) {
//			oldLen = record.getNumFields();
//			int pos = rand.nextInt(record.getNumFields());
//			record.removeField(pos);
//			assertTrue(record.getNumFields() == oldLen - 1);
//		}
//	}

//	@Test
//	public void testProjectLong() {		
//		PactRecord record = new PactRecord();
//		long mask = 0;
//
//		record.addField(this.origVal1);
//		record.addField(this.origVal2);
//		record.addField(this.origVal3);
//
//		// Keep all fields
//		mask = 7L;
//		record.project(mask);
//		assertTrue(record.getNumFields() == 3);
//		assertTrue(this.origVal1.getValue().equals(record.getField(0, PactString.class).getValue()));
//		assertTrue(this.origVal2.getValue() == record.getField(1, PactDouble.class).getValue());
//		assertTrue(this.origVal3.getValue() == record.getField(2, PactInteger.class).getValue());
//
//		// Keep the first and the last field
//		mask = 5L; // Keep the first and the third/ last column
//		record.project(mask);
//		assertTrue(record.getNumFields() == 2);
//		assertTrue(this.origVal1.getValue().equals(record.getField(0, PactString.class).getValue()));
//		assertTrue(this.origVal3.getValue() == record.getField(1, PactInteger.class).getValue());
//
//		// Keep no fields
//		mask = 0L;
//		record.project(mask);
//		assertTrue(record.getNumFields() == 0);
//
//		// Keep random fields
//		record = this.generateFilledDenseRecord(64);
//		mask = this.generateRandomBitmask(64);
//
//		record.project(mask);
//		assertTrue(record.getNumFields() == Long.bitCount(mask));
//	}

//	@Test
//	public void testProjectLongArray() {
//		PactRecord record = this.generateFilledDenseRecord(256);
//		long[] mask = {1L, 1L, 1L, 1L};
//
//		record.project(mask);
//		assertTrue(record.getNumFields() == 4);
//
//		record = this.generateFilledDenseRecord(612);
//		mask = new long[10];
//		int numBits = 0;
//
//		for (int i = 0; i < mask.length; i++) {
//			int offset = i * Long.SIZE;
//			int numFields = ((offset + Long.SIZE) < record.getNumFields()) ? Long.SIZE : record.getNumFields() - offset;
//			mask[i] = this.generateRandomBitmask(numFields);
//			numBits += Long.bitCount(mask[i]);
//		}
//
//		record.project(mask);
//		assertTrue(record.getNumFields() == numBits);
//	}

	@Test
	public void testSetNullInt()
	{
		try {
			PactRecord record = this.generateFilledDenseRecord(58);
	
			record.setNull(42);
			assertTrue(record.getNumFields() == 58);
			assertTrue(record.getField(42, PactInteger.class) == null);
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

	@Test
	public void testSetNullLong()
	{
		try {
			PactRecord record = this.generateFilledDenseRecord(58);
			long mask = generateRandomBitmask(58);
	
			record.setNull(mask);
	
			for (int i = 0; i < 58; i++) {
				if (((1l << i) & mask) != 0) {
					assertTrue(record.getField(i, PactInteger.class) == null);
				}
			}
	
			assertTrue(record.getNumFields() == 58);
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

	@Test
	public void testSetNullLongArray()
	{
		try {
			PactRecord record = this.generateFilledDenseRecord(612);
			long[] mask = {1L, 1L, 1L, 1L};
			record.setNull(mask);
	
			assertTrue(record.getField(0, PactInteger.class) == null);
			assertTrue(record.getField(64, PactInteger.class) == null);
			assertTrue(record.getField(128, PactInteger.class) == null);
			assertTrue(record.getField(192, PactInteger.class) == null);
	
			mask = new long[10];
			for (int i = 0; i < mask.length; i++) {
				int offset = i * Long.SIZE;
				int numFields = ((offset + Long.SIZE) < record.getNumFields()) ? Long.SIZE : record.getNumFields() - offset;
				mask[i] = this.generateRandomBitmask(numFields);
			}
	
			record.setNull(mask);
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

//	@Test
//	public void testAppend() {
//		PactRecord record1 = this.generateFilledDenseRecord(42);
//		PactRecord record2 = this.generateFilledDenseRecord(1337);
//		
//		PactInteger rec1val = record1.getField(12, PactInteger.class);
//		PactInteger rec2val = record2.getField(23, PactInteger.class);
//		
//		record1.append(record2);
//		
//		assertTrue(record1.getNumFields() == 42 + 1337);
//		assertTrue(rec1val.getValue() == record1.getField(12, PactInteger.class).getValue());
//		assertTrue(rec2val.getValue() == record1.getField(42 + 23, PactInteger.class).getValue());
//	}

//	@Test
//	public void testUnion() {
//	}
	
	@Test
	public void testUpdateBinaryRepresentations()
	{
		try {
			// TODO: this is not an extensive test of updateBinaryRepresentation()
			// and should be extended!
	
			PactRecord r = new PactRecord();
	
			PactInteger i1 = new PactInteger(1);
			PactInteger i2 = new PactInteger(2);
	
			try {
				r.setField(1, i1);
				r.setField(3, i2);
	
				r.setNumFields(5);
	
				r.updateBinaryRepresenation();
	
				i1 = new PactInteger(3);
				i2 = new PactInteger(4);
	
				r.setField(7, i1);
				r.setField(8, i2);
	
				r.updateBinaryRepresenation();
	
				assertTrue(r.getField(1, PactInteger.class).getValue() == 1);
				assertTrue(r.getField(3, PactInteger.class).getValue() == 2);
				assertTrue(r.getField(7, PactInteger.class).getValue() == 3);
				assertTrue(r.getField(8, PactInteger.class).getValue() == 4);
			} catch (RuntimeException re) {
				fail("Error updating binary representation: " + re.getMessage());
			}
	
			// Tests an update where modified and unmodified fields are interleaved
			r = new PactRecord();
	
			for (int i = 0; i < 8; i++) {
				r.setField(i, new PactInteger(i));
			}
	
			try {
				// serialize and deserialize to remove all buffered info
				r.write(out);
				r = new PactRecord();
				r.read(in);
	
				r.setField(1, new PactInteger(10));
				r.setField(4, new PactString("Some long value"));
				r.setField(5, new PactString("An even longer value"));
				r.setField(10, new PactInteger(10));
	
				r.write(out);
				r = new PactRecord();
				r.read(in);
	
				assertTrue(r.getField(0, PactInteger.class).getValue() == 0);
				assertTrue(r.getField(1, PactInteger.class).getValue() == 10);
				assertTrue(r.getField(2, PactInteger.class).getValue() == 2);
				assertTrue(r.getField(3, PactInteger.class).getValue() == 3);
				assertTrue(r.getField(4, PactString.class).getValue().equals("Some long value"));
				assertTrue(r.getField(5, PactString.class).getValue().equals("An even longer value"));
				assertTrue(r.getField(6, PactInteger.class).getValue() == 6);
				assertTrue(r.getField(7, PactInteger.class).getValue() == 7);
				assertTrue(r.getField(8, PactInteger.class) == null);
				assertTrue(r.getField(9, PactInteger.class) == null);
				assertTrue(r.getField(10, PactInteger.class).getValue() == 10);
	
			} catch (RuntimeException re) {
				fail("Error updating binary representation: " + re.getMessage());
			} catch (IOException e) {
				fail("Error updating binary representation: " + e.getMessage());
			}
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}
	
	@Test
	public void testDeSerialization()
	{
		try {
			PactString origValue1 = new PactString("Hello World!");
			PactInteger origValue2 = new PactInteger(1337);
			PactRecord record1 = new PactRecord(origValue1, origValue2);
			PactRecord record2 = new PactRecord();
			try {
				// De/Serialize the record
				record1.write(this.out);
				record2.read(this.in);
	
				assertTrue(record1.getNumFields() == record2.getNumFields());
	
				PactString rec1Val1 = record1.getField(0, PactString.class);
				PactInteger rec1Val2 = record1.getField(1, PactInteger.class);
				PactString rec2Val1 = record2.getField(0, PactString.class);
				PactInteger rec2Val2 = record2.getField(1, PactInteger.class);
	
				assertTrue(origValue1.equals(rec1Val1));
				assertTrue(origValue2.equals(rec1Val2));
				assertTrue(origValue1.equals(rec2Val1));
				assertTrue(origValue2.equals(rec2Val2));
			} catch (IOException e) {
				fail("Error writing PactRecord");
				e.printStackTrace();
			}
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}
	
	@Test
	public void testClear() throws IOException
	{
		try {
			PactRecord record = new PactRecord(new PactInteger(42));
	
			record.write(out);
			Assert.assertEquals(42, record.getField(0, PactInteger.class).getValue());
	
			record.setField(0, new PactInteger(23));
			record.write(out);
			Assert.assertEquals(23, record.getField(0, PactInteger.class).getValue());
	
			record.clear();
			Assert.assertEquals(0, record.getNumFields());
	
			PactRecord record2 = new PactRecord(new PactInteger(42));
			record2.read(in);
			Assert.assertEquals(42, record2.getField(0, PactInteger.class).getValue());
			record2.read(in);
			Assert.assertEquals(23, record2.getField(0, PactInteger.class).getValue());
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}

	private PactRecord generateFilledDenseRecord(int numFields) {
		PactRecord record = new PactRecord();

		for (int i = 0; i < numFields; i++) {
			record.addField(new PactInteger(this.rand.nextInt()));
		}

		return record;
	}

	private long generateRandomBitmask(int numFields) {
		long bitmask = 0L;
		long tmp = 0L;

		for (int i = 0; i < numFields; i++) {
			tmp = this.rand.nextBoolean() ? 1L : 0L;
			bitmask = bitmask | (tmp << i);
		}

		return bitmask;
	}
	
	@Test
	public void blackBoxTests()
	{
		try {
			final Value[][] values = new Value[][] {
				// empty
				{},
				// exactly 8 fields
				{new PactInteger(55), new PactString("Hi there!"), new PactLong(457354357357135L), new PactInteger(345), new PactInteger(-468), new PactString("This is the message and the message is this!"), new PactLong(0L), new PactInteger(465)},
				// exactly 16 fields
				{new PactInteger(55), new PactString("Hi there!"), new PactLong(457354357357135L), new PactInteger(345), new PactInteger(-468), new PactString("This is the message and the message is this!"), new PactLong(0L), new PactInteger(465), new PactInteger(55), new PactString("Hi there!"), new PactLong(457354357357135L), new PactInteger(345), new PactInteger(-468), new PactString("This is the message and the message is this!"), new PactLong(0L), new PactInteger(465)},
				// exactly 8 nulls
				{null, null, null, null, null, null, null, null},
				// exactly 16 nulls
				{null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
				// arbitrary example
				{new PactInteger(56), null, new PactInteger(-7628761), new PactString("A test string")},
				// a very long field
				{new PactString(createRandomString(this.rand, 15)), new PactString(createRandomString(this.rand, 1015)), new PactString(createRandomString(this.rand, 32))},
				// two very long fields
				{new PactString(createRandomString(this.rand, 1265)), null, new PactString(createRandomString(this.rand, 855))}
			};
			
			for (int i = 0; i < values.length; i++) {
				blackboxTestRecordWithValues(values[i], this.rand, this.in, this.out);
			}
			
			// random test with records with a small number of fields
			for (int i = 0; i < 10000; i++) {
				final Value[] fields = createRandomValues(this.rand, 0, 32);
				blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
			}
			
			// random tests with records with a moderately large number of fields
			for (int i = 0; i < 1000; i++) {
				final Value[] fields = createRandomValues(this.rand, 20, 150);
				blackboxTestRecordWithValues(fields, this.rand, this.in, this.out);
			}
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}
	
	static final void blackboxTestRecordWithValues(Value[] values, Random rnd, DataInput reader, DataOutput writer)
	throws Exception
	{
		final int[] permutation1 = createPermutation(rnd, values.length);
		final int[] permutation2 = createPermutation(rnd, values.length);
		
		// test adding and retrieving without intermediate binary updating
		PactRecord rec = new PactRecord();
		for (int i = 0; i < values.length; i++) {
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with full binary updating
		rec = new PactRecord();
		for (int i = 0; i < values.length; i++) {
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		rec.updateBinaryRepresenation();
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with intermediate binary updating
		rec = new PactRecord();
		int updatePos = rnd.nextInt(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (i == updatePos)
				rec.updateBinaryRepresenation();
			
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		if (updatePos == values.length)
			rec.updateBinaryRepresenation();
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with full stream serialization and deserialization into a new record
		rec = new PactRecord();
		for (int i = 0; i < values.length; i++) {
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		rec.write(writer);
		rec = new PactRecord();
		rec.read(reader);
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with full stream serialization and deserialization into the same record
		rec = new PactRecord();
		for (int i = 0; i < values.length; i++) {
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		rec.write(writer);
		rec.read(reader);
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with partial stream serialization and deserialization into a new record
		rec = new PactRecord();
		updatePos = rnd.nextInt(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (i == updatePos) {
				rec.write(writer);
				rec = new PactRecord();
				rec.read(reader);
			}
			
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		if (updatePos == values.length) {
			rec.write(writer);
			rec = new PactRecord();
			rec.read(reader);
		}
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with partial stream serialization and deserialization into the same record
		rec = new PactRecord();
		updatePos = rnd.nextInt(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (i == updatePos) {
				rec.write(writer);
				rec.read(reader);
			}
			
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		if (updatePos == values.length) {
			rec.write(writer);
			rec.read(reader);
		}
		testAllRetrievalMethods(rec, permutation2, values);

		// test adding and retrieving with partial stream serialization and deserialization into a new record
		rec = new PactRecord();
		updatePos = rnd.nextInt(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (i == updatePos) {
				rec.write(writer);
				rec = new PactRecord();
				rec.read(reader);
			}
			
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		rec.write(writer);
		rec = new PactRecord();
		rec.read(reader);
		testAllRetrievalMethods(rec, permutation2, values);
		
		// test adding and retrieving with partial stream serialization and deserialization into the same record
		rec = new PactRecord();
		updatePos = rnd.nextInt(values.length + 1);
		for (int i = 0; i < values.length; i++) {
			if (i == updatePos) {
				rec.write(writer);
				rec.read(reader);
			}
			
			final int pos = permutation1[i];
			rec.setField(pos, values[pos]);
		}
		rec.write(writer);
		rec.read(reader);
		testAllRetrievalMethods(rec, permutation2, values);
	}
	
	public static final void testAllRetrievalMethods(PactRecord rec, int[] permutation, Value[] expected)
	throws Exception
	{
		// test getField(int, Class)
		for (int i = 0; i < expected.length; i++) {
			final int pos = permutation[i];
			final Value e = expected[pos];

			if (e == null) {
				final Value retrieved = rec.getField(pos, PactInteger.class);
				if (retrieved != null)
					Assert.fail("Value at position " + pos + " expected to be null in " + Arrays.toString(expected));
			} else {
				final Value retrieved = rec.getField(pos, e.getClass());
				if (!(e.equals(retrieved)))
						Assert.assertEquals("Wrong value at position " + pos + " in " + Arrays.toString(expected), e, retrieved);
			}
		}
		
		// test getField(int, Value)
		for (int i = 0; i < expected.length; i++) {
			final int pos = permutation[i];
			final Value e = expected[pos];

			if (e == null) {
				final Value retrieved = rec.getField(pos, new PactInteger());
				if (retrieved != null)
					Assert.fail("Value at position " + pos + " expected to be null in " + Arrays.toString(expected));
			} else {
				final Value retrieved = rec.getField(pos, e.getClass().newInstance());
				if (!(e.equals(retrieved)))
					Assert.assertEquals("Wrong value at position " + pos + " in " + Arrays.toString(expected), e, retrieved);
			}
		}
		
		// test getFieldInto(Value)
		for (int i = 0; i < expected.length; i++) {
			final int pos = permutation[i];
			final Value e = expected[pos];

			if (e == null) {
				if (rec.getFieldInto(pos, new PactInteger()))
					Assert.fail("Value at position " + pos + " expected to be null in " + Arrays.toString(expected));
			} else {
				final Value retrieved = e.getClass().newInstance();
				if (!rec.getFieldInto(pos, retrieved))
					Assert.fail("Value at position " + pos + " expected to be not null in " + Arrays.toString(expected));
				
				if (!(e.equals(retrieved)))
					Assert.assertEquals("Wrong value at position " + pos + " in " + Arrays.toString(expected), e, retrieved);
			}
		}
	}

	@Test
	public void testUnionFields()
	{
		try {
			final Value[][] values = new Value[][] {
				{new PactInteger(56), null, new PactInteger(-7628761)},
				{null, new PactString("Hellow Test!"), null},
				
				{null, null, null, null, null, null, null, null},
				{null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null},
				
				{new PactInteger(56), new PactInteger(56), new PactInteger(56), new PactInteger(56), null, null, null},
				{null, null, null, null, new PactInteger(56), new PactInteger(56), new PactInteger(56)},
				
				{new PactInteger(43), new PactInteger(42), new PactInteger(41)},
				{new PactInteger(-463), new PactInteger(-464), new PactInteger(-465)}
			};
			
			for (int i = 0; i < values.length - 1; i += 2) {
				testUnionFieldsForValues(values[i], values[i+1], this.rand);
				testUnionFieldsForValues(values[i+1], values[i], this.rand);
			}
		} catch (Throwable t) {
			Assert.fail("Test failed due to an exception: " + t.getMessage());
		}
	}
	
	private final void testUnionFieldsForValues(Value[] rec1fields, Value[] rec2fields, Random rnd)
	{
		// fully in binary sync
		PactRecord rec1 = createRecord(rec1fields);
		PactRecord rec2 = createRecord(rec2fields);
		rec1.updateBinaryRepresenation();
		rec2.updateBinaryRepresenation();
		rec1.unionFields(rec2);
		checkUnionedRecord(rec1, rec1fields, rec2fields);
		
		// fully not in binary sync
		rec1 = createRecord(rec1fields);
		rec2 = createRecord(rec2fields);
		rec1.unionFields(rec2);
		checkUnionedRecord(rec1, rec1fields, rec2fields);
		
		// one in binary sync
		rec1 = createRecord(rec1fields);
		rec2 = createRecord(rec2fields);
		rec1.updateBinaryRepresenation();
		rec1.unionFields(rec2);
		checkUnionedRecord(rec1, rec1fields, rec2fields);
		
		// other in binary sync
		rec1 = createRecord(rec1fields);
		rec2 = createRecord(rec2fields);
		rec2.updateBinaryRepresenation();
		rec1.unionFields(rec2);
		checkUnionedRecord(rec1, rec1fields, rec2fields);
		
		// both partially in binary sync
		rec1 = new PactRecord();
		rec1 = new PactRecord();
		
		int[] permutation1 = createPermutation(rnd, rec1fields.length);
		int[] permutation2 = createPermutation(rnd, rec2fields.length);
		
		int updatePos = rnd.nextInt(rec1fields.length + 1);
		for (int i = 0; i < rec1fields.length; i++) {
			if (i == updatePos)
				rec1.updateBinaryRepresenation();
			
			final int pos = permutation1[i];
			rec1.setField(pos, rec1fields[pos]);
		}
		if (updatePos == rec1fields.length)
			rec1.updateBinaryRepresenation();
		
		updatePos = rnd.nextInt(rec2fields.length + 1);
		for (int i = 0; i < rec2fields.length; i++) {
			if (i == updatePos)
				rec2.updateBinaryRepresenation();
			
			final int pos = permutation2[i];
			rec2.setField(pos, rec2fields[pos]);
		}
		if (updatePos == rec2fields.length)
			rec2.updateBinaryRepresenation();
		
		rec1.unionFields(rec2);
		checkUnionedRecord(rec1, rec1fields, rec2fields);
	}
	
	private static final void checkUnionedRecord(PactRecord union, Value[] rec1fields, Value[] rec2fields)
	{
		for (int i = 0; i < Math.max(rec1fields.length, rec2fields.length); i++) {
			// determine the expected value from the value arrays
			final Value expected;
			if (i < rec1fields.length) {
				if (i < rec2fields.length) {
					expected = rec1fields[i] == null ? rec2fields[i] : rec1fields[i];
				} else {
					expected = rec1fields[i];
				}
			} else {
				expected = rec2fields[i];
			}
			
			// check value from record against expected value
			if (expected == null) {
				final Value retrieved = union.getField(i, PactInteger.class);
				Assert.assertNull("Value at position " + i + " expected to be null in " + 
								Arrays.toString(rec1fields) + " U " + Arrays.toString(rec2fields), retrieved);
			} else {
				final Value retrieved = union.getField(i, expected.getClass());
				Assert.assertEquals("Wrong value at position " + i + " in " + 
					Arrays.toString(rec1fields) + " U " + Arrays.toString(rec2fields), expected, retrieved);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//                                       Utilities
	// --------------------------------------------------------------------------------------------
	
	public static final PactRecord createRecord(Value[] fields)
	{
		final PactRecord rec = new PactRecord();
		for (int i = 0; i < fields.length; i++) {
			rec.setField(i, fields[i]);
		}
		return rec;
	}
	
	public static final Value[] createRandomValues(Random rnd, int minNum, int maxNum)
	{
		final int numFields = rnd.nextInt(maxNum - minNum + 1) + minNum;
		final Value[] values = new Value[numFields];
		
		for (int i = 0; i < numFields; i++)
		{
			final int type = rnd.nextInt(7);
			
			switch (type) {
			case 0:
				values[i] = new PactInteger(rnd.nextInt());
				break;
			case 1:
				values[i] = new PactLong(rnd.nextLong());
				break;
			case 2:
				values[i] = new PactDouble(rnd.nextDouble());
				break;
			case 3:
				values[i] = PactNull.getInstance();
				break;
			case 4:
				values[i] = new PactString(createRandomString(rnd));
				break;
			default:
				values[i] = null;
			}
		}
		
		return values;
	}

	public static String createRandomString(Random rnd)
	{
		return createRandomString(rnd, rnd.nextInt(150));
	}
	
	public static String createRandomString(Random rnd, int length)
	{
		final StringBuilder sb = new StringBuilder();
		sb.ensureCapacity(length);
		
		for (int i = 0; i < length; i++) {
			sb.append((char) (rnd.nextInt(26) + 65));
		}
		return sb.toString();
	}
	
	public static int[] createPermutation(Random rnd, int length)
	{
		final int[] a = new int[length];
		for (int i = 0; i < length; i++) {
			a[i] = i;
		}
		
		for (int i = 0; i < length; i++) {
			final int pos1 = rnd.nextInt(length);
			final int pos2 = rnd.nextInt(length);
			
			int temp = a[pos1];
			a[pos1] = a[pos2];
			a[pos2] = temp;
		}
		return a;
	}
}
