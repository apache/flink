package eu.stratosphere.pact.common.type;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

public class PactRecordTest {
	
	private static final long SEED = 354144423270432543L;
	private final Random rand = new Random(PactRecordTest.SEED);
	
	private DataInputStream in;
	private DataOutputStream out;
	
	// Couple of test values
	private PactString origVal1 = new PactString("Hello World!");
	private PactDouble origVal2 = new PactDouble(Math.PI);
	private PactInteger origVal3 = new PactInteger(1337);
	
	

	@Before
	public void setUp() throws Exception {
		PipedInputStream pipedInput = new PipedInputStream(1000);
		this.in = new DataInputStream(pipedInput);
		this.out = new DataOutputStream(new PipedOutputStream(pipedInput));
	}

	@Test
	public void testAddField() {
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
	public void testSetNullInt() {
		PactRecord record = this.generateFilledDenseRecord(58);

		record.setNull(42);
		assertTrue(record.getNumFields() == 58);
		assertTrue(record.getField(42, PactInteger.class) == null);
	}

	@Test
	public void testSetNullLong() {
		PactRecord record = this.generateFilledDenseRecord(58);
		long mask = generateRandomBitmask(58);

		record.setNull(mask);

		for (int i = 0; i < 58; i++) {
			if (((1l << i) & mask) != 0) {
				assertTrue(record.getField(i, PactInteger.class) == null);
			}
		}

		assertTrue(record.getNumFields() == 58);
	}

	@Test
	public void testSetNullLongArray() {
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
	public void testUpdateBinaryRepresentations() {
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
	}
	
	@Test
	public void testDeSerialization() {
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
	}
	
	@Test
	public void recordFail() throws IOException {
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

}
