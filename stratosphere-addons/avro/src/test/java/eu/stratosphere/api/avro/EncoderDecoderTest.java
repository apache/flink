/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;

import eu.stratosphere.api.java.record.io.avro.generated.Colors;
import eu.stratosphere.api.java.record.io.avro.generated.User;

import static org.junit.Assert.*;


/**
 * Tests the {@link DataOutputEncoder} and {@link DataInputDecoder} classes for Avro serialization.
 */
public class EncoderDecoderTest {
	
	@Test
	public void testPrimitiveTypes() {
		
		testObjectSerialization(new Boolean(true));
		testObjectSerialization(new Boolean(false));
		
		testObjectSerialization(new Byte((byte) 0));
		testObjectSerialization(new Byte((byte) 1));
		testObjectSerialization(new Byte((byte) -1));
		testObjectSerialization(new Byte(Byte.MIN_VALUE));
		testObjectSerialization(new Byte(Byte.MAX_VALUE));
		
		testObjectSerialization(new Short((short) 0));
		testObjectSerialization(new Short((short) 1));
		testObjectSerialization(new Short((short) -1));
		testObjectSerialization(new Short(Short.MIN_VALUE));
		testObjectSerialization(new Short(Short.MAX_VALUE));
		
		testObjectSerialization(new Integer(0));
		testObjectSerialization(new Integer(1));
		testObjectSerialization(new Integer(-1));
		testObjectSerialization(new Integer(Integer.MIN_VALUE));
		testObjectSerialization(new Integer(Integer.MAX_VALUE));
		
		testObjectSerialization(new Long(0));
		testObjectSerialization(new Long(1));
		testObjectSerialization(new Long(-1));
		testObjectSerialization(new Long(Long.MIN_VALUE));
		testObjectSerialization(new Long(Long.MAX_VALUE));
		
		testObjectSerialization(new Float(0));
		testObjectSerialization(new Float(1));
		testObjectSerialization(new Float(-1));
		testObjectSerialization(new Float((float)Math.E));
		testObjectSerialization(new Float((float)Math.PI));
		testObjectSerialization(new Float(Float.MIN_VALUE));
		testObjectSerialization(new Float(Float.MAX_VALUE));
		testObjectSerialization(new Float(Float.MIN_NORMAL));
		testObjectSerialization(new Float(Float.NaN));
		testObjectSerialization(new Float(Float.NEGATIVE_INFINITY));
		testObjectSerialization(new Float(Float.POSITIVE_INFINITY));
		
		testObjectSerialization(new Double(0));
		testObjectSerialization(new Double(1));
		testObjectSerialization(new Double(-1));
		testObjectSerialization(new Double(Math.E));
		testObjectSerialization(new Double(Math.PI));
		testObjectSerialization(new Double(Double.MIN_VALUE));
		testObjectSerialization(new Double(Double.MAX_VALUE));
		testObjectSerialization(new Double(Double.MIN_NORMAL));
		testObjectSerialization(new Double(Double.NaN));
		testObjectSerialization(new Double(Double.NEGATIVE_INFINITY));
		testObjectSerialization(new Double(Double.POSITIVE_INFINITY));
		
		testObjectSerialization("");
		testObjectSerialization("abcdefg");
		testObjectSerialization("ab\u1535\u0155xyz\u706F");
		
		testObjectSerialization(new SimpleTypes(3637, 54876486548L, (byte) 65, "We're out looking for astronauts", (short) 0x2387, 2.65767523));
		testObjectSerialization(new SimpleTypes(705608724, -1L, (byte) -65, "Serve me the sky with a big slice of lemon", (short) Byte.MIN_VALUE, 0.0000001));
	}
	
	@Test
	public void testArrayTypes() {
		{
			int[] array = new int[] {1, 2, 3, 4, 5};
			testObjectSerialization(array);
		}
		{
			long[] array = new long[] {1, 2, 3, 4, 5};
			testObjectSerialization(array);
		}
		{
			float[] array = new float[] {1, 2, 3, 4, 5};
			testObjectSerialization(array);
		}
		{
			double[] array = new double[] {1, 2, 3, 4, 5};
			testObjectSerialization(array);
		}
		{
			String[] array = new String[] {"Oh", "my", "what", "do", "we", "have", "here", "?"};
			testObjectSerialization(array);
		}
	}
	
	@Test
	public void testObjects() {
		testObjectSerialization(new Book(976243875L, "The Serialization Odysse", 42));
		
		{
			ArrayList<String> list = new ArrayList<String>();
			list.add("A");
			list.add("B");
			list.add("C");
			list.add("D");
			list.add("E");
			
			testObjectSerialization(new BookAuthor(976243875L, list, "Arno Nym"));
		}
	}
	
	@Test
	public void testNestedObjectsWithCollections() {
		testObjectSerialization(new ComplexNestedObject2(true));
	}
	
	@Test
	public void testGeneratedObjectWithNullableFields() {
		List<CharSequence> strings = Arrays.asList(new CharSequence[] { "These", "strings", "should", "be", "recognizable", "as", "a", "meaningful", "sequence" });
		List<Boolean> bools = Arrays.asList(true, true, false, false, true, false, true, true);
		Map<CharSequence, Long> map = new HashMap<CharSequence, Long>();
		map.put("1", 1L);
		map.put("2", 2L);
		map.put("3", 3L);
		
		User user = new User("Freudenreich", 1337, "macintosh gray", 1234567890L, 3.1415926, null, true, strings, bools, null, Colors.GREEN, map);
		
		testObjectSerialization(user);
	}
	
	@Test
	public void testVarLenCountEncoding() {
		try {
			long[] values = new long[] { 0, 1, 2, 3, 4, 0, 574, 45236, 0, 234623462, 23462462346L, 0, 9734028767869761L, 0x7fffffffffffffffL};
			
			// write
			ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
			{
				DataOutputStream dataOut = new DataOutputStream(baos);
				
				for (long val : values) {
					DataOutputEncoder.writeVarLongCount(dataOut, val);
				}
				
				dataOut.flush();
				dataOut.close();
			}
			
			// read
			{
				ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
				DataInputStream dataIn = new DataInputStream(bais);
				
				for (long val : values) {
					long read = DataInputDecoder.readVarLongCount(dataIn);
					assertEquals("Wrong var-len encoded value read.", val, read);
				}
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test failed due to an exception: " + e.getMessage());
		}
	}
	
	private static <X> void testObjectSerialization(X obj) {
		
		try {
			
			// serialize
			ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
			{
				DataOutputStream dataOut = new DataOutputStream(baos);
				DataOutputEncoder encoder = new DataOutputEncoder();
				encoder.setOut(dataOut);
				
				@SuppressWarnings("unchecked")
				Class<X> clazz = (Class<X>) obj.getClass();
				ReflectDatumWriter<X> writer = new ReflectDatumWriter<X>(clazz);
				
				writer.write(obj, encoder);
				dataOut.flush();
				dataOut.close();
			}
			
			byte[] data = baos.toByteArray();
			X result = null;
			
			// deserialize
			{
				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				DataInputStream dataIn = new DataInputStream(bais);
				DataInputDecoder decoder = new DataInputDecoder();
				decoder.setIn(dataIn);

				@SuppressWarnings("unchecked")
				Class<X> clazz = (Class<X>) obj.getClass();
				ReflectDatumReader<X> reader = new ReflectDatumReader<X>(clazz);
				
				// create a reuse object if possible, otherwise we have no reuse object 
				X reuse = null;
				try {
					@SuppressWarnings("unchecked")
					X test = (X) obj.getClass().newInstance();
					reuse = test;
				} catch (Throwable t) {}
				
				result = reader.read(reuse, decoder);
			}
			
			// check
			final String message = "Deserialized object is not the same as the original";
			
			if (obj.getClass().isArray()) {
				Class<?> clazz = obj.getClass();
				if (clazz == byte[].class) {
					assertArrayEquals(message, (byte[]) obj, (byte[]) result);
				}
				else if (clazz == short[].class) {
					assertArrayEquals(message, (short[]) obj, (short[]) result);
				}
				else if (clazz == int[].class) {
					assertArrayEquals(message, (int[]) obj, (int[]) result);
				}
				else if (clazz == long[].class) {
					assertArrayEquals(message, (long[]) obj, (long[]) result);
				}
				else if (clazz == char[].class) {
					assertArrayEquals(message, (char[]) obj, (char[]) result);
				}
				else if (clazz == float[].class) {
					assertArrayEquals(message, (float[]) obj, (float[]) result, 0.0f);
				}
				else if (clazz == double[].class) {
					assertArrayEquals(message, (double[]) obj, (double[]) result, 0.0);
				} else {
					assertArrayEquals(message, (Object[]) obj, (Object[]) result);
				}
			} else {
				assertEquals(message, obj, result);
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test failed due to an exception: " + e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Test Objects
	// --------------------------------------------------------------------------------------------


	public static final class SimpleTypes {
		
		private final int iVal;
		private final long lVal;
		private final byte bVal;
		private final String sVal;
		private final short rVal;
		private final double dVal;
		
		
		public SimpleTypes() {
			this(0, 0, (byte) 0, "", (short) 0, 0);
		}
		
		public SimpleTypes(int iVal, long lVal, byte bVal, String sVal, short rVal, double dVal) {
			this.iVal = iVal;
			this.lVal = lVal;
			this.bVal = bVal;
			this.sVal = sVal;
			this.rVal = rVal;
			this.dVal = dVal;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == SimpleTypes.class) {
				SimpleTypes other = (SimpleTypes) obj;
				
				return other.iVal == this.iVal &&
						other.lVal == this.lVal &&
						other.bVal == this.bVal &&
						other.sVal.equals(this.sVal) &&
						other.rVal == this.rVal &&
						other.dVal == this.dVal;
				
			} else {
				return false;
			}
		}
	}
	
	public static class ComplexNestedObject1 {
		
		private double doubleValue;
		
		private List<String> stringList;
		
		public ComplexNestedObject1() {}
		
		public ComplexNestedObject1(int offInit) {
			this.doubleValue = 6293485.6723 + offInit;
				
			this.stringList = new ArrayList<String>();
			this.stringList.add("A" + offInit);
			this.stringList.add("somewhat" + offInit);
			this.stringList.add("random" + offInit);
			this.stringList.add("collection" + offInit);
			this.stringList.add("of" + offInit);
			this.stringList.add("strings" + offInit);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == ComplexNestedObject1.class) {
				ComplexNestedObject1 other = (ComplexNestedObject1) obj;
				return other.doubleValue == this.doubleValue && this.stringList.equals(other.stringList);
			} else {
				return false;
			}
		}
	}
	
	public static class ComplexNestedObject2 {
		
		private long longValue;
		
		private Map<String, ComplexNestedObject1> theMap;
		
		public ComplexNestedObject2() {}
		
		public ComplexNestedObject2(boolean init) {
			this.longValue = 46547;
				
			this.theMap = new HashMap<String, ComplexNestedObject1>();
			this.theMap.put("36354L", new ComplexNestedObject1(43546543));
			this.theMap.put("785611L", new ComplexNestedObject1(45784568));
			this.theMap.put("43L", new ComplexNestedObject1(9876543));
			this.theMap.put("-45687L", new ComplexNestedObject1(7897615));
			this.theMap.put("1919876876896L", new ComplexNestedObject1(27154));
			this.theMap.put("-868468468L", new ComplexNestedObject1(546435));
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == ComplexNestedObject2.class) {
				ComplexNestedObject2 other = (ComplexNestedObject2) obj;
				return other.longValue == this.longValue && this.theMap.equals(other.theMap);
			} else {
				return false;
			}
		}
	}
	
	public static class Book {

		private long bookId;
		private String title;
		private long authorId;

		public Book() {}

		public Book(long bookId, String title, long authorId) {
			this.bookId = bookId;
			this.title = title;
			this.authorId = authorId;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == Book.class) {
				Book other = (Book) obj;
				return other.bookId == this.bookId && other.authorId == this.authorId && this.title.equals(other.title);
			} else {
				return false;
			}
		}
	}

	public static class BookAuthor {

		private long authorId;
		private List<String> bookTitles;
		private String authorName;

		public BookAuthor() {}

		public BookAuthor(long authorId, List<String> bookTitles, String authorName) {
			this.authorId = authorId;
			this.bookTitles = bookTitles;
			this.authorName = authorName;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == BookAuthor.class) {
				BookAuthor other = (BookAuthor) obj;
				return other.authorName.equals(this.authorName) && other.authorId == this.authorId &&
						other.bookTitles.equals(this.bookTitles);
			} else {
				return false;
			}
		}
	}
}
