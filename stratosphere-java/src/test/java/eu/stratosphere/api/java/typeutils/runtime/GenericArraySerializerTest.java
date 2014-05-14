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

package eu.stratosphere.api.java.typeutils.runtime;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.typeutils.SerializerTestInstance;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeSerializerTest.Book;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeSerializerTest.BookAuthor;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeSerializerTest.ComplexNestedObject1;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeSerializerTest.ComplexNestedObject2;
import eu.stratosphere.api.java.typeutils.runtime.GenericTypeSerializerTest.SimpleTypes;
import eu.stratosphere.util.StringUtils;

public class GenericArraySerializerTest {
	
	private final Random rnd = new Random(349712539451944123L);
	
	
	@Test
	public void testString() {
		String[] arr1 = new String[] {"abc", "",
				StringUtils.getRandomString(new Random(289347567856686223L), 10, 100),
				StringUtils.getRandomString(new Random(289347567856686223L), 15, 50),
				StringUtils.getRandomString(new Random(289347567856686223L), 30, 170),
				StringUtils.getRandomString(new Random(289347567856686223L), 14, 15),
				""};
		
		String[] arr2 = new String[] {"foo", "",
				StringUtils.getRandomString(new Random(289347567856686223L), 10, 100),
				StringUtils.getRandomString(new Random(289347567856686223L), 1000, 5000),
				StringUtils.getRandomString(new Random(289347567856686223L), 30000, 35000),
				StringUtils.getRandomString(new Random(289347567856686223L), 100*1024, 105*1024),
				"bar"};
		
		// run tests with the string serializer as the component serializer
		runTests(String.class, new StringSerializer(), arr1, arr2);
		
		// run the tests with the generic serializer as the component serializer
		runTests(arr1, arr2);
	}
	
	@Test
	public void testSimpleTypesObjects() {
		SimpleTypes a = new SimpleTypes();
		SimpleTypes b = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		SimpleTypes c = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		SimpleTypes d = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		SimpleTypes e = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		SimpleTypes f = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		SimpleTypes g = new SimpleTypes(rnd.nextInt(), rnd.nextLong(), (byte) rnd.nextInt(),
				StringUtils.getRandomString(rnd, 10, 100), (short) rnd.nextInt(), rnd.nextDouble());
		
		runTests(new SimpleTypes[] {a, b, c}, new SimpleTypes[] {d, e, f, g});
	}
	
	@Test
	public void testCompositeObject() {
		ComplexNestedObject1 o1 = new ComplexNestedObject1(5626435);
		ComplexNestedObject1 o2 = new ComplexNestedObject1(76923);
		ComplexNestedObject1 o3 = new ComplexNestedObject1(-1100);
		ComplexNestedObject1 o4 = new ComplexNestedObject1(0);
		ComplexNestedObject1 o5 = new ComplexNestedObject1(44);
		
		runTests(new ComplexNestedObject1[] {o1, o2}, new ComplexNestedObject1[] {o3}, new ComplexNestedObject1[] {o4, o5});
	}
	
	@Test
	public void testNestedObjects() {
		ComplexNestedObject2 o1 = new ComplexNestedObject2(rnd);
		ComplexNestedObject2 o2 = new ComplexNestedObject2();
		ComplexNestedObject2 o3 = new ComplexNestedObject2(rnd);
		ComplexNestedObject2 o4 = new ComplexNestedObject2(rnd);
		
		runTests(	new ComplexNestedObject2[] {o1, o2, o3},
					new ComplexNestedObject2[] {},
					new ComplexNestedObject2[] {},
					new ComplexNestedObject2[] {o4},
					new ComplexNestedObject2[] {});
	}
	
	@Test
	public void testBeanStyleObjects() {
		{
			Book b1 = new Book(976243875L, "The Serialization Odysse", 42);
			Book b2 = new Book(0L, "Debugging byte streams", 1337);
			Book b3 = new Book(-1L, "Low level interfaces", 0xC0FFEE);
			Book b4 = new Book(Long.MAX_VALUE, "The joy of bits and bytes", 0xDEADBEEF);
			Book b5 = new Book(Long.MIN_VALUE, "Winnign a prize for creative test strings", 0xBADF00);
			Book b6 = new Book(-2L, "Distributed Systems", 0xABCDEF0123456789L);
			
			runTests(	new Book[] {b1, b2},
						new Book[] {},
						new Book[] {},
						new Book[] {},
						new Book[] {},
						new Book[] {b3, b4, b5, b6});
		}
		
		// object with collection
		{
			ArrayList<String> list = new ArrayList<String>();
			list.add("A");
			list.add("B");
			list.add("C");
			list.add("D");
			list.add("E");
			
			BookAuthor b1 = new BookAuthor(976243875L, list, "Arno Nym");
			
			ArrayList<String> list2 = new ArrayList<String>();
			BookAuthor b2 = new BookAuthor(987654321L, list2, "The Saurus");
			
			runTests(new BookAuthor[] {b1, b2});
		}
	}

	private final <T> void runTests(T[]... instances) {
		try {
			@SuppressWarnings("unchecked")
			Class<T> type = (Class<T>) instances[0][0].getClass();
			TypeSerializer<T> serializer = new AvroSerializer<T>(type);
			runTests(type, serializer, instances);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	private final <T> void runTests(Class<T> type, TypeSerializer<T> componentSerializer, T[]... instances) {
		try {
			if (type == null || componentSerializer == null || instances == null || instances.length == 0) {
				throw new IllegalArgumentException();
			}
			
			@SuppressWarnings("unchecked")
			Class<T[]> arrayClass = (Class<T[]>) Array.newInstance(type, 0).getClass();
			
			GenericArraySerializer<T> serializer = createSerializer(type, componentSerializer);
			SerializerTestInstance<T[]> test = new SerializerTestInstance<T[]>(serializer, arrayClass, -1, instances);
			test.testAll();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	private final <T> GenericArraySerializer<T> createSerializer(Class<T> type, TypeSerializer<T> componentSerializer) {
		return new GenericArraySerializer<T>(type, componentSerializer);
	}
}
