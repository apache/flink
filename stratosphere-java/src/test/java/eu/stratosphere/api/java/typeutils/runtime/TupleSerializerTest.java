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

import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.runtime.GenericSerializerTest.Book;
import eu.stratosphere.api.java.typeutils.runtime.GenericSerializerTest.BookAuthor;
import eu.stratosphere.api.java.typeutils.runtime.GenericSerializerTest.ComplexNestedObject1;
import eu.stratosphere.api.java.typeutils.runtime.GenericSerializerTest.ComplexNestedObject2;
import eu.stratosphere.api.java.typeutils.runtime.GenericSerializerTest.SimpleTypes;
import eu.stratosphere.util.StringUtils;

public class TupleSerializerTest {
	
	@Test
	public void testTuple1Int() {
		@SuppressWarnings("unchecked")
		Tuple1<Integer>[] testTuples = new Tuple1[] {
			new Tuple1<Integer>(42), new Tuple1<Integer>(1), new Tuple1<Integer>(0), new Tuple1<Integer>(-1),
			new Tuple1<Integer>(Integer.MAX_VALUE), new Tuple1<Integer>(Integer.MIN_VALUE)
		};
		
		runTests(testTuples);
	}
	
	@Test
	public void testTuple1String() {
		Random rnd = new Random(68761564135413L);
		
		@SuppressWarnings("unchecked")
		Tuple1<String>[] testTuples = new Tuple1[] {
			new Tuple1<String>(StringUtils.getRandomString(rnd, 10, 100)),
			new Tuple1<String>("abc"),
			new Tuple1<String>(""),
			new Tuple1<String>(StringUtils.getRandomString(rnd, 30, 170)),
			new Tuple1<String>(StringUtils.getRandomString(rnd, 15, 50)),
			new Tuple1<String>("")
		};
		
		runTests(testTuples);
	}
	
	@Test
	public void testTuple1StringArray() {
		Random rnd = new Random(289347567856686223L);
		
		String[] arr1 = new String[] {"abc", "",
				StringUtils.getRandomString(rnd, 10, 100),
				StringUtils.getRandomString(rnd, 15, 50),
				StringUtils.getRandomString(rnd, 30, 170),
				StringUtils.getRandomString(rnd, 14, 15),
				""};
		
		String[] arr2 = new String[] {"foo", "",
				StringUtils.getRandomString(rnd, 10, 100),
				StringUtils.getRandomString(rnd, 1000, 5000),
				StringUtils.getRandomString(rnd, 30000, 35000),
				StringUtils.getRandomString(rnd, 100*1024, 105*1024),
				"bar"};
		
		@SuppressWarnings("unchecked")
		Tuple1<String[]>[] testTuples = new Tuple1[] {
			new Tuple1<String[]>(arr1),
			new Tuple1<String[]>(arr2)
		};
		
		runTests(testTuples);
	}
	
	@Test
	public void testTuple2StringDouble() {
		Random rnd = new Random(807346528946L);
		
		@SuppressWarnings("unchecked")
		Tuple2<String, Double>[] testTuples = new Tuple2[] {
				new Tuple2<String, Double>(StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble()),
				new Tuple2<String, Double>(StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble()),
				new Tuple2<String, Double>(StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble()),
				new Tuple2<String, Double>("", rnd.nextDouble()),
				new Tuple2<String, Double>(StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble()),
				new Tuple2<String, Double>(StringUtils.getRandomString(rnd, 10, 100), rnd.nextDouble())
			};
		
		runTests(testTuples);
	}
	
	@Test
	public void testTuple2StringStringArray() {
		Random rnd = new Random(289347567856686223L);
		
		String[] arr1 = new String[] {"abc", "",
				StringUtils.getRandomString(rnd, 10, 100),
				StringUtils.getRandomString(rnd, 15, 50),
				StringUtils.getRandomString(rnd, 30, 170),
				StringUtils.getRandomString(rnd, 14, 15),
				""};
		
		String[] arr2 = new String[] {"foo", "",
				StringUtils.getRandomString(rnd, 10, 100),
				StringUtils.getRandomString(rnd, 1000, 5000),
				StringUtils.getRandomString(rnd, 30000, 35000),
				StringUtils.getRandomString(rnd, 100*1024, 105*1024),
				"bar"};
		
		@SuppressWarnings("unchecked")
		Tuple2<String, String[]>[] testTuples = new Tuple2[] {
			new Tuple2<String, String[]>(StringUtils.getRandomString(rnd, 30, 170), arr1),
			new Tuple2<String, String[]>(StringUtils.getRandomString(rnd, 30, 170), arr2),
			new Tuple2<String, String[]>(StringUtils.getRandomString(rnd, 30, 170), arr1),
			new Tuple2<String, String[]>(StringUtils.getRandomString(rnd, 30, 170), arr2),
			new Tuple2<String, String[]>(StringUtils.getRandomString(rnd, 30, 170), arr2)
		};
		
		runTests(testTuples);
	}
	

	@Test
	public void testTuple5CustomObjects() {
		Random rnd = new Random(807346528946L);
		
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
		
		ComplexNestedObject1 o1 = new ComplexNestedObject1(5626435);
		ComplexNestedObject1 o2 = new ComplexNestedObject1(76923);
		ComplexNestedObject1 o3 = new ComplexNestedObject1(-1100);
		ComplexNestedObject1 o4 = new ComplexNestedObject1(0);
		ComplexNestedObject1 o5 = new ComplexNestedObject1(44);
		
		ComplexNestedObject2 co1 = new ComplexNestedObject2(rnd);
		ComplexNestedObject2 co2 = new ComplexNestedObject2();
		ComplexNestedObject2 co3 = new ComplexNestedObject2(rnd);
		ComplexNestedObject2 co4 = new ComplexNestedObject2(rnd);
		
		Book b1 = new Book(976243875L, "The Serialization Odysse", 42);
		Book b2 = new Book(0L, "Debugging byte streams", 1337);
		Book b3 = new Book(-1L, "Low level interfaces", 0xC0FFEE);
		Book b4 = new Book(Long.MAX_VALUE, "The joy of bits and bytes", 0xDEADBEEF);
		Book b5 = new Book(Long.MIN_VALUE, "Winnign a prize for creative test strings", 0xBADF00);
		Book b6 = new Book(-2L, "Distributed Systems", 0xABCDEF0123456789L);
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("A");
		list.add("B");
		list.add("C");
		list.add("D");
		list.add("E");
		
		BookAuthor ba1 = new BookAuthor(976243875L, list, "Arno Nym");
		
		ArrayList<String> list2 = new ArrayList<String>();
		BookAuthor ba2 = new BookAuthor(987654321L, list2, "The Saurus");
		
		
		@SuppressWarnings("unchecked")
		Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>[] testTuples = new Tuple5[] {
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(a, b1, o1, ba1, co1),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(b, b2, o2, ba2, co2),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(c, b3, o3, ba1, co3),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(d, b2, o4, ba1, co4),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(e, b4, o5, ba2, co4),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(f, b5, o1, ba2, co4),
				new Tuple5<SimpleTypes, Book, ComplexNestedObject1, BookAuthor, ComplexNestedObject2>(g, b6, o4, ba1, co2)
		};
		
		runTests(testTuples);
	}

	private final <T extends Tuple> void runTests(T... instances) {
		try {
			TupleTypeInfo<T> tupleTypeInfo = (TupleTypeInfo<T>) TypeExtractor.getForObject(instances[0]);
			TupleSerializer<T> serializer = tupleTypeInfo.createSerializer();
			
			Class<T> tupleClass = tupleTypeInfo.getTypeClass();
			
			TupleSerializerTestInstance<T> test = new TupleSerializerTestInstance<T>(serializer, tupleClass, -1, instances);
			test.testAll();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
}
