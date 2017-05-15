/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

abstract public class AbstractGenericTypeComparatorTest {

	@Test
	public void testString() {
		runTests("",
				"Lorem Ipsum Dolor Omit Longer",
				"aaaa",
				"abcd",
				"abce",
				"abdd",
				"accd",
				"bbcd");
	}

	@Test
	public void testSimpleTypesObjects() {
		runTests(
				new SimpleTypes(0, 1, (byte) 2, "", (short) 3, 4.0),
				new SimpleTypes(1, 1, (byte) 2, "", (short) 3, 4.0),
				new SimpleTypes(1, 2, (byte) 2, "", (short) 3, 4.0),
				new SimpleTypes(1, 2, (byte) 3, "", (short) 3, 4.0),
				new SimpleTypes(1, 2, (byte) 3, "a", (short) 3, 4.0),
				new SimpleTypes(1, 2, (byte) 3, "b", (short) 3, 4.0),
				new SimpleTypes(1, 2, (byte) 3, "b", (short) 4, 4.0),
				new SimpleTypes(1, 2, (byte) 3, "b", (short) 4, 6.0)
		);
	}

	@Test
	public void testCompositeObject() {
		ComplexNestedObject1 o1 = new ComplexNestedObject1(-1100);
		ComplexNestedObject1 o2 = new ComplexNestedObject1(0);
		ComplexNestedObject1 o3 = new ComplexNestedObject1(44);
		ComplexNestedObject1 o4 = new ComplexNestedObject1(76923, "A");
		ComplexNestedObject1 o5 = new ComplexNestedObject1(5626435, "A somewhat random collection");

		runTests(o1, o2, o3, o4, o5);
	}

	@Test
	public void testBeanStyleObjects() {
		{
			Book b111 = new Book(-1L, "A Low level interfaces", 0xC);
			Book b122 = new Book(-1L, "Low level interfaces", 0xC);
			Book b123 = new Book(-1L, "Low level interfaces", 0xC0FFEE);

			Book b2 = new Book(0L, "Debugging byte streams", 1337);
			Book b3 = new Book(976243875L, "The Serialization Odysse", 42);

			runTests(b111, b122, b123, b2, b3);
		}

		{
			BookAuthor b1 = new BookAuthor(976243875L, new ArrayList<String>(), "Arno Nym");

			ArrayList<String> list = new ArrayList<String>();
			list.add("A");
			list.add("B");
			list.add("C");
			list.add("D");
			list.add("E");

			BookAuthor b2 = new BookAuthor(976243875L, list, "The Saurus");

			runTests(b1, b2);
		}
	}

	// ------------------------------------------------------------------------

	private <T> void runTests(T... sortedTestData) {
		ComparatorTestInstance<T> testBase = new ComparatorTestInstance<T>(sortedTestData);
		testBase.testAll();
	}

	abstract protected <T> TypeSerializer<T> createSerializer(Class<T> type);

	// ------------------------------------------------------------------------
	// test instance
	// ------------------------------------------------------------------------

	private class ComparatorTestInstance<T> extends ComparatorTestBase<T> {

		private final T[] testData;

		private final Class<T> type;

		@SuppressWarnings("unchecked")
		public ComparatorTestInstance(T[] testData) {
			if (testData == null || testData.length == 0) {
				throw new IllegalArgumentException();
			}

			this.testData = testData;
			this.type = (Class<T>) testData[0].getClass();
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected TypeComparator<T> createComparator(boolean ascending) {
			return new GenericTypeComparator(ascending, AbstractGenericTypeComparatorTest.this.createSerializer(this
					.type), this.type);
		}

		@Override
		protected TypeSerializer<T> createSerializer() {
			return AbstractGenericTypeComparatorTest.this.createSerializer(this.type);
		}

		@Override
		protected T[] getSortedTestData() {
			return this.testData;
		}

		public void testAll() {
			testDuplicate();
			testEquality();
			testEqualityWithReference();
			testInequality();
			testInequalityWithReference();
			testNormalizedKeysEqualsFullLength();
			testNormalizedKeysEqualsHalfLength();
			testNormalizedKeysGreatSmallFullLength();
			testNormalizedKeysGreatSmallAscDescHalfLength();
			testNormalizedKeyReadWriter();
		}
	}

	// ------------------------------------------------------------------------
	//  test objects
	// ------------------------------------------------------------------------

	public static final class SimpleTypes implements Comparable<SimpleTypes> {

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
		public String toString() {
			return String.format("(%d, %d, %d, %s, %d, %f)", iVal, lVal, bVal, sVal, rVal, dVal);
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

		@Override
		public int compareTo(SimpleTypes o) {
			int cmp = (this.iVal < o.iVal ? -1 : (this.iVal == o.iVal ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			cmp = (this.lVal < o.lVal ? -1 : (this.lVal == o.lVal ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			cmp = (this.bVal < o.bVal ? -1 : (this.bVal == o.bVal ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			cmp = this.sVal.compareTo(o.sVal);
			if (cmp != 0) {
				return cmp;
			}

			cmp = (this.rVal < o.rVal ? -1 : (this.rVal == o.rVal ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			return (this.dVal < o.dVal ? -1 : (this.dVal == o.dVal ? 0 : 1));
		}
	}

	public static class ComplexNestedObject1 implements Comparable<ComplexNestedObject1> {

		private double doubleValue;

		private List<String> stringList;

		public ComplexNestedObject1() {
		}

		public ComplexNestedObject1(double value, String... listElements) {
			this.doubleValue = value;

			this.stringList = new ArrayList<String>();
			for (String str : listElements) {
				this.stringList.add(str);
			}
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

		@Override
		public int compareTo(ComplexNestedObject1 o) {
			int cmp = (this.doubleValue < o.doubleValue ? -1 : (this.doubleValue == o.doubleValue ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			int size = this.stringList.size();
			int otherSize = o.stringList.size();

			cmp = (size < otherSize ? -1 : (size == otherSize ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			for (int i = 0; i < size; i++) {
				cmp = this.stringList.get(i).compareTo(o.stringList.get(i));
				if (cmp != 0) {
					return cmp;
				}
			}

			return 0;
		}
	}

	public static class Book implements Comparable<Book> {

		private long bookId;
		private String title;
		private long authorId;

		public Book() {
		}

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

		@Override
		public int compareTo(Book o) {
			int cmp = (this.bookId < o.bookId ? -1 : (this.bookId == o.bookId ? 0 : 1));
			if (cmp != 0) {
				return cmp;
			}

			cmp = title.compareTo(o.title);
			if (cmp != 0) {
				return cmp;
			}

			return (this.authorId < o.authorId ? -1 : (this.authorId == o.authorId ? 0 : 1));
		}
	}

	public static class BookAuthor implements Comparable<BookAuthor> {

		private long authorId;
		private List<String> bookTitles;
		private String authorName;

		public BookAuthor() {
		}

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

		@Override
		public int compareTo(BookAuthor o) {
			int cmp = (this.authorId < o.authorId ? -1 : (this.authorId == o.authorId ? 0 : 1));
			if (cmp != 0) return cmp;

			int size = this.bookTitles.size();
			int oSize = o.bookTitles.size();
			cmp = (size < oSize ? -1 : (size == oSize ? 0 : 1));
			if (cmp != 0) return cmp;

			for (int i = 0; i < size; i++) {
				cmp = this.bookTitles.get(i).compareTo(o.bookTitles.get(i));
				if (cmp != 0) return cmp;
			}

			return this.authorName.compareTo(o.authorName);
		}
	}
}
