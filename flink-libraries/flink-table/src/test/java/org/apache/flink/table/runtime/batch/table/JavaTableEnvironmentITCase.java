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

package org.apache.flink.table.runtime.batch.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableAlreadyExistException;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.runtime.batch.sql.BatchTestBase;
import org.apache.flink.table.runtime.batch.sql.TestData;
import org.apache.flink.types.Either;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.runtime.batch.sql.JavaSqlITCase.getResult;
import static org.apache.flink.test.util.TestBaseUtils.compareResultAsText;

/**
 * Integration tests for {@link BatchTableEnvironment}.
 */
public class JavaTableEnvironmentITCase extends BatchTestBase {

	@Test
	public void testSimpleRegister() throws Exception {
		final String tableName = "MyTable";

		registerCollection(tableName, TestData.data3(), TestData.type3(), "a, b, c");

		Table t = tEnv().scan(tableName);

		Table result = t.select("a, b");

		String expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
				"8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testRegisterWithFields() throws Exception {
		final String tableName = "MyTable";

		registerCollection(tableName, TestData.data3(), TestData.type3(), "a, b, c");

		Table t = tEnv().scan(tableName);

		Table result = t.select("a, b, c");

		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
				"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
				"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
				"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
				"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
				"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
				"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test(expected = TableAlreadyExistException.class)
	public void testRegisterExistingDatasetTable() throws Exception {
		registerCollection("MyTable", TestData.data3(), TestData.type3(), "a, b, c");
		registerCollection("MyTable", TestData.data5(), TestData.type5(), "a, b, c, d, e");
	}

	@Test(expected = TableException.class)
	public void testScanUnregisteredTable() throws Exception {
		// Must fail. No table registered under that name.
		tEnv().scan("nonRegisteredTable");
	}

	@Test
	public void testTableRegister() throws Exception {
		final String tableName = "MyTable";

		registerCollection(tableName, TestData.data3(), TestData.type3(), "f0, f1, f2");

		Table result = tEnv().scan(tableName).select("f0, f1").filter("f0 > 7");

		String expected = "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" +
				"13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test(expected = TableException.class)
	public void testRegisterTableFromOtherEnv() throws Exception {
		BatchTableEnvironment tEnv2 = TableEnvironment.getBatchTableEnvironment(javaEnv(), conf());

		registerCollectionOfJavaTableEnv("T", TestData.data3(), TestData.type3(), "f0, f1, f2");

		Table t = javaTableEnv().scan("T");
		// Must fail. Table is bound to different tEnv()ironment.
		tEnv2.registerTable("MyTable", t);
	}

	@Test
	public void testAsFromTupleByPosition() throws Exception {

		registerCollection("T", TestData.data3(), TestData.type3(), "a, b, c");

		Table table = tEnv().scan("T")
			.select("a, b, c");

		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
			"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
			"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
			"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
			"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
			"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromTupleByName() throws Exception {
		registerCollection("T", TestData.data3(), TestData.type3(), "f2");
		Table table = tEnv().scan("T")
				.select("f2");

		String expected = "Hi\n" + "Hello\n" + "Hello world\n" +
			"Hello world, how are you?\n" + "I am fine.\n" + "Luke Skywalker\n" +
			"Comment#1\n" + "Comment#2\n" + "Comment#3\n" + "Comment#4\n" +
			"Comment#5\n" + "Comment#6\n" + "Comment#7\n" +
			"Comment#8\n" + "Comment#9\n" + "Comment#10\n" +
			"Comment#11\n" + "Comment#12\n" + "Comment#13\n" +
			"Comment#14\n" + "Comment#15\n";
		compareResultAsText(getResult(table), expected);
	}

	@Ignore
	@Test
	public void testAsFromTupleToPojo() throws Exception {

		List<Tuple4<String, Integer, Double, String>> data = new ArrayList<>();
		data.add(new Tuple4<>("Rofl", 1, 1.0, "Hi"));
		data.add(new Tuple4<>("lol", 2, 1.0, "Hi"));
		data.add(new Tuple4<>("Test me", 4, 3.33, "Hello world"));

		Table table = javaTableEnv().fromCollection(data, "q, w, e, r")
			.select("q as a, w as b, e as c, r as d");

		String expected = "Rofl,1,1.0,Hi\n" + "lol,2,1.0,Hi\n" + "Test me,4,3.33,Hello world\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromPojo() throws Exception {

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales", new Integer[] {42}));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering", new Integer[] {}));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR", new Integer[] {1, 2, 3}));

		Table table = javaTableEnv().fromCollection(data,
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d," +
				"roles as e")
			.select("a, b, c, d, e");

		String expected =
			"Sales,28,4000.0,Peter,[42]\n" +
			"Engineering,56,10000.0,Anna,[]\n" +
			"HR,42,6000.0,Lucy,[1, 2, 3]\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testFromNonAtomicAndNonComposite() throws Exception {

		List<Either<String, Integer>> data = new ArrayList<>();
		data.add(new Either.Left<>("Hello"));
		data.add(new Either.Right<>(42));
		data.add(new Either.Left<>("World"));

		TypeInformation<Either<String, Integer>> typeInfo =
				TypeInformation.of(new TypeHint<Either<String, Integer>>() { });
		Table table = javaTableEnv().fromCollection(data, typeInfo, "either").select("either");

		String expected =
			"Left(Hello)\n" +
			"Left(World)\n" +
			"Right(42)\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromPojoProjected() throws Exception {

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales", new Integer[] {42}));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering", new Integer[] {}));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR", new Integer[] {1, 2, 3}));

		Table table = javaTableEnv().fromCollection(data, "name as d")
			.select("d");

		String expected =
			"Peter\n" +
			"Anna\n" +
			"Lucy\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromPrivateFieldsPojo() throws Exception {

		List<PrivateSmallPojo> data = new ArrayList<>();
		data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = javaTableEnv()
			.fromCollection(data,
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromAndToPojo() throws Exception {

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales", new Integer[] {42}));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering", new Integer[] {}));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR", new Integer[] {1, 2, 3}));

		Table table = javaTableEnv()
				.fromCollection(data,
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d," +
				"roles AS e")
			.select("a, b, c, d, e");

		String expected =
			"Sales,28,4000.0,Peter,[42]\n" +
			"Engineering,56,10000.0,Anna,[]\n" +
			"HR,42,6000.0,Lucy,[1, 2, 3]\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsFromAndToPrivateFieldPojo() throws Exception {

		List<PrivateSmallPojo> data = new ArrayList<>();
		data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = javaTableEnv()
				.fromCollection(data,
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test
	public void testAsWithPojoAndGenericTypes() throws Exception {

		List<PojoWithGeneric> data = new ArrayList<>();
		data.add(new PojoWithGeneric("Peter", 28, new HashMap<String, String>(), new ArrayList<String>()));
		HashMap<String, String> hm1 = new HashMap<>();
		hm1.put("test1", "test1");
		data.add(new PojoWithGeneric("Anna", 56, hm1, new ArrayList<String>()));
		HashMap<String, String> hm2 = new HashMap<>();
		hm2.put("abc", "cde");
		data.add(new PojoWithGeneric("Lucy", 42, hm2, new ArrayList<String>()));

		Table table = javaTableEnv()
				.fromCollection(data,
				"name AS a, " +
				"age AS b, " +
				"generic AS c, " +
				"generic2 AS d")
			.select("a, b, c, c as c2, d")
			.select("a, b, c, c === c2, d");

		String expected =
			"Peter,28,{},true,[]\n" +
			"Anna,56,{test1=test1},true,[]\n" +
			"Lucy,42,{abc=cde},true,[]\n";
		compareResultAsText(getResult(table), expected);
	}

	@Test(expected = TableException.class)
	public void testAsWithToManyFields() throws Exception {
		// Must fail. Too many field names specified.
		registerCollectionOfJavaTableEnv("T", TestData.data3(), TestData.type3(), "a, b, c, d");
	}

	@Test(expected = TableException.class)
	public void testAsWithAmbiguousFields() throws Exception {
		// Must fail. Specified field names are not unique.
		registerCollectionOfJavaTableEnv("T", TestData.data3(), TestData.type3(), "a, b, b");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference1() throws Exception {
		registerCollectionOfJavaTableEnv("T", TestData.data3(), TestData.type3(), "a + 1, b, c");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference2() throws Exception {
		registerCollectionOfJavaTableEnv("T", TestData.data3(), TestData.type3(), "a as foo, b, c");
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Small POJO.
	 */
	@SuppressWarnings("unused")
	public static class SmallPojo {

		public SmallPojo() { }

		public SmallPojo(String name, int age, double salary, String department, Integer[] roles) {
			this.name = name;
			this.age = age;
			this.salary = salary;
			this.department = department;
			this.roles = roles;
		}

		public String name;
		public int age;
		public double salary;
		public String department;
		public Integer[] roles;
	}

	/**
	 * POJO with generic fields.
	 */
	@SuppressWarnings("unused")
	public static class PojoWithGeneric {
		public String name;
		public int age;
		public HashMap<String, String> generic;
		public ArrayList<String> generic2;

		public PojoWithGeneric() {
			// default constructor
		}

		public PojoWithGeneric(String name, int age, HashMap<String, String> generic,
				ArrayList<String> generic2) {
			this.name = name;
			this.age = age;
			this.generic = generic;
			this.generic2 = generic2;
		}

		@Override
		public String toString() {
			return name + "," + age + "," + generic + "," + generic2;
		}
	}

	/**
	 * Small POJO with private fields.
	 */
	@SuppressWarnings("unused")
	public static class PrivateSmallPojo {

		public PrivateSmallPojo() { }

		public PrivateSmallPojo(String name, int age, double salary, String department) {
			this.name = name;
			this.age = age;
			this.salary = salary;
			this.department = department;
		}

		private String name;
		private int age;
		private double salary;
		private String department;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public double getSalary() {
			return salary;
		}

		public void setSalary(double salary) {
			this.salary = salary;
		}

		public String getDepartment() {
			return department;
		}

		public void setDepartment(String department) {
			this.department = department;
		}
	}

	/**
	 * Another small POJO.
	 */
	@SuppressWarnings("unused")
	public static class SmallPojo2 {

		public SmallPojo2() { }

		public SmallPojo2(String a, int b, double c, String d, Integer[] e) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
			this.e = e;
		}

		public String a;
		public int b;
		public double c;
		public String d;
		public Integer[] e;

		@Override
		public String toString() {
			return a + "," + b + "," + c + "," + d + "," + Arrays.toString(e);
		}
	}

	/**
	 * Another small POJO with private fields.
	 */
	@SuppressWarnings("unused")
	public static class PrivateSmallPojo2 {

		public PrivateSmallPojo2() { }

		public PrivateSmallPojo2(String a, int b, double c, String d) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
		}

		private String a;
		private int b;
		private double c;
		private String d;

		public String getA() {
			return a;
		}

		public void setA(String a) {
			this.a = a;
		}

		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}

		public double getC() {
			return c;
		}

		public void setC(double c) {
			this.c = c;
		}

		public String getD() {
			return d;
		}

		public void setD(String d) {
			this.d = d;
		}

		@Override
		public String toString() {
			return a + "," + b + "," + c + "," + d;
		}
	}
}
