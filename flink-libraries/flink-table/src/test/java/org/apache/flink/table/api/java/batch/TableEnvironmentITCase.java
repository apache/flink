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

package org.apache.flink.table.api.java.batch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase;
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TableEnvironmentITCase extends TableProgramsCollectionTestBase {

	public TableEnvironmentITCase(TableConfigMode configMode) {
		super(configMode);
	}

	@Parameterized.Parameters(name = "Table config = {0}")
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] {
			{ TableProgramsTestBase.DEFAULT() }
		});
	}

	@Test
	public void testSimpleRegister() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet(tableName, ds);
		Table t = tableEnv.scan(tableName);

		Table result = t.select("f0, f1");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
				"8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testRegisterWithFields() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet(tableName, ds, "a, b, c");
		Table t = tableEnv.scan(tableName);

		Table result = t.select("a, b, c");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
				"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
				"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
				"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
				"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
				"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
				"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testRegisterExistingDatasetTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		tableEnv.registerDataSet("MyTable", ds);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
				CollectionDataSets.getSmall5TupleDataSet(env);
		// Must fail. Name is already used for different table.
		tableEnv.registerDataSet("MyTable", ds2);
	}

	@Test(expected = TableException.class)
	public void testScanUnregisteredTable() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. No table registered under that name.
		tableEnv.scan("nonRegisteredTable");
	}

	@Test
	public void testTableRegister() throws Exception {
		final String tableName = "MyTable";
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.registerTable(tableName, t);
		Table result = tableEnv.scan(tableName).select("f0, f1").filter("f0 > 7");

		DataSet<Row> resultSet = tableEnv.toDataSet(result, Row.class);
		List<Row> results = resultSet.collect();
		String expected = "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" +
				"13,5\n" + "14,5\n" + "15,5\n" +
				"16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testIllegalName() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table t = tableEnv.fromDataSet(ds);
		// Must fail. Table name matches internal name pattern.
		tableEnv.registerTable("_DataSetTable_42", t);
	}

	@Test(expected = TableException.class)
	public void testRegisterTableFromOtherEnv() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv1 = TableEnvironment.getTableEnvironment(env, config());
		BatchTableEnvironment tableEnv2 = TableEnvironment.getTableEnvironment(env, config());

		Table t = tableEnv1.fromDataSet(CollectionDataSets.get3TupleDataSet(env));
		// Must fail. Table is bound to different TableEnvironment.
		tableEnv2.registerTable("MyTable", t);
	}

	@Test
	public void testAsFromTuple() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		Table table = tableEnv
			.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c")
			.select("a, b, c");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
			"4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
			"7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
			"11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" +
			"14,5,Comment#8\n" + "15,5,Comment#9\n" + "16,6,Comment#10\n" +
			"17,6,Comment#11\n" + "18,6,Comment#12\n" + "19,6,Comment#13\n" +
			"20,6,Comment#14\n" + "21,6,Comment#15\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromAndToTuple() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		Table table = tableEnv
			.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c")
			.select("a, b, c");

		TypeInformation<?> ti = new TupleTypeInfo<Tuple3<Integer, Long, String>>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);

		DataSet<?> ds = tableEnv.toDataSet(table, ti);
		List<?> results = ds.collect();
		String expected = "(1,1,Hi)\n" + "(2,2,Hello)\n" + "(3,2,Hello world)\n" +
			"(4,3,Hello world, how are you?)\n" + "(5,3,I am fine.)\n" + "(6,3,Luke Skywalker)\n" +
			"(7,4,Comment#1)\n" + "(8,4,Comment#2)\n" + "(9,4,Comment#3)\n" + "(10,4,Comment#4)\n" +
			"(11,5,Comment#5)\n" + "(12,5,Comment#6)\n" + "(13,5,Comment#7)\n" +
			"(14,5,Comment#8)\n" + "(15,5,Comment#9)\n" + "(16,6,Comment#10)\n" +
			"(17,6,Comment#11)\n" + "(18,6,Comment#12)\n" + "(19,6,Comment#13)\n" +
			"(20,6,Comment#14)\n" + "(21,6,Comment#15)\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromTupleToPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<Tuple4<String, Integer, Double, String>> data = new ArrayList<>();
		data.add(new Tuple4<>("Rofl", 1, 1.0, "Hi"));
		data.add(new Tuple4<>("lol", 2, 1.0, "Hi"));
		data.add(new Tuple4<>("Test me", 4, 3.33, "Hello world"));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data), "q, w, e, r")
			.select("q as a, w as b, e as c, r as d");

		DataSet<SmallPojo2> ds = tableEnv.toDataSet(table, SmallPojo2.class);
		List<SmallPojo2> results = ds.collect();
		String expected = "Rofl,1,1.0,Hi\n" + "lol,2,1.0,Hi\n" + "Test me,4,3.33,Hello world\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data),
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromPrivateFieldsPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<PrivateSmallPojo> data = new ArrayList<>();
		data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data),
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromAndToPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<SmallPojo> data = new ArrayList<>();
		data.add(new SmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new SmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data),
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		DataSet<SmallPojo2> ds = tableEnv.toDataSet(table, SmallPojo2.class);
		List<SmallPojo2> results = ds.collect();
		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsFromAndToPrivateFieldPojo() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<PrivateSmallPojo> data = new ArrayList<>();
		data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"));
		data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"));
		data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data),
				"department AS a, " +
				"age AS b, " +
				"salary AS c, " +
				"name AS d")
			.select("a, b, c, d");

		DataSet<PrivateSmallPojo2> ds = tableEnv.toDataSet(table, PrivateSmallPojo2.class);
		List<PrivateSmallPojo2> results = ds.collect();
		String expected =
			"Sales,28,4000.0,Peter\n" +
			"Engineering,56,10000.0,Anna\n" +
			"HR,42,6000.0,Lucy\n";
		compareResultAsText(results, expected);
	}

	@Test
	public void testAsWithPojoAndGenericTypes() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		List<PojoWithGeneric> data = new ArrayList<>();
		data.add(new PojoWithGeneric("Peter", 28, new HashMap<String, String>(), new ArrayList<String>()));
		HashMap<String, String> hm1 = new HashMap<>();
		hm1.put("test1", "test1");
		data.add(new PojoWithGeneric("Anna", 56, hm1, new ArrayList<String>()));
		HashMap<String, String> hm2 = new HashMap<>();
		hm2.put("abc", "cde");
		data.add(new PojoWithGeneric("Lucy", 42, hm2, new ArrayList<String>()));

		Table table = tableEnv
			.fromDataSet(env.fromCollection(data),
				"name AS a, " +
				"age AS b, " +
				"generic AS c, " +
				"generic2 AS d")
			.select("a, b, c, c as c2, d")
			.select("a, b, c, c === c2, d");

		DataSet<Row> ds = tableEnv.toDataSet(table, Row.class);
		List<Row> results = ds.collect();
		String expected =
			"Peter,28,{},true,[]\n" +
			"Anna,56,{test1=test1},true,[]\n" +
			"Lucy,42,{abc=cde},true,[]\n";
		compareResultAsText(results, expected);
	}

	@Test(expected = TableException.class)
	public void testAsWithToFewFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. Not enough field names specified.
		tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b");
	}

	@Test(expected = TableException.class)
	public void testAsWithToManyFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. Too many field names specified.
		tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c, d");
	}

	@Test(expected = TableException.class)
	public void testAsWithAmbiguousFields() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. Specified field names are not unique.
		tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, b");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference1() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. as() does only allow field name expressions
		tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a + 1, b, c");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail. as() does only allow field name expressions
		tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a as foo, b,  c");
	}

	@Test(expected = TableException.class)
	public void testNonStaticClassInput() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail since class is not static
		tableEnv.fromDataSet(env.fromElements(new MyNonStatic()), "name");
	}

	@Test(expected = TableException.class)
	public void testNonStaticClassOutput() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		// Must fail since class is not static
		Table t = tableEnv.fromDataSet(env.fromElements(1, 2, 3), "number");
		tableEnv.toDataSet(t, MyNonStatic.class);
	}

	@Test(expected = TableException.class)
	public void testCustomCalciteConfig() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, config());

		CalciteConfig cc = new CalciteConfigBuilder()
				.replaceLogicalOptRuleSet(RuleSets.ofList())
				.replacePhysicalOptRuleSet(RuleSets.ofList())
				.build();
		tableEnv.getConfig().setCalciteConfig(cc);

		DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.toDataSet(t, Row.class);
	}

	// --------------------------------------------------------------------------------------------

	public class MyNonStatic {
		public int number;
	}

	@SuppressWarnings("unused")
	public static class SmallPojo {

		public SmallPojo() { }

		public SmallPojo(String name, int age, double salary, String department) {
			this.name = name;
			this.age = age;
			this.salary = salary;
			this.department = department;
		}

		public String name;
		public int age;
		public double salary;
		public String department;
	}

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

	@SuppressWarnings("unused")
	public static class SmallPojo2 {

		public SmallPojo2() { }

		public SmallPojo2(String a, int b, double c, String d) {
			this.a = a;
			this.b = b;
			this.c = c;
			this.d = d;
		}

		public String a;
		public int b;
		public double c;
		public String d;

		@Override
		public String toString() {
			return a + "," + b + "," + c + "," + d;
		}
	}

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
