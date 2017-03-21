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
import java.util.HashMap;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.utils.BatchTableTestUtil;
import org.apache.flink.table.utils.TableTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableEnvironmentTest extends TableTestBase {

	private DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet() {
		DataSet<Tuple3<Integer, Long, String>> ds = mock(DataSet.class);

		TupleTypeInfo<Tuple3<Integer, Long, String>> typeInfo =
		new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO
		);
		when(ds.getType()).thenReturn(typeInfo);

		return ds;
	}

	private DataSet<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataSet() {
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds = mock(DataSet.class);

		TupleTypeInfo<Tuple5<Integer, Long, Integer, String, Long>> typeInfo =
			new TupleTypeInfo<>(
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO
			);
		when(ds.getType()).thenReturn(typeInfo);

		return ds;
	}

	@Test
	public void testSimpleRegister() throws Exception {
		final String tableName = "MyTable";
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		tableEnv.registerDataSet(tableName, ds);
		Table t = tableEnv.scan(tableName);

		Table result = t.select("f0, f1");

		String expected = "DataSetCalc(select=[f0, f1])\n" +
			"  DataSetScan(table=[[MyTable]])";

		util.verifyTable(result, expected, tableEnv);

	}

	@Test
	public void testRegisterWithFields() throws Exception {
		final String tableName = "MyTable";
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		tableEnv.registerDataSet(tableName, ds, "a, b, c");
		Table t = tableEnv.scan(tableName);

		Table result = t.select("a, b, c");

		String expected = "DataSetScan(table=[[MyTable]])";

		util.verifyTable(result, expected, tableEnv);

	}

	@Test(expected = TableException.class)
	public void testRegisterExistingDatasetTable() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		tableEnv.registerDataSet("MyTable", ds);
		DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 = get5TupleDataSet();
		// Must fail. Name is already used for different table.
		tableEnv.registerDataSet("MyTable", ds2);
	}

	@Test(expected = TableException.class)
	public void testScanUnregisteredTable() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. No table registered under that name.
		tableEnv.scan("nonRegisteredTable");
	}

	@Test
	public void testTableRegister() throws Exception {
		final String tableName = "MyTable";
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.registerTable(tableName, t);
		Table result = tableEnv.scan(tableName).select("f0, f1").filter("f0 > 7");

		String expected = "DataSetCalc(select=[f0, f1], where=[>(f0, 7)])\n" +
			"  DataSetScan(table=[[_DataSetTable_0]])";

		util.verifyTable(result, expected, tableEnv);
	}

	@Test(expected = TableException.class)
	public void testTableUnregister() throws Exception {
		final String tableName = "MyTable";
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.registerTable(tableName, t);
		tableEnv.unregisterTable(tableName);
		// Must fail. Table name is not register anymore.
		tableEnv.scan(tableName).select("f0, f1").filter("f0 > 7");
	}

	@Test
	public void testTableRegisterNew() throws Exception {
		final String tableName = "MyTable";
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		Table t = tableEnv.fromDataSet(ds);
		tableEnv.registerTable(tableName, t);

		tableEnv.unregisterTable(tableName);

		Table t2 = tableEnv.fromDataSet(ds).filter("f0 > 8");
		tableEnv.registerTable(tableName, t2);

		Table result = tableEnv.scan(tableName).select("f0, f1").filter("f0 > 7");

		String expected = "DataSetCalc(select=[f0, f1], where=[>(f0, 7)])\n" +
			"  DataSetCalc(select=[f0, f1, f2], where=[>(f0, 8)])\n" +
			"    DataSetScan(table=[[_DataSetTable_1]])";

		util.verifyTable(result, expected, tableEnv);
	}

	@Test(expected = TableException.class)
	public void testIllegalName() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
		Table t = tableEnv.fromDataSet(ds);
		// Must fail. Table name matches internal name pattern.
		tableEnv.registerTable("_DataSetTable_42", t);
	}

	@Test(expected = TableException.class)
	public void testRegisterTableFromOtherEnv() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv1 = TableEnvironment.getTableEnvironment(util.jEnv());
		BatchTableEnvironment tableEnv2 = TableEnvironment.getTableEnvironment(util.jEnv());

		Table t = tableEnv1.fromDataSet(get3TupleDataSet());
		// Must fail. Table is bound to different TableEnvironment.
		tableEnv2.registerTable("MyTable", t);
	}

	@Test(expected = TableException.class)
	public void testAsWithToFewFields() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. Not enough field names specified.
		tableEnv.fromDataSet(get3TupleDataSet(), "a, b");
	}

	@Test(expected = TableException.class)
	public void testAsWithToManyFields() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. Too many field names specified.
		tableEnv.fromDataSet(get3TupleDataSet(), "a, b, c, d");
	}

	@Test(expected = TableException.class)
	public void testAsWithAmbiguousFields() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. Specified field names are not unique.
		tableEnv.fromDataSet(get3TupleDataSet(), "a, b, b");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference1() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. as() does only allow field name expressions
		tableEnv.fromDataSet(get3TupleDataSet(), "a + 1, b, c");
	}

	@Test(expected = TableException.class)
	public void testAsWithNonFieldReference2() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail. as() does only allow field name expressions
		tableEnv.fromDataSet(get3TupleDataSet(), "a as foo, b,  c");
	}

	@Test(expected = TableException.class)
	public void testNonStaticClassInput() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// Must fail since class is not static
		tableEnv.fromDataSet(env.fromElements(new MyNonStatic()), "name");
	}

	@Test(expected = TableException.class)
	public void testNonStaticClassOutput() throws Exception {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		// Must fail since class is not static
		Table t = tableEnv.fromDataSet(util.jEnv().fromElements(1, 2, 3), "number");
		tableEnv.toDataSet(t, MyNonStatic.class);
	}

	@Test(expected = TableException.class)
	public void testCustomCalciteConfig() {
		BatchTableTestUtil util = new BatchTableTestUtil();
		BatchTableEnvironment tableEnv = util.jTableEnv();

		CalciteConfig cc = new CalciteConfigBuilder().replaceOptRuleSet(RuleSets.ofList()).build();
		tableEnv.getConfig().setCalciteConfig(cc);

		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet();
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
