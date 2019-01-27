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

package org.apache.flink.table.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSourceParser;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.catalog.CatalogTestUtil.toRow;
import static org.junit.Assert.assertEquals;

/**
 * IT case for ReadableCatalog.
 */
public class ReadableCatalogITCase extends StreamingTestBase {

	private StreamExecutionEnvironment env;
	private StreamTableEnvironment streamEnv;
	private BatchTableEnvironment batchEnv;
	private ReadableWritableCatalog catalog;

	private final String catalogName = "test";
	private final String dbName = "db";

	@Before
	public void init() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		streamEnv = TableEnvironment.getTableEnvironment(env, new TableConfig());
		batchEnv = TableEnvironment.getBatchTableEnvironment(env, new TableConfig());

		catalog = new FlinkInMemoryCatalog(catalogName);
		catalog.createDatabase(dbName, new CatalogDatabase(), false);
		batchEnv.registerCatalog(catalogName, catalog);
		batchEnv.setDefaultDatabase(catalogName, dbName);

		streamEnv.registerCatalog(catalogName, catalog);
		streamEnv.setDefaultDatabase(catalogName, dbName);

		CollectionTableFactory.checkParam = false;
	}

	@Test
	public void testSqlUpdate_withJoin() {
		// Expected results
		List<Row> expected = new ArrayList<>();
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));

		// Test resolving <table>
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		batchEnv.sqlUpdate("insert into t1 with (tableType = '3') select t1.b, w.a " +
			"from t1 with (tableType = '1') " +
			"join t1 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t1.a = w.a");
		batchEnv.execute();

		assertEquals(expected, CollectionTableFactory.RESULT);

		// Test resolving <db>.<table>
		catalog.createTable(new ObjectPath(dbName, "t2"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		batchEnv.sqlUpdate("insert into db.t2 with (tableType = '3') select t2.b, w.a " +
			"from db.t2 with (tableType = '1') " +
			"join db.t2 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t2.a = w.a");

		batchEnv.execute();

		assertEquals(expected, CollectionTableFactory.RESULT);

		// Test resolving <catalog>.<db>.<table>
		catalog.createTable(new ObjectPath(dbName, "t3"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		batchEnv.sqlUpdate("insert into test.db.t3 with (tableType = '3') select t3.b, w.a " +
			"from test.db.t3 with (tableType = '1') " +
			"join test.db.t3 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t3.a = w.a");
		batchEnv.execute();

		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	@Test
	public void testSqlUpdate() {
		// Test resolving <table>
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		// Test resolving <db>.<table>
		catalog.createTable(new ObjectPath(dbName, "t2"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		batchEnv.sqlUpdate("insert into t2 select * from t1");
		batchEnv.execute();

		assertEquals(CatalogTestUtil.getTestData(), CollectionTableFactory.RESULT);

		batchEnv.sqlUpdate("insert into db.t2 select * from db.t1");
		batchEnv.execute();

		assertEquals(CatalogTestUtil.getTestData(), CollectionTableFactory.RESULT);

		batchEnv.sqlUpdate("insert into test.db.t2 select * from test.db.t1");
		batchEnv.execute();

		assertEquals(CatalogTestUtil.getTestData(), CollectionTableFactory.RESULT);
	}

	@Test
	public void testSqlQuery() {
		// Expected results
		List<Row> expected = new ArrayList<>();
		expected.add(toRow(new Integer(1), new Integer(2)));
		expected.add(toRow(new Integer(1), new Integer(3)));

		// Test resolving <table>
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		Table result1 = batchEnv.sqlQuery("select * from t1");
		result1.collect();

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result1.collect().toList()).asJava());

		// Test resolving <db>.<table>
		Table result2 = batchEnv.sqlQuery("select * from db.t1");
		result2.collect();

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result2.collect().toList()).asJava());

		// Test resolving <catalog>.<db>.<table>
		Table result3 = batchEnv.sqlQuery("select * from test.db.t1");
		result3.collect();

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result3.collect().toList()).asJava());
	}

	@Test
	public void testSqlQuery_withJoin() {
		// Expected results
		List<Row> expected = new ArrayList<>();
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));

		// Test resolving <table>
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);
		Table result = batchEnv.sqlQuery("select t1.b, w.a " +
			"from t1 with (tableType = '1') " +
			"join t1 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t1.a = w.a");

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result.collect().toList()).asJava());

		// Test resolving <db>.<table>
		result = batchEnv.sqlQuery("select t1.b, w.a " +
			"from db.t1 with (tableType = '1') " +
			"join db.t1 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t1.a = w.a");

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result.collect().toList()).asJava());

		// Test resolving <catalog>.<db>.<table>
		result = batchEnv.sqlQuery("select t1.b, w.a " +
			"from test.db.t1 with (tableType = '1') " +
			"join test.db.t1 with (tableType = '2') " +
			"FOR SYSTEM_TIME AS OF PROCTIME() AS w on t1.a = w.a");

		assertEquals(expected, scala.collection.JavaConverters.seqAsJavaListConverter(result.collect().toList()).asJava());
	}

	@Test
	public void testStreamSourceParser() {
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(true), false);

		List<String> parameters = new ArrayList<>();
		parameters.add("b");
		parameters.add("a");
		CollectionTableFactory.parser = new TableSourceParser(new Parser(), parameters);

		streamEnv.sqlUpdate("insert into t1 select * from t1 ");
		streamEnv.execute();

		List<Row> expected = new ArrayList<>();
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}

	@Test
	public void testBatchSourceParser() {
		catalog.createTable(new ObjectPath(dbName, "t1"), CatalogTestUtil.createCatalogTableWithPrimaryKey(false), false);

		List<String> parameters = new ArrayList<>();
		parameters.add("b");
		parameters.add("a");
		CollectionTableFactory.parser = new TableSourceParser(new Parser(), parameters);

		batchEnv.sqlUpdate("insert into t1 select * from t1 ");
		batchEnv.execute();

		List<Row> expected = new ArrayList<>();
		expected.add(toRow(new Integer(2), new Integer(1)));
		expected.add(toRow(new Integer(3), new Integer(1)));
		assertEquals(expected, CollectionTableFactory.RESULT);
	}
}
