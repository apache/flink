/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link JsonBatchTableSourceFactory}.
 */
public class JsonBatchTableSourceFactoryTest extends MultipleProgramsTestBase {

	private static final String[] FIELD_NAMES =  {"author", "title", "price", "zip_code"};
	private static final TypeInformation[] FIELD_TYPES =  {BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO
	};

	private String basePath;
	private BatchTableEnvironment tEnv;
	private FileSystem connector;

	public JsonBatchTableSourceFactoryTest() {
		super(TestExecutionMode.COLLECTION);
	}

	@Before
	public void create() throws IOException {
		basePath = JsonFileUtils.createJsonFile();
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		tEnv = BatchTableEnvironment.create(env);
		connector = new FileSystem().path(basePath);
	}

	@Test
	public void testWithFormatSchema() throws Exception {
		tEnv.connect(connector)
			.withFormat(new Json()
				.schema(new RowTypeInfo(FIELD_TYPES, FIELD_NAMES))
				.failOnMissingField(true)
			)
			.registerTableSource("jsonTable");

		String fullSql = "select author, title, price, zip_code FROM jsonTable";
		Table fullTable = tEnv.sqlQuery(fullSql);

		DataSet<Row> fullDS = tEnv.toDataSet(fullTable, Row.class);
		List<Row> full = fullDS.collect();
		assertEquals(1, full.size());
		assertEquals(
			"J. R. R. Tolkien,The Lord of the Rings,22.99,94025",
			full.get(0).toString());

		String countSql =
			"SELECT author, count(price) FROM jsonTable group by author";
		Table countTable = tEnv.sqlQuery(countSql);
		DataSet<Row> countDS = tEnv.toDataSet(countTable, Row.class);
		List<Row> count = countDS.collect();
		assertEquals(1, count.size());
		assertEquals(
			"J. R. R. Tolkien,1",
			count.get(0).toString());
	}

	@Test
	public void testWithFormatJsonSchema() throws Exception {
		tEnv.connect(connector)
			.withFormat(new Json()
				.jsonSchema("{\"type\":\"object\",\"properties\":{" +
				"\"author\":{\"type\":\"string\"}," +
				"\"title\":{\"type\":\"string\"}," +
				"\"price\":{\"type\":\"number\"}," +
				"\"zip_code\":{\"type\":\"string\"}" +
				"}}")
				.failOnMissingField(true))
			.registerTableSource("jsonTable");

		String fullSql = "select author, title, price, zip_code FROM jsonTable";
		Table fullTable = tEnv.sqlQuery(fullSql);

		DataSet<Row> fullDS = tEnv.toDataSet(fullTable, Row.class);
		List<Row> full = fullDS.collect();
		assertEquals(1, full.size());
		assertEquals(
			"J. R. R. Tolkien,The Lord of the Rings,22.99,94025",
			full.get(0).toString());

		String countSql =
			"SELECT author, count(price) FROM jsonTable group by author";
		Table countTable = tEnv.sqlQuery(countSql);
		DataSet<Row> countDS = tEnv.toDataSet(countTable, Row.class);
		List<Row> count = countDS.collect();
		assertEquals(1, count.size());
		assertEquals(
			"J. R. R. Tolkien,1",
			count.get(0).toString());
	}

	@Test
	public void testWithSchema() throws Exception {
		tEnv.connect(connector)
			.withFormat(new Json().failOnMissingField(true).deriveSchema())
			.withSchema(
				new Schema()
					.field("author", Types.STRING())
					.field("title", Types.STRING())
					.field("price", Types.DOUBLE())
					.field("zip_code", Types.STRING())
			)
			.registerTableSource("jsonTable");

		String fullSql = "select author, title, price, zip_code FROM jsonTable";
		Table fullTable = tEnv.sqlQuery(fullSql);

		DataSet<Row> fullDS = tEnv.toDataSet(fullTable, Row.class);
		List<Row> full = fullDS.collect();
		assertEquals(1, full.size());
		assertEquals(
			"J. R. R. Tolkien,The Lord of the Rings,22.99,94025",
			full.get(0).toString());

		String countSql =
			"SELECT author, count(price) FROM jsonTable group by author";
		Table countTable = tEnv.sqlQuery(countSql);
		DataSet<Row> countDS = tEnv.toDataSet(countTable, Row.class);
		List<Row> count = countDS.collect();
		assertEquals(1, count.size());
		assertEquals(
			"J. R. R. Tolkien,1",
			count.get(0).toString());
	}

	@After
	public void delete() {
		if (basePath != null) {
			new File(basePath).delete();
		}
	}

}
