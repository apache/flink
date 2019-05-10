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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
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
 * Unit tests for {@link JsonBatchTableSource}.
 */
public class JsonBatchTableSourceTest extends MultipleProgramsTestBase {

	private static final String[] FIELD_NAMES =  {"author", "title", "price", "zip_code"};
	private static final TypeInformation[] FIELD_TYPES =  {BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.DOUBLE_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO
	};

	private String basePath = null;

	public JsonBatchTableSourceTest() {
		super(TestExecutionMode.COLLECTION);
	}

	@Before
	public void create() throws IOException {
		basePath = JsonFileUtils.createJsonFile();
	}

	@Test
	public void testFullScan() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		JsonBatchTableSource json = new JsonBatchTableSource(basePath, FIELD_NAMES, FIELD_TYPES);
		tEnv.registerTableSource("jsonTable", json);

		String query =
			"SELECT author, title, price, zip_code FROM jsonTable";
		Table t = tEnv.sqlQuery(query);

		DataSet<Row> dataSet = tEnv.toDataSet(t, Row.class);
		List<Row> result = dataSet.collect();

		assertEquals(1, result.size());
		assertEquals(
			"J. R. R. Tolkien,The Lord of the Rings,22.99,94025",
			result.get(0).toString());
	}

	@Test
	public void testScanWithProjection() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		JsonBatchTableSource json = new JsonBatchTableSource(basePath, FIELD_NAMES, FIELD_TYPES);
		json.projectFields(new int[] {0, 2});
		tEnv.registerTableSource("jsonTable", json);

		String query =
			"SELECT author, sum(price) FROM jsonTable group by author";
		Table t = tEnv.sqlQuery(query);

		DataSet<Row> dataSet = tEnv.toDataSet(t, Row.class);
		List<Row> result = dataSet.collect();

		assertEquals(1, result.size());
		assertEquals(
			"J. R. R. Tolkien,22.99",
			result.get(0).toString());
	}

	@After
	public void delete() {
		if (basePath != null) {
			new File(basePath).delete();
		}
	}

}
