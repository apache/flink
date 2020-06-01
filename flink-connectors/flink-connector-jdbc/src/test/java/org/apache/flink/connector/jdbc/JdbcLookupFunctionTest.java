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

package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.table.JdbcLookupFunction;
import org.apache.flink.connector.jdbc.table.JdbcLookupTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test suite for {@link JdbcLookupFunction}.
 */
public class JdbcLookupFunctionTest extends JdbcLookupTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";
	public static final String DB_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";

	private static String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
	private static TypeInformation[] fieldTypes = new TypeInformation[] {
		BasicTypeInfo.INT_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO,
		BasicTypeInfo.STRING_TYPE_INFO
	};

	private static String[] lookupKeys = new String[] {"id1", "id2"};

	@Test
	public void testEval() throws Exception {

		JdbcLookupFunction lookupFunction = buildLookupFunction();
		ListOutputCollector collector = new ListOutputCollector();
		lookupFunction.setCollector(collector);

		lookupFunction.open(null);

		lookupFunction.eval(1, "1");

		// close connection
		lookupFunction.getDbConnection().close();

		lookupFunction.eval(2, "3");

		List<String> result = Lists.newArrayList(collector.getOutputs()).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	private JdbcLookupFunction buildLookupFunction() {
		JdbcOptions jdbcOptions = JdbcOptions.builder()
			.setDriverName(DB_DRIVER)
			.setDBUrl(DB_URL)
			.setTableName(LOOKUP_TABLE)
			.build();

		JdbcLookupOptions lookupOptions = JdbcLookupOptions.builder().build();

		JdbcLookupFunction lookupFunction = JdbcLookupFunction.builder()
			.setOptions(jdbcOptions)
			.setLookupOptions(lookupOptions)
			.setFieldTypes(fieldTypes)
			.setFieldNames(fieldNames)
			.setKeyNames(lookupKeys)
			.build();

		return lookupFunction;
	}

	private static final class ListOutputCollector implements Collector<Row> {

		private final List<Row> output = new ArrayList<>();

		@Override
		public void collect(Row row) {
			this.output.add(row);
		}

		@Override
		public void close() {}

		public List<Row> getOutputs() {
			return output;
		}
	}
}
