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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * IT case for lookup source of JDBC connector.
 */
@RunWith(Parameterized.class)
public class JdbcLookupTableITCase extends JdbcLookupTestBase {

	private final String tableFactory;
	private final boolean useCache;

	public JdbcLookupTableITCase(String tableFactory, boolean useCache) {
		this.useCache = useCache;
		this.tableFactory = tableFactory;
	}

	@Parameterized.Parameters(name = "Table factory = {0}, use cache {1}")
	@SuppressWarnings("unchecked,rawtypes")
	public static Collection<Object[]>  useCache() {
		return Arrays.asList(new Object[][]{
			{"legacyFactory", true},
			{"legacyFactory", false},
			{"dynamicFactory", true},
			{"dynamicFactory", false}});
	}

	@Test
	public void testLookup() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Iterator<Row> collected;
		if ("legacyFactory".equals(tableFactory)) {
			collected = useLegacyTableFactory(env, tEnv);
		} else {
			collected = useDynamicTableFactory(env, tEnv);
		}
		List<String> result = CollectionUtil.iteratorToList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v1,11-c2-v1");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("1,1,11-c1-v2,11-c2-v2");
		expected.add("2,3,null,23-c2");
		expected.add("2,5,25-c1,25-c2");
		expected.add("3,8,38-c1,38-c2");
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	private Iterator<Row> useLegacyTableFactory(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, "1"),
			new Tuple2<>(1, "1"),
			new Tuple2<>(2, "3"),
			new Tuple2<>(2, "5"),
			new Tuple2<>(3, "5"),
			new Tuple2<>(3, "8")
		)), $("id1"), $("id2"));

		tEnv.registerTable("T", t);
		JdbcTableSource.Builder builder = JdbcTableSource.builder()
			.setOptions(JdbcOptions.builder()
				.setDBUrl(DB_URL)
				.setTableName(LOOKUP_TABLE)
				.build())
			.setSchema(TableSchema.builder().fields(
				new String[]{"id1", "comment1", "comment2", "id2"},
				new DataType[]{DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()})
				.build());
		JdbcLookupOptions.Builder lookupOptionsBuilder = JdbcLookupOptions.builder().setMaxRetryTimes(0);
		if (useCache) {
			lookupOptionsBuilder.setCacheMaxSize(1000).setCacheExpireMs(1000 * 1000);
		}
		builder.setLookupOptions(lookupOptionsBuilder.build());
		tEnv.registerFunction("jdbcLookup",
			builder.build().getLookupFunction(t.getSchema().getFieldNames()));

		// do not use the first N fields as lookup keys for better coverage
		String sqlQuery = "SELECT id1, id2, comment1, comment2 FROM T, " +
			"LATERAL TABLE(jdbcLookup(id1, id2)) AS S(l_id1, comment1, comment2, l_id2)";
		return tEnv.executeSql(sqlQuery).collect();
	}

	private Iterator<Row> useDynamicTableFactory(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, "1"),
			new Tuple2<>(1, "1"),
			new Tuple2<>(2, "3"),
			new Tuple2<>(2, "5"),
			new Tuple2<>(3, "5"),
			new Tuple2<>(3, "8")
		)), $("id1"), $("id2"), $("proctime").proctime());

		tEnv.createTemporaryView("T", t);

		String cacheConfig = ", 'lookup.cache.max-rows'='4', 'lookup.cache.ttl'='10000'";
		tEnv.executeSql(
			String.format("create table lookup (" +
				"  id1 INT," +
				"  comment1 VARCHAR," +
				"  comment2 VARCHAR," +
				"  id2 VARCHAR" +
				") with(" +
				"  'connector'='jdbc'," +
				"  'url'='" + DB_URL + "'," +
				"  'table-name'='" + LOOKUP_TABLE + "'," +
				"  'lookup.max-retries' = '0'" +
				"  %s)", useCache ? cacheConfig : ""));

		// do not use the first N fields as lookup keys for better coverage
		String sqlQuery = "SELECT source.id1, source.id2, L.comment1, L.comment2 FROM T AS source " +
			"JOIN lookup for system_time as of source.proctime AS L " +
			"ON source.id1 = L.id1 and source.id2 = L.id2";
		return tEnv.executeSql(sqlQuery).collect();
	}
}
