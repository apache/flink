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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * IT case for lookup source of Elasticsearch connector.
 */
@RunWith(Parameterized.class)
public class Elasticsearch7LookupTableITCase extends Elasticsearch7DynamicTableTestBase {

	private final String stringType;
	private final boolean useCache;

	public Elasticsearch7LookupTableITCase(String stringType, boolean useCache) {
		this.stringType = stringType;
		this.useCache = useCache;
	}

	@Parameterized.Parameters(name = "Elasticsearch string type = {0}, use cache {1}")
	@SuppressWarnings("unchecked,rawtypes")
	public static Collection<Object[]> useCache() {
		return Arrays.asList(new Object[][]{
			{"keyword", true},
			{"keyword", false},
			{"text", true},
			{"text", false}});
	}

	private final String lookupIndexPrefix = "lookup-index";
	private final String lookupType = "lookup-type";
	private String index;

	@Before
	public void before() throws IOException, ExecutionException, InterruptedException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		index = lookupIndexPrefix + "-" + stringType + "-" + useCache;
		assertTrue(createIndex(client, index, lookupType, stringType));
		insertData(tEnv, index, lookupType);
		// if not sleep some time, several unit test will not pass. Because the Elasticsearch will get empty result.
		Thread.sleep(1000);
	}

	/**
	 * Elasticsearch string has two diffirent type text and keyword in high level version.
	 * Diffirence:
	 * Keyword: store the original content.
	 * Text: will be analyzed and converted to lowercase.
	 */
	@Test
	public void testLookup() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		List<String> result;
		if ("keyword".equals(stringType)) {
			result = runLookup(env, tEnv, index, lookupType);
			List<String> expected =
				Stream.of(
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"2,B,3,00:00:12,2003-10-21,2012-12-12T12:12:13")
					.sorted()
					.collect(
						Collectors.toList()
					);
			assertEquals(expected, result);
		} else if ("text".equals(stringType)) {
			result = runLookup(env, tEnv, index, lookupType);
			List<String> expected =
				Stream.of(
					"1,A B,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12",
					"1,A,2,00:00:12,2003-10-20,2012-12-12T12:12:12")
					.sorted()
					.collect(
						Collectors.toList()
					);
			assertEquals(expected, result);
		}
	}

	private List<String> runLookup(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String index, String type) {
		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1L, "a"),
			new Tuple2<>(1L, "A"),
			new Tuple2<>(1L, "A"),
			new Tuple2<>(2L, "B"),
			new Tuple2<>(2L, "F"),
			new Tuple2<>(3L, "F"),
			new Tuple2<>(4L, "F")
		)), $("id1"), $("id2"), $("proctime").proctime());

		tEnv.createTemporaryView("T", t);

		String cacheConfig = ", 'lookup.cache.max-rows'='4', 'lookup.cache.ttl'='60000', 'lookup.max-retries'='5'";
		tEnv.executeSql("CREATE TABLE lookup (" +
			"a BIGINT NOT NULL,\n" +
			"b STRING NOT NULL,\n" +
			"c FLOAT,\n" +
			"d TINYINT NOT NULL,\n" +
			"e TIME,\n" +
			"f DATE,\n" +
			"g TIMESTAMP NOT NULL,\n" +
			"PRIMARY KEY (c, d) NOT ENFORCED\n" +
			")\n" +
			"WITH (\n" +
			String.format("'%s'='%s',\n", "connector", "elasticsearch-7") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.INDEX_OPTION.key(), index) +
			String.format("'%s'='%s'\n", ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200") +
			String.format("%s", useCache ? cacheConfig : "") +
			")");

		String sqlQuery = "SELECT L.a, L.b, L.d, L.e, L.f, L.g FROM T AS source " +
			"JOIN lookup for system_time as of source.proctime AS L " +
			"ON source.id1 = L.a and source.id2 = L.b";

		List<Row> collected = Lists.newArrayList(tEnv.executeSql(sqlQuery).collect());
		return Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());
	}
}
