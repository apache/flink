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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.ElasticsearchResource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;
import static org.apache.flink.table.api.Expressions.row;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * IT tests for {@link Elasticsearch6DynamicSink}.
 */
public class Elasticsearch6DynamicSourceITCase {

	@ClassRule
	public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource("es-6-dynamic-source-tests");

	@Test
	public void testWritingDocuments() throws Exception {
		TableSchema schema = TableSchema.builder()
			.field("a", DataTypes.BIGINT().notNull())
			.field("b", DataTypes.TIME())
			.field("c", DataTypes.STRING().notNull())
			.field("d", DataTypes.FLOAT())
			.field("e", DataTypes.TINYINT().notNull())
			.field("f", DataTypes.DATE())
			.field("g", DataTypes.TIMESTAMP().notNull())
			.primaryKey("a", "g")
			.build();
		GenericRowData rowData = GenericRowData.of(
			1L,
			12345,
			StringData.fromString("ABCDE"),
			12.12f,
			(byte) 2,
			12345,
			TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")));

		String index = "writing-documents";
		String myType = "MyType";
		Elasticsearch6DynamicTableFactory sinkFactory = new Elasticsearch6DynamicTableFactory();

		SinkFunctionProvider sinkRuntimeProvider = (SinkFunctionProvider) sinkFactory.createDynamicTableSink(
			context()
				.withSchema(schema)
				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), index)
				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType)
				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200")
				.withOption(ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false")
				.build()
		).getSinkRuntimeProvider(new MockContext());

		SinkFunction<RowData> sinkFunction = sinkRuntimeProvider.createSinkFunction();
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		rowData.setRowKind(RowKind.UPDATE_AFTER);
		environment.<RowData>fromElements(rowData).addSink(sinkFunction);
		environment.execute();

		Client client = elasticsearchResource.getClient();
		Map<String, Object> response = client.get(new GetRequest(index, myType, "1_2012-12-12T12:12:12")).actionGet().getSource();
		Map<Object, Object> expectedMap = new HashMap<>();
		expectedMap.put("a", 1);
		expectedMap.put("b", "00:00:12Z");
		expectedMap.put("c", "ABCDE");
		expectedMap.put("d", 12.12d);
		expectedMap.put("e", 2);
		expectedMap.put("f", "2003-10-20");
		expectedMap.put("g", "2012-12-12T12:12:12Z");
		assertThat(response, equalTo(expectedMap));
	}
//	@Test
//	public void testScanDocuments() throws Exception {
//		TableSchema schema = TableSchema.builder()
//			.field("a", DataTypes.BIGINT().notNull())
//			.field("b", DataTypes.TIME())
//			.field("c", DataTypes.STRING().notNull())
//			.field("d", DataTypes.FLOAT())
//			.field("e", DataTypes.TINYINT().notNull())
//			.field("f", DataTypes.DATE())
//			.field("g", DataTypes.TIMESTAMP().notNull())
//			.primaryKey("a", "g")
//			.build();
//		GenericRowData rowData = GenericRowData.of(
//			1L,
//			12345,
//			StringData.fromString("ABCDE"),
//			12.12f,
//			(byte) 2,
//			12345,
//			TimestampData.fromLocalDateTime(LocalDateTime.parse("2012-12-12T12:12:12")));
//
//		String index = "writing-documents";
//		String myType = "MyType";
//		Elasticsearch6DynamicTableFactory sinkFactory = new Elasticsearch6DynamicTableFactory();
//
//		SinkFunctionProvider sinkRuntimeProvider = (SinkFunctionProvider) sinkFactory.createDynamicTableSink(
//			context()
//				.withSchema(schema)
//				.withOption(ElasticsearchOptions.INDEX_OPTION.key(), index)
//				.withOption(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType)
//				.withOption(ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200")
//				.withOption(ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false")
//				.build()
//		).getSinkRuntimeProvider(new MockContext());
//
//		SinkFunction<RowData> sinkFunction = sinkRuntimeProvider.createSinkFunction();
//		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//		rowData.setRowKind(RowKind.UPDATE_AFTER);
//		environment.<RowData>fromElements(rowData).addSink(sinkFunction);
//		environment.execute();
//
//		Client client = elasticsearchResource.getClient();
//		Map<String, Object> response = client.get(new GetRequest(index, myType, "1_2012-12-12T12:12:12")).actionGet().getSource();
//		Map<Object, Object> expectedMap = new HashMap<>();
//		expectedMap.put("a", 1);
//		expectedMap.put("b", "00:00:12Z");
//		expectedMap.put("c", "ABCDE");
//		expectedMap.put("d", 12.12d);
//		expectedMap.put("e", 2);
//		expectedMap.put("f", "2003-10-20");
//		expectedMap.put("g", "2012-12-12T12:12:12Z");
//		assertThat(response, equalTo(expectedMap));
//	}

	@Test
	public void testWritingDocumentsFromTableApi() throws Exception {
		TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build());

		String index = "table-api";
		String myType = "MyType";
		tableEnvironment.executeSql("CREATE TABLE esTable (" +
			"a BIGINT NOT NULL,\n" +
			"b TIME,\n" +
			"c STRING NOT NULL,\n" +
			"d FLOAT,\n" +
			"e TINYINT NOT NULL,\n" +
			"f DATE,\n" +
			"g TIMESTAMP NOT NULL,\n" +
			"h as a + 2,\n" +
			"PRIMARY KEY (a, g) NOT ENFORCED\n" +
			")\n" +
			"WITH (\n" +
			String.format("'%s'='%s',\n", "connector", "elasticsearch-6") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.INDEX_OPTION.key(), index) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200") +
			String.format("'%s'='%s'\n", ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false") +
			")");

		tableEnvironment.fromValues(
			row(
				1L,
				LocalTime.ofNanoOfDay(12345L * 1_000_000L),
				"ABCDE",
				12.12f,
				(byte) 2,
				LocalDate.ofEpochDay(12345),
				LocalDateTime.parse("2012-12-12T12:12:12"))
		).executeInsert("esTable")
			.getJobClient()
			.get()
			.getJobExecutionResult(this.getClass().getClassLoader())
			.get();

		Client client = elasticsearchResource.getClient();
		Map<String, Object> response = client.get(new GetRequest(index, myType, "1_2012-12-12T12:12:12"))
			.actionGet()
			.getSource();
		Map<Object, Object> expectedMap = new HashMap<>();
		expectedMap.put("a", 1);
		expectedMap.put("b", "00:00:12Z");
		expectedMap.put("c", "ABCDE");
		expectedMap.put("d", 12.12d);
		expectedMap.put("e", 2);
		expectedMap.put("f", "2003-10-20");
		expectedMap.put("g", "2012-12-12T12:12:12Z");
		assertThat(response, equalTo(expectedMap));
	}

	@Test
	public void testReadingDocumentsFromTableApi() throws Exception {
		TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build());

		String index = "table-api";
		String myType = "MyType";
		tableEnvironment.executeSql("CREATE TABLE esTable (" +
			"a BIGINT NOT NULL,\n" +
			"b TIME,\n" +
			"c STRING NOT NULL,\n" +
			"d FLOAT,\n" +
			"e TINYINT NOT NULL,\n" +
			"f DATE,\n" +
			"g TIMESTAMP NOT NULL,\n" +
			"h as a + 2,\n" +
			"PRIMARY KEY (a, g) NOT ENFORCED\n" +
			")\n" +
			"WITH (\n" +
			String.format("'%s'='%s',\n", "connector", "elasticsearch-6") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.INDEX_OPTION.key(), index) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200") +
			String.format("'%s'='%s'\n", ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false") +
			")");

		tableEnvironment.fromValues(
			row(
				1L,
				LocalTime.ofNanoOfDay(12345L * 1_000_000L),
				"ABCDE",
				12.12f,
				(byte) 2,
				LocalDate.ofEpochDay(12345),
				LocalDateTime.parse("2012-12-12T12:12:12")),
			row(
				2L,
				LocalTime.ofNanoOfDay(12346L * 1_000_000L),
				"ABCDF",
				12.13f,
				(byte) 3,
				LocalDate.ofEpochDay(12346),
				LocalDateTime.parse("2012-12-12T12:12:13")),
			row(
				3L,
				LocalTime.ofNanoOfDay(12347L * 1_000_000L),
				"ABCDG",
				12.14f,
				(byte) 4,
				LocalDate.ofEpochDay(12347),
				LocalDateTime.parse("2012-12-12T12:12:14")),
			row(
				4L,
				LocalTime.ofNanoOfDay(12348L * 1_000_000L),
				"ABCDH",
				12.15f,
				(byte) 5,
				LocalDate.ofEpochDay(12348),
				LocalDateTime.parse("2012-12-12T12:12:15")),
			row(
				5L,
				LocalTime.ofNanoOfDay(12349L * 1_000_000L),
				"ABCDI",
				12.16f,
				(byte) 6,
				LocalDate.ofEpochDay(12349),
				LocalDateTime.parse("2012-12-12T12:12:16"))
		).executeInsert("esTable")
			.getJobClient()
			.get()
			.getJobExecutionResult(this.getClass().getClassLoader())
			.get();

//		List<String> expected =
//			Stream.of(
//				"1,2020-01-01T15:35:00.123456,2020-01-01T15:35:00.123456789,15:35,1.175E-37,1.79769E308,100.1234",
//				"2,2020-01-01T15:36:01.123456,2020-01-01T15:36:01.123456789,15:36:01,-1.175E-37,-1.79769E308,101.1234")
//				.sorted().collect(Collectors.toList());
//		assertEquals(expected, result);

		tableEnvironment.executeSql("CREATE TABLE esTableSource (" +
			"a BIGINT NOT NULL,\n" +
			"b TIME,\n" +
			"c STRING NOT NULL,\n" +
			"d FLOAT,\n" +
			"e TINYINT NOT NULL,\n" +
			"f DATE,\n" +
			"g TIMESTAMP NOT NULL,\n" +
			"h as a + 2,\n" +
			"PRIMARY KEY (a, g) NOT ENFORCED\n" +
			")\n" +
			"WITH (\n" +
			String.format("'%s'='%s',\n", "connector", "elasticsearch-6") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.INDEX_OPTION.key(), index) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.SCROLL_MAX_SIZE_OPTION.key(), 10) +
			String.format("'%s'='%s'\n", ElasticsearchOptions.SCROLL_TIMEOUT_OPTION.key(), 1000) +
			")");

		Iterator<Row> collected = tableEnvironment.executeSql("SELECT a, b, c, d, e, f, g, h FROM esTableSource").collect();
		List<String> result = Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		result.forEach(System.out::println);

//		List<String> expected =
//			Stream.of(
//				"1, ",
//				""
//			).sorted().collect(Collectors.toList());
//		assertEquals(expected, result);

//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		Map<String, String> userConfig = new HashMap<>();
//		userConfig.put("cluster.name", CLUSTER_NAME);
////		userConfig.put("transport.tcp.connect_timeout", "100s");
//
//		DataType dataType = ROW(FIELD("data", STRING()));
//		RowType schema = (RowType) dataType.getLogicalType();
//
//		// pass on missing field
//		DeserializationSchema<RowData> deserializationSchema = new JsonRowDataDeserializationSchema(
//			schema, new RowDataTypeInfo(schema), false, false);
//
//		ElasticSearchInputFormatBase inputFormat = createElasticsearchInputFormat(
//			userConfig,
//			(DeserializationSchema<T>) deserializationSchema,
//			null,	//对于datastream api可以不设置要获取的字段
//			"elasticsearch-sink-test-index",
//			"flink-es-test-type",
//			10000,
//			10,
//			null,
//			0
//		);
//		DataStream<RowTypeInfo> dataStream = env.createInput(inputFormat);
//		dataStream.print();
//		env.execute("Elasticsearch Source Test");

	}

	@Test
	public void testWritingDocumentsNoPrimaryKey() throws Exception {
		TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build());

		String index = "no-primary-key";
		String myType = "MyType";
		tableEnvironment.executeSql("CREATE TABLE esTable (" +
			"a BIGINT NOT NULL,\n" +
			"b TIME,\n" +
			"c STRING NOT NULL,\n" +
			"d FLOAT,\n" +
			"e TINYINT NOT NULL,\n" +
			"f DATE,\n" +
			"g TIMESTAMP NOT NULL\n" +
			")\n" +
			"WITH (\n" +
			String.format("'%s'='%s',\n", "connector", "elasticsearch-6") +
			String.format("'%s'='%s',\n", ElasticsearchOptions.INDEX_OPTION.key(), index) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), myType) +
			String.format("'%s'='%s',\n", ElasticsearchOptions.HOSTS_OPTION.key(), "http://127.0.0.1:9200") +
			String.format("'%s'='%s'\n", ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false") +
			")");

		tableEnvironment.fromValues(
			row(
				1L,
				LocalTime.ofNanoOfDay(12345L * 1_000_000L),
				"ABCDE",
				12.12f,
				(byte) 2,
				LocalDate.ofEpochDay(12345),
				LocalDateTime.parse("2012-12-12T12:12:12"))
		).executeInsert("esTable")
			.getJobClient()
			.get()
			.getJobExecutionResult(this.getClass().getClassLoader())
			.get();

		Client client = elasticsearchResource.getClient();

		// search API does not return documents that were not indexed, we might need to query
		// the index a few times
		Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
		SearchHits hits;
		do {
			hits = client.prepareSearch(index)
				.execute()
				.actionGet()
				.getHits();
			if (hits.getTotalHits() < 1) {
				Thread.sleep(200);
			}
		} while (hits.getTotalHits() < 1 && deadline.hasTimeLeft());

		if (hits.getTotalHits() < 1) {
			throw new AssertionError("Could not retrieve results from Elasticsearch.");
		}

		Map<String, Object> result = hits.getAt(0).getSourceAsMap();
		Map<Object, Object> expectedMap = new HashMap<>();
		expectedMap.put("a", 1);
		expectedMap.put("b", "00:00:12Z");
		expectedMap.put("c", "ABCDE");
		expectedMap.put("d", 12.12d);
		expectedMap.put("e", 2);
		expectedMap.put("f", "2003-10-20");
		expectedMap.put("g", "2012-12-12T12:12:12Z");
		assertThat(result, equalTo(expectedMap));
	}

	private static class MockContext implements DynamicTableSink.Context {
		@Override
		public boolean isBounded() {
			return false;
		}

		@Override
		public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
			return null;
		}

		@Override
		public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType consumedDataType) {
			return null;
		}
	}
}
