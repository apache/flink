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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.ElasticsearchResource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.elasticsearch.table.TestContext.context;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * IT tests for {@link Elasticsearch6DynamicSink}.
 */
public class Elasticsearch6DynamicSinkITCase {

	@ClassRule
	public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource("es-6-dynamic-sink-tests");

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

		String index = "my-index";
		String myType = "MyType";
		Elasticsearch6DynamicSinkFactory sinkFactory = new Elasticsearch6DynamicSinkFactory();

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
