/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGenerator;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGeneratorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.TestTableDescriptor;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Version-agnostic test base for {@link ElasticsearchUpsertTableSinkFactoryBase}.
 */
public abstract class ElasticsearchUpsertTableSinkFactoryTestBase extends TestLogger {

	protected static final String HOSTNAME = "host1";
	protected static final int PORT = 1234;
	protected static final String SCHEMA = "https";
	protected static final String INDEX = "MyIndex";
	protected static final String DOC_TYPE = "MyType";
	protected static final String KEY_DELIMITER = "#";
	protected static final String KEY_NULL_LITERAL = "";

	private static final String FIELD_KEY = "key";
	private static final String FIELD_FRUIT_NAME = "fruit_name";
	private static final String FIELD_COUNT = "count";
	private static final String FIELD_TS = "ts";

	@Test
	public void testTableSink() {
		// prepare parameters for Elasticsearch table sink

		final TableSchema schema = createTestSchema();

		final ElasticsearchUpsertTableSinkBase expectedSink = getExpectedTableSink(
			false,
			schema,
			Collections.singletonList(new Host(HOSTNAME, PORT, SCHEMA)),
			INDEX,
			DOC_TYPE,
			KEY_DELIMITER,
			KEY_NULL_LITERAL,
			JsonRowSerializationSchema.builder().withTypeInfo(schema.toRowType()).build(),
			XContentType.JSON,
			new DummyFailureHandler(),
			createTestSinkOptions(),
			IndexGeneratorFactory.createIndexGenerator(INDEX, schema));

		// construct table sink using descriptors and table sink factory
		final Map<String, String> elasticSearchProperties = createElasticSearchProperties();
		final TableSink<?> actualSink = TableFactoryService.find(StreamTableSinkFactory.class, elasticSearchProperties)
			.createStreamTableSink(elasticSearchProperties);

		assertEquals(expectedSink, actualSink);
	}

	@Test
	public void testTableSinkWithLegacyProperties() {
		// prepare parameters for Elasticsearch table sink
		final TableSchema schema = createTestSchema();

		final ElasticsearchUpsertTableSinkBase expectedSink = getExpectedTableSink(
			false,
			schema,
			Collections.singletonList(new Host(HOSTNAME, PORT, SCHEMA)),
			INDEX,
			DOC_TYPE,
			KEY_DELIMITER,
			KEY_NULL_LITERAL,
			JsonRowSerializationSchema.builder().withTypeInfo(schema.toRowType()).build(),
			XContentType.JSON,
			new DummyFailureHandler(),
			createTestSinkOptions(),
			IndexGeneratorFactory.createIndexGenerator(INDEX, schema));

		// construct table sink using descriptors and table sink factory
		final Map<String, String> elasticSearchProperties = createElasticSearchProperties();

		final Map<String, String> legacyPropertiesMap = new HashMap<>();
		legacyPropertiesMap.putAll(elasticSearchProperties);
		// use legacy properties
		legacyPropertiesMap.remove("connector.hosts");

		legacyPropertiesMap.put("connector.hosts.0.hostname", "host1");
		legacyPropertiesMap.put("connector.hosts.0.port", "1234");
		legacyPropertiesMap.put("connector.hosts.0.protocol", "https");

		final TableSink<?> actualSink = TableFactoryService.find(StreamTableSinkFactory.class, legacyPropertiesMap)
			.createStreamTableSink(legacyPropertiesMap);

		assertEquals(expectedSink, actualSink);
	}

	protected TableSchema createTestSchema() {
		return TableSchema.builder()
			.field(FIELD_KEY, DataTypes.BIGINT())
			.field(FIELD_FRUIT_NAME, DataTypes.STRING())
			.field(FIELD_COUNT, DataTypes.DECIMAL(10, 4))
			.field(FIELD_TS, DataTypes.TIMESTAMP(3))
			.build();
	}

	protected Map<SinkOption, String> createTestSinkOptions() {
		final Map<SinkOption, String> sinkOptions = new HashMap<>();
		sinkOptions.put(SinkOption.BULK_FLUSH_BACKOFF_ENABLED, "true");
		sinkOptions.put(SinkOption.BULK_FLUSH_BACKOFF_TYPE, "EXPONENTIAL");
		sinkOptions.put(SinkOption.BULK_FLUSH_BACKOFF_DELAY, "123");
		sinkOptions.put(SinkOption.BULK_FLUSH_BACKOFF_RETRIES, "3");
		sinkOptions.put(SinkOption.BULK_FLUSH_INTERVAL, "100");
		sinkOptions.put(SinkOption.BULK_FLUSH_MAX_ACTIONS, "1000");
		sinkOptions.put(SinkOption.BULK_FLUSH_MAX_SIZE, "1 mb");
		sinkOptions.put(SinkOption.REST_MAX_RETRY_TIMEOUT, "100");
		sinkOptions.put(SinkOption.REST_PATH_PREFIX, "/myapp");
		return sinkOptions;
	}

	protected Map<String, String> createElasticSearchProperties() {
		return new TestTableDescriptor(
			new Elasticsearch()
				.version(getElasticsearchVersion())
				.host(HOSTNAME, PORT, SCHEMA)
				.index(INDEX)
				.documentType(DOC_TYPE)
				.keyDelimiter(KEY_DELIMITER)
				.keyNullLiteral(KEY_NULL_LITERAL)
				.bulkFlushBackoffExponential()
				.bulkFlushBackoffDelay(123L)
				.bulkFlushBackoffMaxRetries(3)
				.bulkFlushInterval(100L)
				.bulkFlushMaxActions(1000)
				.bulkFlushMaxSize("1 MB")
				.failureHandlerCustom(DummyFailureHandler.class)
				.connectionMaxRetryTimeout(100)
				.connectionPathPrefix("/myapp"))
			.withFormat(
				new Json()
					.deriveSchema())
			.withSchema(
				new Schema()
					.field(FIELD_KEY, DataTypes.BIGINT())
					.field(FIELD_FRUIT_NAME, DataTypes.STRING())
					.field(FIELD_COUNT, DataTypes.DECIMAL(10, 4))
					.field(FIELD_TS, DataTypes.TIMESTAMP(3)))
			.inUpsertMode()
			.toProperties();
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific tests
	// --------------------------------------------------------------------------------------------

	protected abstract String getElasticsearchVersion();

	protected abstract ElasticsearchUpsertTableSinkBase getExpectedTableSink(
		boolean isAppendOnly,
		TableSchema schema,
		List<Host> hosts,
		String index,
		String docType,
		String keyDelimiter,
		String keyNullLiteral,
		SerializationSchema<Row> serializationSchema,
		XContentType contentType,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		IndexGenerator indexGenerator);

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Custom failure handler for testing.
	 */
	public static class DummyFailureHandler implements ActionRequestFailureHandler {

		@Override
		public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) {
			// do nothing
		}

		@Override
		public boolean equals(Object o) {
			return this == o || o instanceof DummyFailureHandler;
		}

		@Override
		public int hashCode() {
			return DummyFailureHandler.class.hashCode();
		}
	}
}
