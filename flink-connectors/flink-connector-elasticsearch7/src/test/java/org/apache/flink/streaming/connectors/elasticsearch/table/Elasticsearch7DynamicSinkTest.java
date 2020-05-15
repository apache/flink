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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.SinkFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link Elasticsearch7DynamicSink} parameters.
 */
public class Elasticsearch7DynamicSinkTest {

	private static final String FIELD_KEY = "key";
	private static final String FIELD_FRUIT_NAME = "fruit_name";
	private static final String FIELD_COUNT = "count";
	private static final String FIELD_TS = "ts";

	private static final String HOSTNAME = "host1";
	private static final int PORT = 1234;
	private static final String SCHEMA = "https";
	private static final String INDEX = "MyIndex";
	private static final String DOC_TYPE = "MyType";

	@Test
	public void testBuilder() {
		final TableSchema schema = createTestSchema();

		BuilderProvider provider = new BuilderProvider();
		final Elasticsearch7DynamicSink testSink = new Elasticsearch7DynamicSink(
			new DummySinkFormat(),
			new Elasticsearch7Configuration(getConfig(), this.getClass().getClassLoader()),
			schema,
			provider
		);

		testSink.getSinkRuntimeProvider(new MockSinkContext()).createSinkFunction();

		verify(provider.builderSpy).setFailureHandler(new DummyFailureHandler());
		verify(provider.builderSpy).setBulkFlushBackoff(true);
		verify(provider.builderSpy).setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
		verify(provider.builderSpy).setBulkFlushBackoffDelay(123);
		verify(provider.builderSpy).setBulkFlushBackoffRetries(3);
		verify(provider.builderSpy).setBulkFlushInterval(100);
		verify(provider.builderSpy).setBulkFlushMaxActions(1000);
		verify(provider.builderSpy).setBulkFlushMaxSizeMb(1);
		verify(provider.builderSpy).setRestClientFactory(new Elasticsearch7DynamicSink.DefaultRestClientFactory("/myapp"));
		verify(provider.sinkSpy).disableFlushOnCheckpoint();
	}

	private Configuration getConfig() {
		Configuration configuration = new Configuration();
		configuration.setString(ElasticsearchOptions.INDEX_OPTION.key(), INDEX);
		configuration.setString(ElasticsearchOptions.DOCUMENT_TYPE_OPTION.key(), DOC_TYPE);
		configuration.setString(ElasticsearchOptions.HOSTS_OPTION.key(), SCHEMA + "://" + HOSTNAME + ":" + PORT);
		configuration.setString(ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION.key(), "exponential");
		configuration.setString(ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION.key(), "123");
		configuration.setString(ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(), "3");
		configuration.setString(ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION.key(), "100");
		configuration.setString(ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(), "1000");
		configuration.setString(ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION.key(), "1mb");
		configuration.setString(ElasticsearchOptions.CONNECTION_PATH_PREFIX.key(), "/myapp");
		configuration.setString(ElasticsearchOptions.FAILURE_HANDLER_OPTION.key(), DummyFailureHandler.class.getName());
		configuration.setString(ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION.key(), "false");
		return configuration;
	}

	private static class BuilderProvider implements Elasticsearch7DynamicSink.ElasticSearchBuilderProvider {
		public ElasticsearchSink.Builder<RowData> builderSpy;
		public ElasticsearchSink<RowData> sinkSpy;

		@Override
		public ElasticsearchSink.Builder<RowData> createBuilder(
				List<HttpHost> httpHosts,
				RowElasticsearchSinkFunction upsertSinkFunction) {
			builderSpy = Mockito.spy(new ElasticsearchSink.Builder<>(httpHosts, upsertSinkFunction));
			doAnswer(
				invocation -> {
					sinkSpy = Mockito.spy((ElasticsearchSink<RowData>) invocation.callRealMethod());
					return sinkSpy;
				}
			).when(builderSpy).build();

			return builderSpy;
		}
	}

	private TableSchema createTestSchema() {
		return TableSchema.builder()
			.field(FIELD_KEY, DataTypes.BIGINT())
			.field(FIELD_FRUIT_NAME, DataTypes.STRING())
			.field(FIELD_COUNT, DataTypes.DECIMAL(10, 4))
			.field(FIELD_TS, DataTypes.TIMESTAMP(3))
			.build();
	}

	private static class DummySerializationSchema implements SerializationSchema<RowData> {

		private static final DummySerializationSchema INSTANCE = new DummySerializationSchema();

		@Override
		public byte[] serialize(RowData element) {
			return new byte[0];
		}
	}

	private static class DummySinkFormat implements SinkFormat<SerializationSchema<RowData>> {
		@Override
		public SerializationSchema<RowData> createSinkFormat(
				DynamicTableSink.Context context,
				DataType consumedDataType) {
			return DummySerializationSchema.INSTANCE;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return null;
		}
	}

	private static class MockSinkContext implements DynamicTableSink.Context {
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
			return o instanceof DummyFailureHandler;
		}

		@Override
		public int hashCode() {
			return DummyFailureHandler.class.hashCode();
		}
	}
}
