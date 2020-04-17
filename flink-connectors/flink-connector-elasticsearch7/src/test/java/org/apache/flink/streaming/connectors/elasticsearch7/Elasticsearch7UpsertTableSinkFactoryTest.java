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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.ElasticsearchUpsertSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkFactoryTestBase;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGenerator;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGeneratorFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_VERSION_VALUE_7;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link Elasticsearch7UpsertTableSink} created by {@link Elasticsearch7UpsertTableSinkFactory}.
 */
public class Elasticsearch7UpsertTableSinkFactoryTest extends ElasticsearchUpsertTableSinkFactoryTestBase {

	@Test
	public void testBuilder() {
		final TableSchema schema = createTestSchema();
		final IndexGenerator indexGenerator = IndexGeneratorFactory.createIndexGenerator(INDEX, schema);

		final TestElasticsearch7UpsertTableSink testSink = new TestElasticsearch7UpsertTableSink(
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
			createTestSinkOptions());

		final DataStreamMock dataStreamMock = new DataStreamMock(
				new StreamExecutionEnvironmentMock(),
				Types.TUPLE(Types.BOOLEAN, schema.toRowType()));

		testSink.consumeDataStream(dataStreamMock);

		final ElasticsearchSink.Builder<Tuple2<Boolean, Row>> expectedBuilder = new ElasticsearchSink.Builder<>(
			Collections.singletonList(new HttpHost(HOSTNAME, PORT, SCHEMA)),
			new ElasticsearchUpsertSinkFunction(
				indexGenerator,
				DOC_TYPE,
				KEY_DELIMITER,
				KEY_NULL_LITERAL,
				JsonRowSerializationSchema.builder().withTypeInfo(schema.toRowType()).build(),
				XContentType.JSON,
				Elasticsearch7UpsertTableSink.UPDATE_REQUEST_FACTORY,
				new int[0]));
		expectedBuilder.setFailureHandler(new DummyFailureHandler());
		expectedBuilder.setBulkFlushBackoff(true);
		expectedBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
		expectedBuilder.setBulkFlushBackoffDelay(123);
		expectedBuilder.setBulkFlushBackoffRetries(3);
		expectedBuilder.setBulkFlushInterval(100);
		expectedBuilder.setBulkFlushMaxActions(1000);
		expectedBuilder.setBulkFlushMaxSizeMb(1);
		expectedBuilder.setRestClientFactory(new Elasticsearch7UpsertTableSink.DefaultRestClientFactory("/myapp"));
		assertEquals(expectedBuilder, testSink.builder);
	}

	@Override
	protected String getElasticsearchVersion() {
		return CONNECTOR_VERSION_VALUE_7;
	}

	@Override
	protected ElasticsearchUpsertTableSinkBase getExpectedTableSink(
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
			IndexGenerator indexGenerator) {
		return new Elasticsearch7UpsertTableSink(
			isAppendOnly,
			schema,
			hosts,
			index,
			keyDelimiter,
			keyNullLiteral,
			serializationSchema,
			contentType,
			failureHandler,
			sinkOptions);
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	private static class TestElasticsearch7UpsertTableSink extends Elasticsearch7UpsertTableSink {

		public ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder;

		public TestElasticsearch7UpsertTableSink(
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
				Map<SinkOption, String> sinkOptions) {

			super(
				isAppendOnly,
				schema,
				hosts,
				index,
				docType,
				keyDelimiter,
				keyNullLiteral,
				serializationSchema,
				contentType,
				failureHandler,
				sinkOptions);
		}

		@Override
		protected ElasticsearchSink.Builder<Tuple2<Boolean, Row>> createBuilder(
				ElasticsearchUpsertSinkFunction upsertSinkFunction,
				List<HttpHost> httpHosts) {
			builder = super.createBuilder(upsertSinkFunction, httpHosts);
			return builder;
		}
	}

	private static class StreamExecutionEnvironmentMock extends StreamExecutionEnvironment {

		@Override
		public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
			throw new UnsupportedOperationException();
		}
	}

	private static class DataStreamMock extends DataStream<Tuple2<Boolean, Row>> {

		public SinkFunction<?> sinkFunction;

		public DataStreamMock(StreamExecutionEnvironment environment, TypeInformation<Tuple2<Boolean, Row>> outType) {
			super(environment, new TransformationMock("name", outType, 1));
		}

		@Override
		public DataStreamSink<Tuple2<Boolean, Row>> addSink(SinkFunction<Tuple2<Boolean, Row>> sinkFunction) {
			this.sinkFunction = sinkFunction;
			return super.addSink(sinkFunction);
		}
	}

	private static class TransformationMock extends Transformation<Tuple2<Boolean, Row>> {

		public TransformationMock(String name, TypeInformation<Tuple2<Boolean, Row>> outputType, int parallelism) {
			super(name, outputType, parallelism);
		}

		@Override
		public Collection<Transformation<?>> getTransitivePredecessors() {
			return null;
		}
	}
}
