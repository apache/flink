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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.typeutils.TypeCheckUtils;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A version-agnostic Elasticsearch {@link UpsertStreamTableSink}.
 */
@Internal
public abstract class ElasticsearchUpsertTableSinkBase implements UpsertStreamTableSink<Row> {

	/** Flag that indicates that only inserts are accepted. */
	private final boolean isAppendOnly;

	/** Schema of the table. */
	private final TableSchema schema;

	/** Version-agnostic hosts configuration. */
	private final List<Host> hosts;

	/** Default index for all requests. */
	private final String index;

	/** Default document type for all requests. */
	private final String docType;

	/** Delimiter for composite keys. */
	private final String keyDelimiter;

	/** String literal for null keys. */
	private final String keyNullLiteral;

	/** Serialization schema used for the document. */
	private final SerializationSchema<Row> serializationSchema;

	/** Content type describing the serialization schema. */
	private final XContentType contentType;

	/** Failure handler for failing {@link ActionRequest}s. */
	private final ActionRequestFailureHandler failureHandler;

	/**
	 * Map of optional configuration parameters for the Elasticsearch sink. The config is
	 * internal and can change at any time.
	 */
	private final Map<SinkOption, String> sinkOptions;

	/**
	 * Version-agnostic creation of {@link ActionRequest}s.
	 */
	private final RequestFactory requestFactory;

	/** Key field indices determined by the query. */
	private int[] keyFieldIndices = new int[0];

	public ElasticsearchUpsertTableSinkBase(
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
			RequestFactory requestFactory) {

		this.isAppendOnly = isAppendOnly;
		this.schema = Preconditions.checkNotNull(schema);
		this.hosts = Preconditions.checkNotNull(hosts);
		this.index = Preconditions.checkNotNull(index);
		this.keyDelimiter = Preconditions.checkNotNull(keyDelimiter);
		this.keyNullLiteral = Preconditions.checkNotNull(keyNullLiteral);
		this.docType = Preconditions.checkNotNull(docType);
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
		this.contentType = Preconditions.checkNotNull(contentType);
		this.failureHandler = Preconditions.checkNotNull(failureHandler);
		this.sinkOptions = Preconditions.checkNotNull(sinkOptions);
		this.requestFactory = Preconditions.checkNotNull(requestFactory);
	}

	@Override
	public void setKeyFields(String[] keyNames) {
		if (keyNames == null) {
			this.keyFieldIndices = new int[0];
			return;
		}

		final String[] fieldNames = getFieldNames();
		final int[] keyFieldIndices = new int[keyNames.length];
		for (int i = 0; i < keyNames.length; i++) {
			keyFieldIndices[i] = -1;
			for (int j = 0; j < fieldNames.length; j++) {
				if (keyNames[i].equals(fieldNames[j])) {
					keyFieldIndices[i] = j;
					break;
				}
			}
			if (keyFieldIndices[i] == -1) {
				throw new RuntimeException("Invalid key fields: " + Arrays.toString(keyNames));
			}
		}

		validateKeyTypes(keyFieldIndices);

		this.keyFieldIndices = keyFieldIndices;
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		if (this.isAppendOnly && !isAppendOnly) {
			throw new ValidationException(
				"The given query is not supported by this sink because the sink is configured to " +
				"operate in append mode only. Thus, it only support insertions (no queries " +
				"with updating results).");
		}
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		final ElasticsearchUpsertSinkFunction upsertFunction =
			new ElasticsearchUpsertSinkFunction(
				index,
				docType,
				keyDelimiter,
				keyNullLiteral,
				serializationSchema,
				contentType,
				requestFactory,
				keyFieldIndices);
		final SinkFunction<Tuple2<Boolean, Row>> sinkFunction = createSinkFunction(
			hosts,
			failureHandler,
			sinkOptions,
			upsertFunction);
		dataStream.addSink(sinkFunction)
			.name(TableConnectorUtil.generateRuntimeName(this.getClass(), getFieldNames()));
	}

	@Override
	public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return Types.TUPLE(Types.BOOLEAN, getRecordType());
	}

	@Override
	public String[] getFieldNames() {
		return schema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return schema.getFieldTypes();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(getFieldNames(), fieldNames) || !Arrays.equals(getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(getFieldNames()) + " / " + Arrays.toString(getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}
		return copy(
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
			sinkOptions,
			requestFactory);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ElasticsearchUpsertTableSinkBase that = (ElasticsearchUpsertTableSinkBase) o;
		return Objects.equals(isAppendOnly, that.isAppendOnly) &&
			Objects.equals(schema, that.schema) &&
			Objects.equals(hosts, that.hosts) &&
			Objects.equals(index, that.index) &&
			Objects.equals(docType, that.docType) &&
			Objects.equals(keyDelimiter, that.keyDelimiter) &&
			Objects.equals(keyNullLiteral, that.keyNullLiteral) &&
			Objects.equals(serializationSchema, that.serializationSchema) &&
			Objects.equals(contentType, that.contentType) &&
			Objects.equals(failureHandler, that.failureHandler) &&
			Objects.equals(sinkOptions, that.sinkOptions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
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

	// --------------------------------------------------------------------------------------------
	// For version-specific implementations
	// --------------------------------------------------------------------------------------------

	protected abstract ElasticsearchUpsertTableSinkBase copy(
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
		RequestFactory requestFactory);

	protected abstract SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
		List<Host> hosts,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		ElasticsearchUpsertSinkFunction upsertFunction);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Validate the types that are used for conversion to string.
	 */
	private void validateKeyTypes(int[] keyFieldIndices) {
		final TypeInformation<?>[] types = getFieldTypes();
		for (int keyFieldIndex : keyFieldIndices) {
			final TypeInformation<?> type = types[keyFieldIndex];
			if (!TypeCheckUtils.isSimpleStringRepresentation(type)) {
				throw new ValidationException(
					"Only simple types that can be safely converted into a string representation " +
						"can be used as keys. But was: " + type);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	/**
	 * Keys for optional parameterization of the sink.
	 */
	public enum SinkOption {
		DISABLE_FLUSH_ON_CHECKPOINT,
		BULK_FLUSH_MAX_ACTIONS,
		BULK_FLUSH_MAX_SIZE,
		BULK_FLUSH_INTERVAL,
		BULK_FLUSH_BACKOFF_ENABLED,
		BULK_FLUSH_BACKOFF_TYPE,
		BULK_FLUSH_BACKOFF_RETRIES,
		BULK_FLUSH_BACKOFF_DELAY,
		REST_MAX_RETRY_TIMEOUT,
		REST_PATH_PREFIX
	}

	/**
	 * Entity for describing a host of Elasticsearch.
	 */
	public static class Host {
		public final String hostname;
		public final int port;
		public final String protocol;

		public Host(String hostname, int port, String protocol) {
			this.hostname = hostname;
			this.port = port;
			this.protocol = protocol;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Host host = (Host) o;
			return port == host.port &&
				Objects.equals(hostname, host.hostname) &&
				Objects.equals(protocol, host.protocol);
		}

		@Override
		public int hashCode() {
			return Objects.hash(
				hostname,
				port,
				protocol);
		}
	}

	/**
	 * For version-agnostic creating of {@link ActionRequest}s.
	 */
	public interface RequestFactory extends Serializable {

		/**
		 * Creates an update request to be added to a {@link RequestIndexer}.
		 */
		UpdateRequest createUpdateRequest(
			String index,
			String docType,
			String key,
			XContentType contentType,
			byte[] document);

		/**
		 * Creates an index request to be added to a {@link RequestIndexer}.
		 */
		IndexRequest createIndexRequest(
			String index,
			String docType,
			XContentType contentType,
			byte[] document);

		/**
		 * Creates a delete request to be added to a {@link RequestIndexer}.
		 */
		DeleteRequest createDeleteRequest(
			String index,
			String docType,
			String key);
	}

	/**
	 * Sink function for converting upserts into Elasticsearch {@link ActionRequest}s.
	 */
	public static class ElasticsearchUpsertSinkFunction implements ElasticsearchSinkFunction<Tuple2<Boolean, Row>> {

		private final String index;
		private final String docType;
		private final String keyDelimiter;
		private final String keyNullLiteral;
		private final SerializationSchema<Row> serializationSchema;
		private final XContentType contentType;
		private final RequestFactory requestFactory;
		private final int[] keyFieldIndices;

		public ElasticsearchUpsertSinkFunction(
				String index,
				String docType,
				String keyDelimiter,
				String keyNullLiteral,
				SerializationSchema<Row> serializationSchema,
				XContentType contentType,
				RequestFactory requestFactory,
				int[] keyFieldIndices) {

			this.index = Preconditions.checkNotNull(index);
			this.docType = Preconditions.checkNotNull(docType);
			this.keyDelimiter = Preconditions.checkNotNull(keyDelimiter);
			this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
			this.contentType = Preconditions.checkNotNull(contentType);
			this.keyFieldIndices = Preconditions.checkNotNull(keyFieldIndices);
			this.requestFactory = Preconditions.checkNotNull(requestFactory);
			this.keyNullLiteral = Preconditions.checkNotNull(keyNullLiteral);
		}

		@Override
		public void process(Tuple2<Boolean, Row> element, RuntimeContext ctx, RequestIndexer indexer) {
			if (element.f0) {
				processUpsert(element.f1, indexer);
			} else {
				processDelete(element.f1, indexer);
			}
		}

		private void processUpsert(Row row, RequestIndexer indexer) {
			final byte[] document = serializationSchema.serialize(row);
			if (keyFieldIndices.length == 0) {
				final IndexRequest indexRequest = requestFactory.createIndexRequest(
					index,
					docType,
					contentType,
					document);
				indexer.add(indexRequest);
			} else {
				final String key = createKey(row);
				final UpdateRequest updateRequest = requestFactory.createUpdateRequest(
					index,
					docType,
					key,
					contentType,
					document);
				indexer.add(updateRequest);
			}
		}

		private void processDelete(Row row, RequestIndexer indexer) {
			final String key = createKey(row);
			final DeleteRequest deleteRequest = requestFactory.createDeleteRequest(
				index,
				docType,
				key);
			indexer.add(deleteRequest);
		}

		private String createKey(Row row) {
			final StringBuilder builder = new StringBuilder();
			for (int i = 0; i < keyFieldIndices.length; i++) {
				final int keyFieldIndex = keyFieldIndices[i];
				if (i > 0) {
					builder.append(keyDelimiter);
				}
				final Object value = row.getField(keyFieldIndex);
				if (value == null) {
					builder.append(keyNullLiteral);
				} else {
					builder.append(value.toString());
				}
			}
			return builder.toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ElasticsearchUpsertSinkFunction that = (ElasticsearchUpsertSinkFunction) o;
			return Objects.equals(index, that.index) &&
				Objects.equals(docType, that.docType) &&
				Objects.equals(keyDelimiter, that.keyDelimiter) &&
				Objects.equals(keyNullLiteral, that.keyNullLiteral) &&
				Objects.equals(serializationSchema, that.serializationSchema) &&
				contentType == that.contentType &&
				Objects.equals(requestFactory, that.requestFactory) &&
				Arrays.equals(keyFieldIndices, that.keyFieldIndices);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(
				index,
				docType,
				keyDelimiter,
				keyNullLiteral,
				serializationSchema,
				contentType,
				requestFactory);
			result = 31 * result + Arrays.hashCode(keyFieldIndices);
			return result;
		}
	}
}
