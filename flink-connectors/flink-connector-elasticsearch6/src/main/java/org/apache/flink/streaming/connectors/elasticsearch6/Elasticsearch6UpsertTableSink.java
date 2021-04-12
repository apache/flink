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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_DELAY;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_ENABLED;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_RETRIES;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_BACKOFF_TYPE;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_INTERVAL;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.BULK_FLUSH_MAX_SIZE;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.DISABLE_FLUSH_ON_CHECKPOINT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.REST_MAX_RETRY_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption.REST_PATH_PREFIX;

/** Version-specific upsert table sink for Elasticsearch 6. */
@Internal
public class Elasticsearch6UpsertTableSink extends ElasticsearchUpsertTableSinkBase {

    @VisibleForTesting
    static final RequestFactory UPDATE_REQUEST_FACTORY = new Elasticsearch6RequestFactory();

    public Elasticsearch6UpsertTableSink(
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
                sinkOptions,
                UPDATE_REQUEST_FACTORY);
    }

    @Override
    protected ElasticsearchUpsertTableSinkBase copy(
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

        return new Elasticsearch6UpsertTableSink(
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
    protected SinkFunction<Tuple2<Boolean, Row>> createSinkFunction(
            List<Host> hosts,
            ActionRequestFailureHandler failureHandler,
            Map<SinkOption, String> sinkOptions,
            ElasticsearchUpsertSinkFunction upsertSinkFunction) {

        final List<HttpHost> httpHosts =
                hosts.stream()
                        .map((host) -> new HttpHost(host.hostname, host.port, host.protocol))
                        .collect(Collectors.toList());

        final ElasticsearchSink.Builder<Tuple2<Boolean, Row>> builder =
                createBuilder(upsertSinkFunction, httpHosts);

        builder.setFailureHandler(failureHandler);

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_ACTIONS))
                .ifPresent(v -> builder.setBulkFlushMaxActions(Integer.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_MAX_SIZE))
                .ifPresent(v -> builder.setBulkFlushMaxSizeMb(MemorySize.parse(v).getMebiBytes()));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_INTERVAL))
                .ifPresent(v -> builder.setBulkFlushInterval(Long.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_ENABLED))
                .ifPresent(v -> builder.setBulkFlushBackoff(Boolean.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_TYPE))
                .ifPresent(
                        v ->
                                builder.setBulkFlushBackoffType(
                                        ElasticsearchSinkBase.FlushBackoffType.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_RETRIES))
                .ifPresent(v -> builder.setBulkFlushBackoffRetries(Integer.valueOf(v)));

        Optional.ofNullable(sinkOptions.get(BULK_FLUSH_BACKOFF_DELAY))
                .ifPresent(v -> builder.setBulkFlushBackoffDelay(Long.valueOf(v)));

        builder.setRestClientFactory(
                new DefaultRestClientFactory(
                        Optional.ofNullable(sinkOptions.get(REST_MAX_RETRY_TIMEOUT))
                                .map(Integer::valueOf)
                                .orElse(null),
                        sinkOptions.get(REST_PATH_PREFIX)));

        final ElasticsearchSink<Tuple2<Boolean, Row>> sink = builder.build();

        Optional.ofNullable(sinkOptions.get(DISABLE_FLUSH_ON_CHECKPOINT))
                .ifPresent(
                        v -> {
                            if (Boolean.valueOf(v)) {
                                sink.disableFlushOnCheckpoint();
                            }
                        });

        return sink;
    }

    @VisibleForTesting
    ElasticsearchSink.Builder<Tuple2<Boolean, Row>> createBuilder(
            ElasticsearchUpsertSinkFunction upsertSinkFunction, List<HttpHost> httpHosts) {
        return new ElasticsearchSink.Builder<>(httpHosts, upsertSinkFunction);
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    /** Serializable {@link RestClientFactory} used by the sink. */
    @VisibleForTesting
    static class DefaultRestClientFactory implements RestClientFactory {

        private Integer maxRetryTimeout;
        private String pathPrefix;

        public DefaultRestClientFactory(
                @Nullable Integer maxRetryTimeout, @Nullable String pathPrefix) {
            this.maxRetryTimeout = maxRetryTimeout;
            this.pathPrefix = pathPrefix;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (maxRetryTimeout != null) {
                restClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeout);
            }
            if (pathPrefix != null) {
                restClientBuilder.setPathPrefix(pathPrefix);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultRestClientFactory that = (DefaultRestClientFactory) o;
            return Objects.equals(maxRetryTimeout, that.maxRetryTimeout)
                    && Objects.equals(pathPrefix, that.pathPrefix);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxRetryTimeout, pathPrefix);
        }
    }

    /**
     * Version-specific creation of {@link org.elasticsearch.action.ActionRequest}s used by the
     * sink.
     */
    private static class Elasticsearch6RequestFactory implements RequestFactory {

        @Override
        public UpdateRequest createUpdateRequest(
                String index,
                String docType,
                String key,
                XContentType contentType,
                byte[] document) {
            return new UpdateRequest(index, docType, key)
                    .doc(document, contentType)
                    .upsert(document, contentType);
        }

        @Override
        public IndexRequest createIndexRequest(
                String index, String docType, XContentType contentType, byte[] document) {
            return new IndexRequest(index, docType).source(document, contentType);
        }

        @Override
        public DeleteRequest createDeleteRequest(String index, String docType, String key) {
            return new DeleteRequest(index, docType, key);
        }
    }
}
