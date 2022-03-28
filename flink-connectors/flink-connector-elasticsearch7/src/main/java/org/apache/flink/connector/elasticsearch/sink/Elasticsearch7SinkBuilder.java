/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Builder to construct an Elasticsearch 7 compatible {@link ElasticsearchSink}.
 *
 * <p>The following example shows the minimal setup to create a ElasticsearchSink that submits
 * actions on checkpoint or the default number of actions was buffered (1000).
 *
 * <pre>{@code
 * ElasticsearchSink<String> sink = new Elasticsearch7SinkBuilder<String>()
 *     .setHosts(new HttpHost("localhost:9200")
 *     .setEmitter((element, context, indexer) -> {
 *          indexer.add(
 *              new IndexRequest("my-index")
 *              .id(element.f0.toString())
 *              .source(element.f1)
 *          );
 *      })
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 */
@PublicEvolving
public class Elasticsearch7SinkBuilder<IN>
        extends ElasticsearchSinkBuilderBase<IN, Elasticsearch7SinkBuilder<IN>> {

    public Elasticsearch7SinkBuilder() {}

    @Override
    public <T extends IN> Elasticsearch7SinkBuilder<T> setEmitter(
            ElasticsearchEmitter<? super T> emitter) {
        super.<T>setEmitter(emitter);
        return self();
    }

    @Override
    protected BulkProcessorBuilderFactory getBulkProcessorBuilderFactory() {
        return new BulkProcessorBuilderFactory() {
            @Override
            public BulkProcessor.Builder apply(
                    RestHighLevelClient client,
                    BulkProcessorConfig bulkProcessorConfig,
                    BulkProcessor.Listener listener) {
                BulkProcessor.Builder builder =
                        BulkProcessor.builder(
                                new BulkRequestConsumerFactory() { // This cannot be inlined as a
                                    // lambda because then
                                    // deserialization fails
                                    @Override
                                    public void accept(
                                            BulkRequest bulkRequest,
                                            ActionListener<BulkResponse>
                                                    bulkResponseActionListener) {
                                        client.bulkAsync(
                                                bulkRequest,
                                                RequestOptions.DEFAULT,
                                                bulkResponseActionListener);
                                    }
                                },
                                listener);

                if (bulkProcessorConfig.getBulkFlushMaxActions() != -1) {
                    builder.setBulkActions(bulkProcessorConfig.getBulkFlushMaxActions());
                }

                if (bulkProcessorConfig.getBulkFlushMaxMb() != -1) {
                    builder.setBulkSize(
                            new ByteSizeValue(
                                    bulkProcessorConfig.getBulkFlushMaxMb(), ByteSizeUnit.MB));
                }

                if (bulkProcessorConfig.getBulkFlushInterval() != -1) {
                    builder.setFlushInterval(
                            new TimeValue(bulkProcessorConfig.getBulkFlushInterval()));
                }

                BackoffPolicy backoffPolicy;
                final TimeValue backoffDelay =
                        new TimeValue(bulkProcessorConfig.getBulkFlushBackOffDelay());
                final int maxRetryCount = bulkProcessorConfig.getBulkFlushBackoffRetries();
                switch (bulkProcessorConfig.getFlushBackoffType()) {
                    case CONSTANT:
                        backoffPolicy = BackoffPolicy.constantBackoff(backoffDelay, maxRetryCount);
                        break;
                    case EXPONENTIAL:
                        backoffPolicy =
                                BackoffPolicy.exponentialBackoff(backoffDelay, maxRetryCount);
                        break;
                    case NONE:
                        backoffPolicy = BackoffPolicy.noBackoff();
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Received unknown backoff policy type "
                                        + bulkProcessorConfig.getFlushBackoffType());
                }
                builder.setBackoffPolicy(backoffPolicy);
                return builder;
            }
        };
    }
}
