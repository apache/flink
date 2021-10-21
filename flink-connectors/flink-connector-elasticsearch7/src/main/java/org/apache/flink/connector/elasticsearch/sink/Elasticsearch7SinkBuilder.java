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

import org.elasticsearch.client.RequestOptions;

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
 *     .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 */
@PublicEvolving
public class Elasticsearch7SinkBuilder<IN> extends ElasticsearchSinkBuilderBase<IN> {

    public Elasticsearch7SinkBuilder() {}

    @Override
    public BulkRequestConsumerFactory getBulkRequestConsumer() {
        return (client) ->
                (bulkRequest, bulkResponseActionListener) ->
                        client.bulkAsync(
                                bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
    }
}
