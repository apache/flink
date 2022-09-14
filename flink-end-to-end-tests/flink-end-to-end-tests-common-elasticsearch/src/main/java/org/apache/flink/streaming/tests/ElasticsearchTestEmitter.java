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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;

import org.elasticsearch.action.update.UpdateRequest;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Test emitter for performing ElasticSearch indexing requests. */
public class ElasticsearchTestEmitter implements ElasticsearchEmitter<KeyValue<Integer, String>> {

    private static final long serialVersionUID = 1L;

    private final UpdateRequestFactory factory;

    /**
     * Instantiates a new Elasticsearch test emitter.
     *
     * @param factory The factory for creating {@link UpdateRequest}s.
     */
    public ElasticsearchTestEmitter(UpdateRequestFactory factory) {
        this.factory = checkNotNull(factory);
    }

    @Override
    public void emit(
            KeyValue<Integer, String> element, SinkWriter.Context context, RequestIndexer indexer) {
        UpdateRequest updateRequest = factory.createUpdateRequest(element);
        indexer.add(updateRequest);
    }
}
