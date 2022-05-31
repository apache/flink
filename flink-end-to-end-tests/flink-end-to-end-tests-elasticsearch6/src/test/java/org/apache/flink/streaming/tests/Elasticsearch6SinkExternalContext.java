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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;

import org.apache.http.HttpHost;

import java.net.URL;
import java.util.List;

class Elasticsearch6SinkExternalContext extends ElasticsearchSinkExternalContextBase {

    /**
     * Instantiates a new Elasticsearch 6 sink context base.
     *
     * @param addressExternal The address to access Elasticsearch from the host machine (outside of
     *     the containerized environment).
     * @param addressInternal The address to access Elasticsearch from Flink. When running in a
     *     containerized environment, should correspond to the network alias that resolves within
     *     the environment's network together with the exposed port.
     * @param connectorJarPaths The connector jar paths.
     */
    Elasticsearch6SinkExternalContext(
            String addressExternal, String addressInternal, List<URL> connectorJarPaths) {
        super(addressInternal, connectorJarPaths, new Elasticsearch6Client(addressExternal));
    }

    @Override
    public Sink<KeyValue<Integer, String>> createSink(TestingSinkSettings sinkSettings) {
        client.createIndexIfDoesNotExist(indexName, 1, 0);
        return new Elasticsearch6SinkBuilder<KeyValue<Integer, String>>()
                .setHosts(HttpHost.create(this.addressInternal))
                .setEmitter(new ElasticsearchTestEmitter(new UpdateRequest6Factory(indexName)))
                .setBulkFlushMaxActions(BULK_BUFFER)
                .build();
    }

    @Override
    public ExternalSystemDataReader<KeyValue<Integer, String>> createSinkDataReader(
            TestingSinkSettings sinkSettings) {
        return new ElasticsearchDataReader(client, indexName, PAGE_LENGTH);
    }

    @Override
    public String toString() {
        return "Elasticsearch 6 sink context.";
    }
}
