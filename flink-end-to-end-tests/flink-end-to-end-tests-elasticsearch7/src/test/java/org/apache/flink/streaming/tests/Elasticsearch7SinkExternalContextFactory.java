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

import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.net.URL;
import java.util.List;

/** Elasticsearch sink external context factory. */
class Elasticsearch7SinkExternalContextFactory
        extends ElasticsearchSinkExternalContextFactoryBase<Elasticsearch7SinkExternalContext> {

    /**
     * Instantiates a new Elasticsearch 7 sink external context factory.
     *
     * @param elasticsearchContainer The Elasticsearch container.
     * @param connectorJars The connector jars.
     */
    Elasticsearch7SinkExternalContextFactory(
            ElasticsearchContainer elasticsearchContainer, List<URL> connectorJars) {
        super(elasticsearchContainer, connectorJars);
    }

    @Override
    public Elasticsearch7SinkExternalContext createExternalContext(String testName) {
        return new Elasticsearch7SinkExternalContext(
                elasticsearchContainer.getHttpHostAddress(),
                formatInternalAddress(elasticsearchContainer),
                connectorJars);
    }
}
