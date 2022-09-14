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

import org.apache.http.HttpHost;

/** Tests for {@link Elasticsearch6SinkBuilder}. */
class Elasticsearch6SinkBuilderTest
        extends ElasticsearchSinkBuilderBaseTest<Elasticsearch6SinkBuilder<Object>> {

    @Override
    Elasticsearch6SinkBuilder<Object> createEmptyBuilder() {
        return new Elasticsearch6SinkBuilder<>();
    }

    @Override
    Elasticsearch6SinkBuilder<Object> createMinimalBuilder() {
        return new Elasticsearch6SinkBuilder<>()
                .setEmitter((element, indexer, context) -> {})
                .setHosts(new HttpHost("localhost:3000"));
    }
}
