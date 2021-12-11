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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.DockerImageVersions;

import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Tests for {@link ElasticsearchSink}. */
@Testcontainers
class Elasticsearch6SinkITCase extends ElasticsearchSinkBaseITCase {

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            new ElasticsearchContainer(DockerImageName.parse(DockerImageVersions.ELASTICSEARCH_6))
                    .withPassword(ELASTICSEARCH_PASSWORD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Override
    String getElasticsearchHttpHostAddress() {
        return ES_CONTAINER.getHttpHostAddress();
    }

    @Override
    TestClientBase createTestClient(RestHighLevelClient client) {
        return new Elasticsearch6TestClient(client);
    }

    @Override
    Elasticsearch6SinkBuilder<Tuple2<Integer, String>> getSinkBuilder() {
        return new Elasticsearch6SinkBuilder<>();
    }
}
