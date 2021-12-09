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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.util.DockerImageVersions;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.connector.elasticsearch.table.TestContext.context;

/** IT tests for {@link ElasticsearchDynamicSink}. */
@Testcontainers
@Disabled
public class Elasticsearch7DynamicSinkITCase extends ElasticsearchDynamicSinkBaseITCase {

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            new ElasticsearchContainer(DockerImageVersions.ELASTICSEARCH_7);

    @Override
    String getElasticsearchHttpHostAddress() {
        return ES_CONTAINER.getHttpHostAddress();
    }

    @Override
    ElasticsearchDynamicSinkFactoryBase getDynamicSinkFactory() {
        return new Elasticsearch7DynamicSinkFactory();
    }

    @Override
    Map<String, Object> makeGetRequest(RestHighLevelClient client, String index, String id)
            throws IOException {
        return client.get(new GetRequest(index, id), RequestOptions.DEFAULT).getSource();
    }

    @Override
    SearchHits makeSearchRequest(RestHighLevelClient client, String index) throws IOException {
        return client.search(new SearchRequest(index), RequestOptions.DEFAULT).getHits();
    }

    @Override
    long getTotalSearchHits(SearchHits searchHits) {
        return searchHits.getTotalHits().value;
    }

    @Override
    TestContext getPrefilledTestContext(String index) {
        return context()
                .withOption(ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                .withOption(
                        ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                        ES_CONTAINER.getHttpHostAddress());
    }

    @Override
    String getConnectorSql(String index) {
        return String.format("'%s'='%s',\n", "connector", "elasticsearch-7")
                + String.format(
                        "'%s'='%s',\n", ElasticsearchConnectorOptions.INDEX_OPTION.key(), index)
                + String.format(
                        "'%s'='%s'\n",
                        ElasticsearchConnectorOptions.HOSTS_OPTION.key(),
                        ES_CONTAINER.getHttpHostAddress());
    }
}
