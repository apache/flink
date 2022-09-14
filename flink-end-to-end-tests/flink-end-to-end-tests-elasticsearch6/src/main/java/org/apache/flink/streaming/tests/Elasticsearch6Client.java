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

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The type Elasticsearch 6 client. */
public class Elasticsearch6Client implements ElasticsearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6Client.class);

    private final RestHighLevelClient restClient;

    /**
     * Instantiates a new Elasticsearch 6 client.
     *
     * @param addressExternal The address to access Elasticsearch from the host machine (outside of
     *     the containerized environment).
     */
    public Elasticsearch6Client(String addressExternal) {
        checkNotNull(addressExternal);
        HttpHost httpHost = HttpHost.create(addressExternal);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        this.restClient = new RestHighLevelClient(restClientBuilder);
        checkNotNull(restClient);
    }

    @Override
    public void deleteIndex(String indexName) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        try {
            restClient.indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Cannot delete index {}", indexName, e);
        }
        // This is needed to avoid race conditions between tests that reuse the same index
        refreshIndex(indexName);
    }

    @Override
    public void refreshIndex(String indexName) {
        RefreshRequest refresh = new RefreshRequest(indexName);
        refresh.indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        try {
            restClient.indices().refresh(refresh, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Cannot refresh index {}", indexName, e);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                LOG.info("Index {} not found", indexName);
            }
        }
    }

    @Override
    public void createIndexIfDoesNotExist(String indexName, int shards, int replicas) {
        GetIndexRequest request = new GetIndexRequest(indexName);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.settings(
                Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas));
        try {
            boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
            if (!exists) {
                restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            } else {
                LOG.info("Index already exists {}", indexName);
            }
        } catch (IOException e) {
            LOG.error("Cannot create index {}", indexName, e);
        }
    }

    @Override
    public void close() throws Exception {
        restClient.close();
    }

    @Override
    public List<KeyValue<Integer, String>> fetchAll(QueryParams params) {
        try {
            SearchResponse response =
                    restClient.search(
                            new SearchRequest(params.indexName())
                                    .source(
                                            new SearchSourceBuilder()
                                                    .sort(params.sortField(), SortOrder.ASC)
                                                    .from(params.from())
                                                    .size(params.pageLength())
                                                    .trackTotalHits(params.trackTotalHits())),
                            RequestOptions.DEFAULT);
            SearchHit[] searchHits = response.getHits().getHits();
            return Arrays.stream(searchHits)
                    .map(
                            searchHit ->
                                    KeyValue.of(
                                            Integer.valueOf(searchHit.getId()),
                                            searchHit.getSourceAsMap().get("value").toString()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("Fetching records failed", e);
            return Collections.emptyList();
        }
    }
}
