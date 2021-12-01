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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.elasticsearch.common.ElasticsearchUtil;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** This class reads all available {@link SearchHit}s from a {@link ElasticsearchSplit}. */
@Internal
class ElasticsearchSearchReader implements Closeable {

    private final ElasticsearchSourceConfiguration sourceConfiguration;
    private final NetworkClientConfig networkClientConfig;
    private final ElasticsearchSplit split;

    private final RestHighLevelClient client;

    private Object[] searchAfterSortValues;

    static ElasticsearchSearchReader createReader(
            ElasticsearchSourceConfiguration config,
            NetworkClientConfig networkClientConfig,
            ElasticsearchSplit split) {
        return new ElasticsearchSearchReader(config, networkClientConfig, split);
    }

    ElasticsearchSearchReader(
            ElasticsearchSourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig,
            ElasticsearchSplit split) {
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
        this.split = split;
        this.client = createRestClient();
    }

    private RestHighLevelClient createRestClient() {
        return new RestHighLevelClient(
                ElasticsearchUtil.configureRestClientBuilder(
                        RestClient.builder(sourceConfiguration.getHosts().toArray(new HttpHost[0])),
                        networkClientConfig));
    }

    // returns null when finished
    Collection<SearchHit> readNextSearchHits() throws IOException {
        SearchResponse response = makeSearchRequest(split.getPitId(), searchAfterSortValues);
        SearchHit[] searchHits = response.getHits().getHits();

        if (searchHits == null || searchHits.length == 0) {
            return null;
        }

        SearchHit lastHit = searchHits[searchHits.length - 1];
        searchAfterSortValues =
                lastHit.getSortValues(); // last element contains sortValues for next request

        return Arrays.asList(searchHits);
    }

    private SearchResponse makeSearchRequest(String pitId, @Nullable Object[] searchAfterSortValues)
            throws IOException {
        final PointInTimeBuilder pitBuilder =
                new PointInTimeBuilder(pitId)
                        .setKeepAlive(
                                TimeValue.timeValueMillis(
                                        sourceConfiguration.getPitKeepAlive().toMillis()));

        final SliceBuilder sliceBuilder =
                new SliceBuilder(split.getSliceId(), sourceConfiguration.getNumberOfSlices());

        final FieldSortBuilder sortBuilder = SortBuilders.pitTiebreaker();

        final SearchSourceBuilder searchSourceBuilder =
                new SearchSourceBuilder()
                        .trackTotalHits(false) // disable track_total_hits to speed up pagination
                        .pointInTimeBuilder(pitBuilder)
                        .slice(sliceBuilder)
                        .sort(sortBuilder);

        if (searchAfterSortValues != null) {
            searchSourceBuilder.searchAfter(searchAfterSortValues);
        }

        final SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
