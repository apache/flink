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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchInputSplit;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 7 and later versions.
 */
@Internal
public class Elasticsearch7ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7ApiCallBridge.class);

	/**
	 * User-provided HTTP Host.
	 */
	private final List<HttpHost> httpHosts;

	/**
	 * The factory to configure the rest client.
	 */
	private final RestClientFactory restClientFactory;

	private final ObjectMapper jsonParser = new ObjectMapper();

	Elasticsearch7ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
		Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());
		this.httpHosts = httpHosts;
		this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
	}

	@Override
	public RestHighLevelClient createClient(Map<String, String> clientConfig) {
		RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
		restClientFactory.configureRestClientBuilder(builder);

		RestHighLevelClient rhlClient = new RestHighLevelClient(builder);

		return rhlClient;
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
	}

	@Override
	public ElasticsearchInputSplit[] createInputSplitsInternal(String index, String type, RestHighLevelClient client, int minNumSplits) {
		Map<String, String> nodeMap = constructNodeId2Hostnames(new Request("GET", "/_nodes"), client);

		List<ElasticsearchInputSplit> splits = new ArrayList<>();
		JsonNode jsonNode = null;
		try {
			Request request = new Request("GET", "/" + index + "/_search_shards");
			String entity = executeRest(request, client);

			jsonNode = jsonParser.readTree(entity);
			jsonNode.get("shards").elements()
				.forEachRemaining(shardReplicasJsonArray -> {

					List<String> locations = new ArrayList<>();
					AtomicInteger shardId = new AtomicInteger(Integer.MIN_VALUE);

					shardReplicasJsonArray.elements().forEachRemaining(shardJsonObject -> {
						shardId.set(shardJsonObject.get("shard").asInt());
						String nodeId = shardJsonObject.get("node").asText();

						if (nodeMap.containsKey(nodeId)) {
							locations.add(nodeMap.get(nodeId));
						} else {
							LOG.warn("shard " + shardId + " is on the node " + nodeId + ", which is not in the discovery node " + Stream.of((String[]) nodeMap.keySet().toArray()).collect(Collectors.joining(",")));
						}
					});

					int id = splits.size();
					ElasticsearchInputSplit split = new ElasticsearchInputSplit(
						id,
						locations.toArray(new String[0]),
						index,
						type,
						shardId.get()
					);
					splits.add(split);
				});
		} catch (IOException e) {
			LOG.info("Get split failed: {}", e.getMessage());
		}
		return splits.toArray(new ElasticsearchInputSplit[0]);
	}

	private String executeRest(Request request, RestHighLevelClient client) throws IOException {
		Response response = client.getLowLevelClient().performRequest(request);
		return EntityUtils.toString(response.getEntity());
	}

	private Map<String, String> constructNodeId2Hostnames(Request request, RestHighLevelClient client) {
		JsonNode jsonNode = null;
		Map<String, String> nodeMap = new HashMap<>();
		try {
			String entity = executeRest(request, client);
			jsonNode = jsonParser.readTree(entity);
			jsonNode.get("nodes").fields()
				.forEachRemaining(nodeJsonField -> {
					nodeMap.put(nodeJsonField.getKey(), nodeJsonField.getValue().get("transport_address").asText());
				});
		} catch (IOException e) {
			LOG.info("Get nodes failed: {}", e.getMessage());
		}
		return nodeMap;
	}

	@Override
	public SearchResponse search(RestHighLevelClient client, SearchRequest searchRequest) throws IOException {
		return client.search(searchRequest, RequestOptions.DEFAULT);
	}

	@Override
	public SearchResponse scroll(RestHighLevelClient client, SearchScrollRequest searchScrollRequest) throws IOException {
		return client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
	}

	@Override
	public void close(RestHighLevelClient client) throws IOException {
		client.close();
	}

	@Override
	public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return bulkItemResponse.getFailure().getCause();
		}
	}

	@Override
	public void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

		BackoffPolicy backoffPolicy;
		if (flushBackoffPolicy != null) {
			switch (flushBackoffPolicy.getBackoffType()) {
				case CONSTANT:
					backoffPolicy = BackoffPolicy.constantBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
					break;
				case EXPONENTIAL:
				default:
					backoffPolicy = BackoffPolicy.exponentialBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
			}
		} else {
			backoffPolicy = BackoffPolicy.noBackoff();
		}

		builder.setBackoffPolicy(backoffPolicy);
	}

	@Override
	public RequestIndexer createBulkProcessorIndexer(
			BulkProcessor bulkProcessor,
			boolean flushOnCheckpoint,
			AtomicLong numPendingRequestsRef) {
		return new Elasticsearch7BulkProcessorIndexer(
			bulkProcessor,
			flushOnCheckpoint,
			numPendingRequestsRef);
	}

	@Override
	public void verifyClientConnection(RestHighLevelClient client) throws IOException {
		if (LOG.isInfoEnabled()) {
			LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHosts);
		}

		if (!client.ping(RequestOptions.DEFAULT)) {
			throw new RuntimeException("There are no reachable Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Elasticsearch RestHighLevelClient is connected to {}", httpHosts.toString());
		}
	}
}
