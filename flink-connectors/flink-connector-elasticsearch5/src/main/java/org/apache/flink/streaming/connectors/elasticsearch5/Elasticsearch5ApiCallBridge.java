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

package org.apache.flink.streaming.connectors.elasticsearch5;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchInputSplit;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 5.x.
 */
@Internal
public class Elasticsearch5ApiCallBridge implements ElasticsearchApiCallBridge<TransportClient> {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch5ApiCallBridge.class);

	/**
	 * User-provided transport addresses.
	 *
	 * <p>We are using {@link InetSocketAddress} because {@link TransportAddress} is not serializable in Elasticsearch 5.x.
	 */
	private final List<InetSocketAddress> transportAddresses;

	Elasticsearch5ApiCallBridge(List<InetSocketAddress> transportAddresses) {
		Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
		this.transportAddresses = transportAddresses;
	}

	@Override
	public TransportClient createClient(Map<String, String> clientConfig) {
		Settings settings = Settings.builder()
			.put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
			.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
			.put(clientConfig)
			.build();

		TransportClient transportClient = new PreBuiltTransportClient(settings);
		for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
			transportClient.addTransportAddress(transport);
		}

		return transportClient;
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(TransportClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder(client, listener);
	}

	@Override
	public ElasticsearchInputSplit[] createInputSplitsInternal(String index, String type, TransportClient client, int minNumSplits) {
		//type get from config do later
		ClusterSearchShardsResponse response = client.admin().cluster().searchShards(new ClusterSearchShardsRequest(index)).actionGet();

		DiscoveryNode[] nodes = response.getNodes();
		Map<String, String>  nodeMap = constructNodeId2Hostnames(nodes);

		List<ElasticsearchInputSplit> splits = new ArrayList<>();
		for (ClusterSearchShardsGroup group : response.getGroups()) {
			List<String> locations = new ArrayList<>();
			for (ShardRouting shard: group.getShards()) {
				if (nodeMap.containsKey(shard.currentNodeId())) {
					locations.add(nodeMap.get(shard.currentNodeId()));
				} else {
					LOG.warn("shard " + shard + " is on the node " + shard.currentNodeId() + ", which is not in the discovery node " + Stream.of((String[]) nodeMap.keySet().toArray()).collect(Collectors.joining(",")));
				}
			}

			int id = splits.size();
			ElasticsearchInputSplit split = new ElasticsearchInputSplit(
				id,
				locations.toArray(new String[0]),
				index,
				type,
				group.getShardId().getId()
			);
			splits.add(split);
		}
		return splits.toArray(new ElasticsearchInputSplit[0]);
	}

	private Map<String, String> constructNodeId2Hostnames(DiscoveryNode[] nodes) {
		Map<String, String> nodeMap = new HashMap<>();
		for (DiscoveryNode node: nodes) {
			nodeMap.put(node.getId(), node.getAddress().toString());
		}
		return nodeMap;
	}

	@Override
	public SearchResponse search(TransportClient client, SearchRequest searchRequest) throws IOException {
		return client.search(searchRequest).actionGet();
	}

	@Override
	public SearchResponse scroll(TransportClient client, SearchScrollRequest searchScrollRequest) {
		return client.searchScroll(searchScrollRequest).actionGet();
	}

	@Override
	public void close(TransportClient client) {
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
	public void verifyClientConnection(TransportClient client) {
		// verify that we actually are connected to a cluster
		if (client.connectedNodes().isEmpty()) {
			// close the transportClient here
			IOUtils.closeQuietly(client);

			throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Elasticsearch TransportClient is connected to nodes {}", client.connectedNodes());
		}
	}
}
