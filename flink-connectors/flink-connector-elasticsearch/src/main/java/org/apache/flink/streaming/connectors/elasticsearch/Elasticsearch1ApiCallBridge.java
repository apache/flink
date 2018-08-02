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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 1.x.
 */
@Internal
public class Elasticsearch1ApiCallBridge implements ElasticsearchApiCallBridge<Client> {

	private static final long serialVersionUID = -2632363720584123682L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch1ApiCallBridge.class);

	/** User-provided transport addresses. This is null if we are using an embedded {@link Node} for communication. */
	private final List<TransportAddress> transportAddresses;

	/** The embedded {@link Node} used for communication. This is null if we are using a TransportClient. */
	private transient Node node;

	/**
	 * Constructor for use of an embedded {@link Node} for communication with the Elasticsearch cluster.
	 */
	Elasticsearch1ApiCallBridge() {
		this.transportAddresses = null;
	}

	/**
	 * Constructor for use of a {@link TransportClient} for communication with the Elasticsearch cluster.
	 */
	Elasticsearch1ApiCallBridge(List<TransportAddress> transportAddresses) {
		Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
		this.transportAddresses = transportAddresses;
	}

	@Override
	public Client createClient(Map<String, String> clientConfig) {
		if (transportAddresses == null) {

			// Make sure that we disable http access to our embedded node
			Settings settings = settingsBuilder()
				.put(clientConfig)
				.put("http.enabled", false)
				.build();

			node = nodeBuilder()
				.settings(settings)
				.client(true)
				.data(false)
				.node();

			Client client = node.client();

			if (LOG.isInfoEnabled()) {
				LOG.info("Created Elasticsearch client from embedded node");
			}

			return client;
		} else {
			Settings settings = settingsBuilder()
				.put(clientConfig)
				.build();

			TransportClient transportClient = new TransportClient(settings);
			for (TransportAddress transport: transportAddresses) {
				transportClient.addTransportAddress(transport);
			}

			// verify that we actually are connected to a cluster
			if (transportClient.connectedNodes().isEmpty()) {
				throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
			}

			if (LOG.isInfoEnabled()) {
				LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
			}

			return transportClient;
		}
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(Client client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder(client, listener);
	}

	@Override
	public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return new RuntimeException(bulkItemResponse.getFailureMessage());
		}
	}

	@Override
	public void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {
		// Elasticsearch 1.x does not support backoff retries for failed bulk requests
		LOG.warn("Elasticsearch 1.x does not support backoff retries.");
	}

	@Override
	public void cleanup() {
		if (node != null && !node.isClosed()) {
			node.close();
			node = null;
		}
	}
}
