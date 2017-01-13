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

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchClientFactory;
import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of {@link ElasticsearchClientFactory} for Elasticsearch version 5.x.
 *
 * Flink Elasticsearch Sink for versions 5.x uses a {@link TransportClient} for communication with an Elasticsearch cluster.
 */
class ElasticsearchClientFactoryImpl implements ElasticsearchClientFactory {

	private static final long serialVersionUID = -7185607275081428567L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClientFactoryImpl.class);

	/**
	 * User-provided transport addresses.
	 *
	 * We are using {@link InetSocketAddress} because {@link TransportAddress} is not serializable in Elasticsearch 5.x.
	 */
	private final List<InetSocketAddress> transportAddresses;

	ElasticsearchClientFactoryImpl(List<InetSocketAddress> transportAddresses) {
		Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
		this.transportAddresses = transportAddresses;
	}

	@Override
	public Client create(Map<String, String> clientConfig) {
		Settings settings = Settings.builder().put(clientConfig)
			.put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
			.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
			.build();

		TransportClient transportClient = new PreBuiltTransportClient(settings);
		for (TransportAddress transport : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
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

	@Override
	public void cleanup() {
		// nothing to cleanup
	}
}
