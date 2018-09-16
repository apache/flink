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

package org.apache.flink.streaming.connectors.elasticsearch2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.util.ElasticsearchUtils;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 2.x.
 */
@Internal
public class Elasticsearch2ApiCallBridge implements ElasticsearchApiCallBridge<TransportClient> {

	private static final long serialVersionUID = 2638252694744361079L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch2ApiCallBridge.class);

	/**
	 * User-provided transport addresses.
	 *
	 * <p>We are using {@link InetSocketAddress} because {@link TransportAddress} is not serializable in Elasticsearch 2.x.
	 */
	private final List<InetSocketAddress> transportAddresses;

	Elasticsearch2ApiCallBridge(List<InetSocketAddress> transportAddresses) {
		Preconditions.checkArgument(transportAddresses != null && !transportAddresses.isEmpty());
		this.transportAddresses = transportAddresses;
	}

	@Override
	public TransportClient createClient(Map<String, String> clientConfig) {
		Settings settings = Settings.settingsBuilder().put(clientConfig).build();

		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		for (TransportAddress address : ElasticsearchUtils.convertInetSocketAddresses(transportAddresses)) {
			transportClient.addTransportAddress(address);
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
	public BulkProcessor.Builder createBulkProcessorBuilder(TransportClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder(client, listener);
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
}
