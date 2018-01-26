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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 6.x.
 */
public class Elasticsearch6ApiCallBridge implements ElasticsearchApiCallBridge {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6ApiCallBridge.class);

	/**
	 * User-provided HttpHost list.
	 *
	 */
	private final HttpHost[] clusterHosts;

	Elasticsearch6ApiCallBridge(List<HttpHost> clusterHosts) {
		Preconditions.checkArgument(clusterHosts != null && !clusterHosts.isEmpty());
		this.clusterHosts = clusterHosts.toArray(new HttpHost[0]);
	}

	@Override
	public RestHighLevelClient createClient() {

		RestClientBuilder restClientBuilder = RestClient.builder(clusterHosts);
		RestHighLevelClient esClient = new RestHighLevelClient(restClientBuilder);
		try {
			MainResponse info = esClient.info();
			// verify that we actually are connected to a cluster
			if (info == null) {
				throw new RuntimeException(
						"Elasticsearch client is not connected to any Elasticsearch nodes!");
			}

			if (LOG.isInfoEnabled()) {
				LOG.info("Created Elasticsearch REST client, connection ok to ES cluster {}",
						info.getClusterName());
			}
		} catch (IOException e) {
			LOG.error("Elasticsearch connection error", e);
			throw new RuntimeException("Elasticsearch connection error", e);
		}

		return esClient;
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
	public void configureBulkProcessorBackoff(BulkProcessor.Builder builder,
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
	public void cleanup() {
		// nothing to cleanup
	}

}
