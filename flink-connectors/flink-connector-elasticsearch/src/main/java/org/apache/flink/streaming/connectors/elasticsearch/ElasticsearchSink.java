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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;

import java.util.List;
import java.util.Map;

/**
 * Elasticsearch 1.x sink that requests multiple {@link ActionRequest ActionRequests}
 * against a cluster for each incoming element.
 *
 * <p>When using the first constructor {@link #ElasticsearchSink(java.util.Map, ElasticsearchSinkFunction)}
 * the sink will create a local {@link Node} for communicating with the Elasticsearch cluster. When using the second
 * constructor {@link #ElasticsearchSink(java.util.Map, java.util.List, ElasticsearchSinkFunction)} a
 * {@link TransportClient} will be used instead.
 *
 * <p><b>Attention: </b> When using the {@code TransportClient} the sink will fail if no cluster
 * can be connected to. When using the local {@code Node} for communicating, the sink will block and wait for a cluster
 * to come online.
 *
 * <p>The {@link Map} passed to the constructor is used to create the {@link Node} or {@link TransportClient}. The config
 * keys can be found in the <a href="https://www.elastic.co">Elasticsearch documentation</a>. An important setting is
 * {@code cluster.name}, which should be set to the name of the cluster that the sink should emit to.
 *
 * <p>Internally, the sink will use a {@link BulkProcessor} to send {@link ActionRequest ActionRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>You also have to provide an {@link ElasticsearchSinkFunction}. This is used to create multiple
 * {@link ActionRequest ActionRequests} for each incoming element. See the class level documentation of
 * {@link ElasticsearchSinkFunction} for an example.
 *
 * @param <T> Type of the elements handled by this sink
 */
@PublicEvolving
public class ElasticsearchSink<T> extends ElasticsearchSinkBase<T, Client> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using an embedded {@link Node}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the {@link Node} and {@link BulkProcessor}
	 * @param indexRequestBuilder This is used to generate the IndexRequest from the incoming element
	 *
	 * @deprecated Deprecated since version 1.2, to be removed at version 2.0.
	 *             Please use {@link ElasticsearchSink#ElasticsearchSink(Map, ElasticsearchSinkFunction)} instead.
	 */
	@Deprecated
	public ElasticsearchSink(Map<String, String> userConfig, IndexRequestBuilder<T> indexRequestBuilder) {
		this(userConfig, new IndexRequestBuilderWrapperFunction<>(indexRequestBuilder));
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link TransportClient}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the {@link TransportClient} and {@link BulkProcessor}
	 * @param transportAddresses The addresses of Elasticsearch nodes to which to connect using a {@link TransportClient}
	 * @param indexRequestBuilder This is used to generate a {@link IndexRequest} from the incoming element
	 *
	 * @deprecated Deprecated since 1.2, to be removed at 2.0.
	 *             Please use {@link ElasticsearchSink#ElasticsearchSink(Map, List, ElasticsearchSinkFunction)} instead.
	 */
	@Deprecated
	public ElasticsearchSink(Map<String, String> userConfig, List<TransportAddress> transportAddresses, IndexRequestBuilder<T> indexRequestBuilder) {
		this(userConfig, transportAddresses, new IndexRequestBuilderWrapperFunction<>(indexRequestBuilder));
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using an embedded {@link Node}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the embedded {@link Node} and {@link BulkProcessor}
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element
	 */
	public ElasticsearchSink(Map<String, String> userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		this(userConfig, elasticsearchSinkFunction, new NoOpFailureHandler());
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link TransportClient}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the {@link TransportClient} and {@link BulkProcessor}
	 * @param transportAddresses The addresses of Elasticsearch nodes to which to connect using a {@link TransportClient}
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element
	 */
	public ElasticsearchSink(Map<String, String> userConfig, List<TransportAddress> transportAddresses, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		this(userConfig, transportAddresses, elasticsearchSinkFunction, new NoOpFailureHandler());
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using an embedded {@link Node}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the embedded {@link Node} and {@link BulkProcessor}
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element
	 * @param failureHandler This is used to handle failed {@link ActionRequest}
	 */
	public ElasticsearchSink(
		Map<String, String> userConfig,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		ActionRequestFailureHandler failureHandler) {

		super(new Elasticsearch1ApiCallBridge(), userConfig, elasticsearchSinkFunction, failureHandler);
	}

	/**
	 * Creates a new {@code ElasticsearchSink} that connects to the cluster using a {@link TransportClient}.
	 *
	 * @param userConfig The map of user settings that are used when constructing the {@link TransportClient} and {@link BulkProcessor}
	 * @param transportAddresses The addresses of Elasticsearch nodes to which to connect using a {@link TransportClient}
	 * @param elasticsearchSinkFunction This is used to generate multiple {@link ActionRequest} from the incoming element
	 * @param failureHandler This is used to handle failed {@link ActionRequest}
	 */
	public ElasticsearchSink(
		Map<String, String> userConfig,
		List<TransportAddress> transportAddresses,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		ActionRequestFailureHandler failureHandler) {

		super(new Elasticsearch1ApiCallBridge(transportAddresses), userConfig, elasticsearchSinkFunction, failureHandler);
	}
}
