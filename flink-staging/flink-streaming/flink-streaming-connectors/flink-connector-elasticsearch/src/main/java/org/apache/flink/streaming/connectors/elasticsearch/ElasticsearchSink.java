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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


/**
 * Sink that emits its input elements to an Elasticsearch cluster.
 *
 * <p>
 * When using the first constructor {@link #ElasticsearchSink(java.util.Map, IndexRequestBuilder)}
 * the sink will create a local {@link Node} for communicating with the
 * Elasticsearch cluster. When using the second constructor
 * {@link #ElasticsearchSink(java.util.Map, IndexRequestBuilder)} a {@link TransportClient} will
 * be used instead.
 *
 * <p>
 * <b>Attention: </b> When using the {@code TransportClient} the sink will fail if no cluster
 * can be connected to. With the {@code Node Client} the sink will block and wait for a cluster
 * to come online.
 *
 * <p>
 * The {@link Map} passed to the constructor is forwarded to Elasticsearch when creating
 * the {@link Node} or {@link TransportClient}. The config keys can be found in the Elasticsearch
 * documentation. An important setting is {@code cluster.name}, this should be set to the name
 * of the cluster that the sink should emit to.
 *
 * <p>
 * Internally, the sink will use a {@link BulkProcessor} to send {@link IndexRequest IndexRequests}.
 * This will buffer elements before sending a request to the cluster. The behaviour of the
 * {@code BulkProcessor} can be configured using these config keys:
 * <ul>
 *   <li> {@code bulk.flush.max.actions}: Maximum amount of elements to buffer
 *   <li> {@code bulk.flush.max.size.mb}: Maximum amount of data (in megabytes) to buffer
 *   <li> {@code bulk.flush.interval.ms}: Interval at which to flush data regardless of the other two
 *   settings in milliseconds
 * </ul>
 *
 * <p>
 * You also have to provide an {@link IndexRequestBuilder}. This is used to create an
 * {@link IndexRequest} from an element that needs to be added to Elasticsearch. See
 * {@link org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder} for an example.
 *
 * @param <T> Type of the elements emitted by this sink
 */
public class ElasticsearchSink<T> extends RichSinkFunction<T> {

	public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
	public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

	/**
	 * The user specified config map that we forward to Elasticsearch when we create the Client.
	 */
	private final Map<String, String> userConfig;

	/**
	 * The list of nodes that the TransportClient should connect to. This is null if we are using
	 * an embedded Node to get a Client.
	 */
	private final List<TransportAddress> transportNodes;

	/**
	 * The builder that is used to construct an {@link IndexRequest} from the incoming element.
	 */
	private final IndexRequestBuilder<T> indexRequestBuilder;

	/**
	 * The embedded Node that is used to communicate with the Elasticsearch cluster. This is null
	 * if we are using a TransportClient.
	 */
	private transient Node node;

	/**
	 * The Client that was either retrieved from a Node or is a TransportClient.
	 */
	private transient Client client;

	/**
	 * Bulk processor that was created using the client
	 */
	private transient BulkProcessor bulkProcessor;

	/**
	 * This is set from inside the BulkProcessor listener if there where failures in processing.
	 */
	private final AtomicBoolean hasFailure = new AtomicBoolean(false);

	/**
	 * This is set from inside the BulkProcessor listener if a Throwable was thrown during processing.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	/**
	 * Creates a new ElasticsearchSink that connects to the cluster using an embedded Node.
	 *
	 * @param userConfig The map of user settings that are passed when constructing the Node and BulkProcessor
	 * @param indexRequestBuilder This is used to generate the IndexRequest from the incoming element
	 */
	public ElasticsearchSink(Map<String, String> userConfig, IndexRequestBuilder<T> indexRequestBuilder) {
		this.userConfig = userConfig;
		this.indexRequestBuilder = indexRequestBuilder;
		transportNodes = null;
	}

	/**
	 * Creates a new ElasticsearchSink that connects to the cluster using a TransportClient.
	 *
	 * @param userConfig The map of user settings that are passed when constructing the TransportClient and BulkProcessor
	 * @param transportNodes The Elasticsearch Nodes to which to connect using a {@code TransportClient}
	 * @param indexRequestBuilder This is used to generate the IndexRequest from the incoming element
	 *
	 */
	public ElasticsearchSink(Map<String, String> userConfig, List<TransportAddress> transportNodes, IndexRequestBuilder<T> indexRequestBuilder) {
		this.userConfig = userConfig;
		this.indexRequestBuilder = indexRequestBuilder;
		this.transportNodes = transportNodes;
	}

	/**
	 * Initializes the connection to Elasticsearch by either creating an embedded
	 * {@link org.elasticsearch.node.Node} and retrieving the
	 * {@link org.elasticsearch.client.Client} from it or by creating a
	 * {@link org.elasticsearch.client.transport.TransportClient}.
	 */
	@Override
	public void open(Configuration configuration) {
		if (transportNodes == null) {
			// Make sure that we disable http access to our embedded node
			Settings settings =
					ImmutableSettings.settingsBuilder()
							.put(userConfig)
							.put("http.enabled", false)
							.build();

			node =
					nodeBuilder()
							.settings(settings)
							.client(true)
							.data(false)
							.node();

			client = node.client();

			if (LOG.isInfoEnabled()) {
				LOG.info("Created Elasticsearch Client {} from embedded Node", client);
			}

		} else {
			Settings settings = ImmutableSettings.settingsBuilder()
					.put(userConfig)
					.build();

			TransportClient transportClient = new TransportClient(settings);
			for (TransportAddress transport: transportNodes) {
				transportClient.addTransportAddress(transport);
			}

			// verify that we actually are connected to a cluster
			ImmutableList<DiscoveryNode> nodes = transportClient.connectedNodes();
			if (nodes.isEmpty()) {
				throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Connected to nodes: " + nodes.toString());
				}
			}

			client = transportClient;

			if (LOG.isInfoEnabled()) {
				LOG.info("Created Elasticsearch TransportClient {}", client);
			}
		}

		BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
				client,
				new BulkProcessor.Listener() {
					@Override
					public void beforeBulk(long executionId,
							BulkRequest request) {

					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request,
							BulkResponse response) {
						if (response.hasFailures()) {
							for (BulkItemResponse itemResp : response.getItems()) {
								if (itemResp.isFailed()) {
									LOG.error("Failed to index document in Elasticsearch: " + itemResp.getFailureMessage());
									failureThrowable.compareAndSet(null, new RuntimeException(itemResp.getFailureMessage()));
								}
							}
							hasFailure.set(true);
						}
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request,
							Throwable failure) {
						LOG.error(failure.getMessage());
						failureThrowable.compareAndSet(null, failure);
						hasFailure.set(true);
					}
				});

		// This makes flush() blocking
		bulkProcessorBuilder.setConcurrentRequests(0);

		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorBuilder.setBulkActions(params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS));
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(params.getInt(
					CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB), ByteSizeUnit.MB));
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(params.getInt(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)));
		}

		bulkProcessor = bulkProcessorBuilder.build();
	}

	@Override
	public void invoke(T element) {
		IndexRequest indexRequest = indexRequestBuilder.createIndexRequest(element, getRuntimeContext());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Emitting IndexRequest: {}", indexRequest);
		}

		bulkProcessor.add(indexRequest);
	}

	@Override
	public void close() {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			client.close();
		}

		if (node != null) {
			node.close();
		}

		if (hasFailure.get()) {
			Throwable cause = failureThrowable.get();
			if (cause != null) {
				throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
			} else {
				throw new RuntimeException("An error occured in ElasticsearchSink.");

			}
		}
	}

}
