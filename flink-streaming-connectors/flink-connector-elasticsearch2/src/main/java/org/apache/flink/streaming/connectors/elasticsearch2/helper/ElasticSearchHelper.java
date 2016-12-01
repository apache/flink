/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flink.streaming.connectors.elasticsearch2.helper;

import com.google.common.collect.ImmutableList;

import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * This class manages the creation of index templates and index mapping on elasticsearch.
 * 
 * <p>
 * Example:
 *
 * <pre>{@code
 *				ElasticSearchHelper esHelper = new ElasticSearchHelper(config, transports);
 *
 *				//Create an Index Template given a name and the json structure
 *				esHelper.initTemplate(templateName, templateRequest);
 * 
 *				//Create an Index Mapping given the Index Name, DocType and the json structure
 *				esHelper.initIndexMapping(indexName, docType, mappingsRequest);
 *
 * }</pre>
 * 
 * <p>
 * The {@link Map} passed to the constructor is forwarded to Elasticsearch when
 * creating {@link TransportClient}. The config keys can be found in the
 * Elasticsearch documentation. An important setting is {@code cluster.name},
 * this should be set to the name of the cluster that the sink should emit to.
 *
 */
public class ElasticSearchHelper {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);

	private Client client;
	
	private final static int DEFAULT_INDEX_SHARDS = 2;
	private final static int DEFAULT_INDEX_REPLICAS = 0;

	/**
	 * Creates a new ElasticSearchHelper that connects to the cluster using a TransportClient.
	 *
	 * @param userConfig The map of user settings that are passed when constructing the TransportClients
	 * @param transportAddresses The Elasticsearch Nodes to which to connect using a {@code TransportClient}
	 */
	public ElasticSearchHelper(Map<String, String> userConfig, List<InetSocketAddress> transportAddresses) {
		client = buildElasticsearchClient(userConfig, transportAddresses);
	}

	/**
	 * Build a TransportClient to connect to the cluster.
	 * 
	 * @param userConfig The map of user settings that are passed when constructing the TransportClients
	 * @param transportAddresses The Elasticsearch Nodes to which to connect using a {@code TransportClient}
	 * @return Initialized TransportClient
	 */
	public static Client buildElasticsearchClient(Map<String, String> userConfig,
			List<InetSocketAddress> transportAddresses) {
		List<TransportAddress> transportNodes;
		transportNodes = new ArrayList<>(transportAddresses.size());
		for (InetSocketAddress address : transportAddresses) {
			transportNodes.add(new InetSocketTransportAddress(address));
		}

		Settings settings = Settings.settingsBuilder().put(userConfig).build();

		TransportClient transportClient = TransportClient.builder().settings(settings).build();
		for (TransportAddress transport : transportNodes) {
			transportClient.addTransportAddress(transport);
		}

		// verify that we actually are connected to a cluster
		ImmutableList<DiscoveryNode> nodes = ImmutableList.copyOf(transportClient.connectedNodes());
		if (nodes.isEmpty()) {
			throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
		}
		return transportClient;
	}

	/**
	 * Create a new index template.
	 * 
	 * @param templateName Name of the template to create
	 * @param templateReq Json defining the index template
	 */
	public void initTemplate(String templateName, String templateReq) throws Exception {
		// Check if the template is set
		if (templateReq != null && !templateReq.equals("")) {
			// Json deserialization
			PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName).source(templateReq);
			// Sending the request to elastic search
			sendIndexTemplateRequest(request);
		}
	}

	/**
	 * Send the index template request to elasticsearch.
	 * 
	 * @param indexTemplateRequest A valid index template request
	 */
	public void sendIndexTemplateRequest(PutIndexTemplateRequest indexTemplateRequest) throws Exception {
		// Check if the template is set
		if (indexTemplateRequest != null) {
			// Sending the request to elastic search
			client.admin().indices().putTemplate(indexTemplateRequest).get();
		}
	}

	/**
	 * Create a new mapping for a document type for an index.
	 * 
	 * @param indexName Index name where add the mapping
	 * @param docType Document type of the mapping
	 * @param mappingReq Json defining the index mapping
	 */
	public void initIndexMapping(String indexName, String docType, String mappingReq) throws Exception {
		PutMappingRequest request=new PutMappingRequest(indexName).source(mappingReq).type(docType);
		
		// Put the mapping to the index
		sendIndexMappingRequest(request);
		LOG.debug("Updating mappings...");
	}
	
	/**
	 * Send the index mapping request to elasticsearch.
	 * 
	 * @param mappingRequest A valid index mapping request
	 */
	public void sendIndexMappingRequest(PutMappingRequest mappingRequest) throws Exception {
		// Check if the template is set
		if (mappingRequest != null) {
			try {
				// Check if the index exists
				SearchResponse response = client.prepareSearch(mappingRequest.indices())
						.setTypes(mappingRequest.type()).get();
				if (response != null) {
					LOG.debug("Index found, no need to create it...");
				}
			} catch (IndexNotFoundException infe) {
				for (String indexName:mappingRequest.indices()){
					// If the index does not exist, create it
					client.admin().indices().prepareCreate(indexName)
							.setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, DEFAULT_INDEX_SHARDS)
							.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, DEFAULT_INDEX_REPLICAS)).execute().actionGet();
					LOG.info("Index "+indexName+" not found, creating it...");
				}
			}
			// Sending the request to elastic search
			client.admin().indices().putMapping(mappingRequest).get();
		}
	}

}
