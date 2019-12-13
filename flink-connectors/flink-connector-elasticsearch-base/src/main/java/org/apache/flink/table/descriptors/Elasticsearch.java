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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_DELAY;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_MAX_SIZE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_PATH_PREFIX;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_DOCUMENT_TYPE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_CLASS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FLUSH_ON_CHECKPOINT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_INDEX;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_KEY_DELIMITER;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_KEY_NULL_LITERAL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_TYPE_VALUE_ELASTICSEARCH;

/**
 * Connector descriptor for the Elasticsearch search engine.
 */
@PublicEvolving
public class Elasticsearch extends ConnectorDescriptor {

	private DescriptorProperties internalProperties = new DescriptorProperties(true);
	private List<Host> hosts = new ArrayList<>();

	/**
	 * Connector descriptor for the Elasticsearch search engine.
	 */
	public Elasticsearch() {
		super(CONNECTOR_TYPE_VALUE_ELASTICSEARCH, 1, true);
	}

	/**
	 * Sets the Elasticsearch version to be used. Required.
	 *
	 * @param version Elasticsearch version. E.g., "6".
	 */
	public Elasticsearch version(String version) {
		internalProperties.putString(CONNECTOR_VERSION, version);
		return this;
	}

	/**
	 * Adds an Elasticsearch host to connect to. Required.
	 *
	 * <p>Multiple hosts can be declared by calling this method multiple times.
	 *
	 * @param hostname connection hostname
	 * @param port connection port
	 * @param protocol connection protocol; e.g. "http"
	 */
	public Elasticsearch host(String hostname, int port, String protocol) {
		final Host host =
			new Host(
				Preconditions.checkNotNull(hostname),
				port,
				Preconditions.checkNotNull(protocol));
		hosts.add(host);
		return this;
	}

	/**
	 * Declares the Elasticsearch index for every record. Required.
	 *
	 * @param index Elasticsearch index
	 */
	public Elasticsearch index(String index) {
		internalProperties.putString(CONNECTOR_INDEX, index);
		return this;
	}

	/**
	 * Declares the Elasticsearch document type for every record. Required.
	 *
	 * @param documentType Elasticsearch document type
	 */
	public Elasticsearch documentType(String documentType) {
		internalProperties.putString(CONNECTOR_DOCUMENT_TYPE, documentType);
		return this;
	}

	/**
	 * Sets a custom key delimiter in case the Elasticsearch ID needs to be constructed from
	 * multiple fields. Optional.
	 *
	 * @param keyDelimiter key delimiter; e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
	 */
	public Elasticsearch keyDelimiter(String keyDelimiter) {
		internalProperties.putString(CONNECTOR_KEY_DELIMITER, keyDelimiter);
		return this;
	}

	/**
	 * Sets a custom representation for null fields in keys. Optional.
	 *
	 * @param keyNullLiteral key null literal string; e.g. "N/A" would result in IDs "KEY1_N/A_KEY3"
	 */
	public Elasticsearch keyNullLiteral(String keyNullLiteral) {
		internalProperties.putString(CONNECTOR_KEY_NULL_LITERAL, keyNullLiteral);
		return this;
	}

	/**
	 * Configures a failure handling strategy in case a request to Elasticsearch fails.
	 *
	 * <p>This strategy throws an exception if a request fails and thus causes a job failure.
	 */
	public Elasticsearch failureHandlerFail() {
		internalProperties.putString(CONNECTOR_FAILURE_HANDLER, ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_FAIL);
		return this;
	}

	/**
	 * Configures a failure handling strategy in case a request to Elasticsearch fails.
	 *
	 * <p>This strategy ignores failures and drops the request.
	 */
	public Elasticsearch failureHandlerIgnore() {
		internalProperties.putString(CONNECTOR_FAILURE_HANDLER, ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE);
		return this;
	}

	/**
	 * Configures a failure handling strategy in case a request to Elasticsearch fails.
	 *
	 * <p>This strategy re-adds requests that have failed due to queue capacity saturation.
	 */
	public Elasticsearch failureHandlerRetryRejected() {
		internalProperties.putString(CONNECTOR_FAILURE_HANDLER, ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_RETRY);
		return this;
	}

	/**
	 * Configures a failure handling strategy in case a request to Elasticsearch fails.
	 *
	 * <p>This strategy allows for custom failure handling using a {@link ActionRequestFailureHandler}.
	 */
	public Elasticsearch failureHandlerCustom(Class<? extends ActionRequestFailureHandler> failureHandlerClass) {
		internalProperties.putString(CONNECTOR_FAILURE_HANDLER, ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM);
		internalProperties.putClass(CONNECTOR_FAILURE_HANDLER_CLASS, failureHandlerClass);
		return this;
	}

	/**
	 * Disables flushing on checkpoint. When disabled, a sink will not wait for all pending action
	 * requests to be acknowledged by Elasticsearch on checkpoints.
	 *
	 * <p>Note: If flushing on checkpoint is disabled, a Elasticsearch sink does NOT
	 * provide any strong guarantees for at-least-once delivery of action requests.
	 */
	public Elasticsearch disableFlushOnCheckpoint() {
		internalProperties.putBoolean(CONNECTOR_FLUSH_ON_CHECKPOINT, false);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets the maximum number of actions to buffer for each bulk request.
	 *
	 * @param maxActions the maximum number of actions to buffer per bulk request.
	 */
	public Elasticsearch bulkFlushMaxActions(int maxActions) {
		internalProperties.putInt(CONNECTOR_BULK_FLUSH_MAX_ACTIONS, maxActions);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets the maximum size of buffered actions per bulk request (using the syntax of {@link MemorySize}).
	 */
	public Elasticsearch bulkFlushMaxSize(String maxSize) {
		internalProperties.putMemorySize(CONNECTOR_BULK_FLUSH_MAX_SIZE, MemorySize.parse(maxSize, MemorySize.MemoryUnit.BYTES));
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets the bulk flush interval (in milliseconds).
	 *
	 * @param interval bulk flush interval (in milliseconds).
	 */
	public Elasticsearch bulkFlushInterval(long interval) {
		internalProperties.putLong(CONNECTOR_BULK_FLUSH_INTERVAL, interval);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets a constant backoff type to use when flushing bulk requests.
	 */
	public Elasticsearch bulkFlushBackoffConstant() {
		internalProperties.putString(
			CONNECTOR_BULK_FLUSH_BACKOFF_TYPE,
			ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets an exponential backoff type to use when flushing bulk requests.
	 */
	public Elasticsearch bulkFlushBackoffExponential() {
		internalProperties.putString(
			CONNECTOR_BULK_FLUSH_BACKOFF_TYPE,
			ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
	 *
	 * <p>Make sure to enable backoff by selecting a strategy ({@link #bulkFlushBackoffConstant()} or
	 * {@link #bulkFlushBackoffExponential()}).
	 *
	 * @param maxRetries the maximum number of retries.
	 */
	public Elasticsearch bulkFlushBackoffMaxRetries(int maxRetries) {
		internalProperties.putInt(CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES, maxRetries);
		return this;
	}

	/**
	 * Configures how to buffer elements before sending them in bulk to the cluster for efficiency.
	 *
	 * <p>Sets the amount of delay between each backoff attempt when flushing bulk requests (in milliseconds).
	 *
	 * <p>Make sure to enable backoff by selecting a strategy ({@link #bulkFlushBackoffConstant()} or
	 * {@link #bulkFlushBackoffExponential()}).
	 *
	 * @param delay delay between each backoff attempt (in milliseconds).
	 */
	public Elasticsearch bulkFlushBackoffDelay(long delay) {
		internalProperties.putLong(CONNECTOR_BULK_FLUSH_BACKOFF_DELAY, delay);
		return this;
	}

	/**
	 * Sets connection properties to be used during REST communication to Elasticsearch.
	 *
	 * <p>Sets the maximum timeout (in milliseconds) in case of multiple retries of the same request.
	 *
	 * @param maxRetryTimeout maximum timeout (in milliseconds)
	 */
	public Elasticsearch connectionMaxRetryTimeout(int maxRetryTimeout) {
		internalProperties.putInt(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT, maxRetryTimeout);
		return this;
	}

	/**
	 * Sets connection properties to be used during REST communication to Elasticsearch.
	 *
	 * <p>Adds a path prefix to every REST communication.
	 *
	 * @param pathPrefix prefix string to be added to every REST communication
	 */
	public Elasticsearch connectionPathPrefix(String pathPrefix) {
		internalProperties.putString(CONNECTOR_CONNECTION_PATH_PREFIX, pathPrefix);
		return this;
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(internalProperties);

		if (hosts.size() > 0) {
			properties.putString(
				CONNECTOR_HOSTS,
				hosts.stream()
					.map(Host::toString)
					.collect(Collectors.joining(";")));
		}
		return properties.asMap();
	}
}
