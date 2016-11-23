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
import org.apache.flink.util.InstantiationUtil;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all Flink Elasticsearch Sinks.
 *
 * <p>
 * This class implements the common behaviour across Elasticsearch versions, such as
 * the use of an internal {@link BulkProcessor} to buffer multiple {@link ActionRequest}s before
 * sending the requests to the cluster, as well as passing input records to the user provided
 * {@link ElasticsearchSinkFunction} for processing.
 *
 * <p>
 * The version specific API calls for different Elasticsearch versions should be defined by a concrete implementation of
 * a {@link ElasticsearchApiCallBridge}, which is provided to the constructor of this class. This call bridge is used,
 * for example, to create a Elasticsearch {@link Client}, handle failed item responses, etc.
 *
 * @param <T> Type of the elements handled by this sink
 */
public abstract class ElasticsearchSinkBase<T> extends RichSinkFunction<T> {

	private static final long serialVersionUID = -1007596293618451942L;

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSinkBase.class);

	// ------------------------------------------------------------------------
	//  Internal bulk processor configuration
	// ------------------------------------------------------------------------

	public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
	public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
	public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";

	private final Integer bulkProcessorFlushMaxActions;
	private final Integer bulkProcessorFlushMaxSizeMb;
	private final Integer bulkProcessorFlushIntervalMillis;

	// ------------------------------------------------------------------------
	//  User-facing API and configuration
	// ------------------------------------------------------------------------

	/** The user specified config map that we forward to Elasticsearch when we create the {@link Client}. */
	private final Map<String, String> userConfig;

	/** The function that is used to construct mulitple {@link ActionRequest ActionRequests} from each incoming element. */
	private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

	/** Provided to the user via the {@link ElasticsearchSinkFunction} to add {@link ActionRequest ActionRequests}. */
	private transient BulkProcessorIndexer requestIndexer;

	/**
	 * When set to <code>true</code> and the bulk action fails, the error message will be checked for
	 * common patterns like <i>timeout</i>, <i>UnavailableShardsException</i> or a full buffer queue on the node.
	 * When a matching pattern is found, the bulk will be retried.
	 */
	protected boolean checkErrorAndRetryBulk = false;

	// ------------------------------------------------------------------------
	//  Internals for the Flink Elasticsearch Sink
	// ------------------------------------------------------------------------

	/** Call bridge for different version-specfic */
	private final ElasticsearchApiCallBridge callBridge;

	/** Elasticsearch client created using the call bridge. */
	private transient Client client;

	/** Bulk processor to buffer and send requests to Elasticsearch, created using the client. */
	private transient BulkProcessor bulkProcessor;

	/**
	 * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown in callbacks.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public ElasticsearchSinkBase(
		ElasticsearchApiCallBridge callBridge,
		Map<String, String> userConfig,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {

		this.callBridge = checkNotNull(callBridge);
		this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);

		// we eagerly check if the user-provided sink function is serializable;
		// otherwise, if it isn't serializable, users will merely get a non-informative error message
		// "ElasticsearchSinkBase is not serializable"
		try {
			InstantiationUtil.serializeObject(elasticsearchSinkFunction);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"The implementation of the provided ElasticsearchSinkFunction is not serializable. " +
				"The object probably contains or references non serializable fields.");
		}

		checkNotNull(userConfig);

		// extract and remove bulk processor related configuration from the user-provided config,
		// so that the resulting user config only contains configuration related to the Elasticsearch client.
		ParameterTool params = ParameterTool.fromMap(userConfig);

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
			bulkProcessorFlushMaxActions = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
		} else {
			bulkProcessorFlushMaxActions = null;
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
			bulkProcessorFlushMaxSizeMb = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
		} else {
			bulkProcessorFlushMaxSizeMb = null;
		}

		if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
			bulkProcessorFlushIntervalMillis = params.getInt(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
			userConfig.remove(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
		} else {
			bulkProcessorFlushIntervalMillis = null;
		}

		this.userConfig = userConfig;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		client = callBridge.createClient(userConfig);

		BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
			client,
			new BulkProcessor.Listener() {
				@Override
				public void beforeBulk(long executionId, BulkRequest request) { }

				@Override
				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					if (response.hasFailures()) {
						boolean allRequestsRepeatable = true;

						for (BulkItemResponse itemResp : response.getItems()) {
							Throwable failure = callBridge.extractFailureCauseFromBulkItemResponse(itemResp);
							if (failure != null) {
								String failureMessageLowercase = itemResp.getFailureMessage().toLowerCase();

								// Check if index request can be retried
								if (checkErrorAndRetryBulk && (
									failureMessageLowercase.contains("timeout") ||
										failureMessageLowercase.contains("timed out") || // Generic timeout errors
										failureMessageLowercase.contains("UnavailableShardsException".toLowerCase()) || // Shard not available due to rebalancing or node down
										(failureMessageLowercase.contains("data/write/bulk") && failureMessageLowercase.contains("bulk")))) // Bulk index queue on node full
								{
									LOG.debug("Retry bulk: {}", itemResp.getFailureMessage());
								} else {
									// Cannot retry action
									allRequestsRepeatable = false;
									LOG.error("Failed Elasticsearch item request: {}", failure.getMessage(), failure);
									failureThrowable.compareAndSet(null, failure);
								}
							}
						}

						if (allRequestsRepeatable) {
							reAddBulkRequest(request);
						}
					}
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					if (checkErrorAndRetryBulk && (
						failure instanceof ClusterBlockException // Examples: "no master"
							|| failure instanceof ElasticsearchTimeoutException) // ElasticsearchTimeoutException sounded good, not seen in stress tests yet
						)
					{
						LOG.debug("Retry bulk on throwable: {}", failure.getMessage());
						reAddBulkRequest(request);
					} else {
						LOG.error("Failed Elasticsearch bulk request: {}", failure.getMessage(), failure);
						failureThrowable.compareAndSet(null, failure);
					}
				}
			}
		);

		// This makes flush() blocking
		bulkProcessorBuilder.setConcurrentRequests(0);

		if (bulkProcessorFlushMaxActions != null) {
			bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
		}

		if (bulkProcessorFlushMaxSizeMb != null) {
			bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeMb, ByteSizeUnit.MB));
		}

		if (bulkProcessorFlushIntervalMillis != null) {
			bulkProcessorBuilder.setFlushInterval(TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
		}

		bulkProcessor = bulkProcessorBuilder.build();
		requestIndexer = new BulkProcessorIndexer(bulkProcessor);
	}

	@Override
	public void invoke(T value) throws Exception {
		// if bulk processor callbacks have previously reported an error, we rethrow the error and fail the sink
		checkErrorAndRethrow();

		elasticsearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
	}

	@Override
	public void close() throws Exception {
		if (bulkProcessor != null) {
			bulkProcessor.close();
			bulkProcessor = null;
		}

		if (client != null) {
			client.close();
			client = null;
		}

		callBridge.cleanup();

		// make sure any errors from callbacks are rethrown
		checkErrorAndRethrow();
	}

	/**
	 * Adds all requests of the bulk to the BulkProcessor. Used when trying again.
	 * @param bulkRequest
	 */
	public void reAddBulkRequest(BulkRequest bulkRequest) {
		//TODO Check what happens when bulk contains a DeleteAction and IndexActions and the DeleteAction fails because the document already has been deleted. This may not happen in typical Flink jobs.

		for (IndicesRequest req : bulkRequest.subRequests()) {
			if (req instanceof ActionRequest) {
				// There is no waiting time between index requests, so this may produce additional pressure on cluster
				bulkProcessor.add((ActionRequest<?>) req);
			}
		}
	}

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
		}
	}
}
