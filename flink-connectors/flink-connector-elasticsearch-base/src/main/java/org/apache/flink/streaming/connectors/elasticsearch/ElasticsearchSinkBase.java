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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
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
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE = "bulk.flush.backoff.enable";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE = "bulk.flush.backoff.type";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES = "bulk.flush.backoff.retries";
	public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY = "bulk.flush.backoff.delay";

	public enum FlushBackoffType {
		CONSTANT,
		EXPONENTIAL
	}

	public class BulkFlushBackoffPolicy implements Serializable {

		private static final long serialVersionUID = -6022851996101826049L;

		// the default values follow the Elasticsearch default settings for BulkProcessor
		private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
		private int maxRetryCount = 8;
		private long delayMillis = 50;

		public FlushBackoffType getBackoffType() {
			return backoffType;
		}

		public int getMaxRetryCount() {
			return maxRetryCount;
		}

		public long getDelayMillis() {
			return delayMillis;
		}

		public void setBackoffType(FlushBackoffType backoffType) {
			this.backoffType = checkNotNull(backoffType);
		}

		public void setMaxRetryCount(int maxRetryCount) {
			checkArgument(maxRetryCount > 0);
			this.maxRetryCount = maxRetryCount;
		}

		public void setDelayMillis(long delayMillis) {
			checkArgument(delayMillis > 0);
			this.delayMillis = delayMillis;
		}
	}

	private final Integer bulkProcessorFlushMaxActions;
	private final Integer bulkProcessorFlushMaxSizeMb;
	private final Integer bulkProcessorFlushIntervalMillis;
	private final BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

	// ------------------------------------------------------------------------
	//  User-facing API and configuration
	// ------------------------------------------------------------------------

	/** The user specified config map that we forward to Elasticsearch when we create the {@link Client}. */
	private final Map<String, String> userConfig;

	/** The function that is used to construct mulitple {@link ActionRequest ActionRequests} from each incoming element. */
	private final ElasticsearchSinkFunction<T> elasticsearchSinkFunction;

	/** User-provided handler for failed {@link ActionRequest ActionRequests}. */
	private final ActionRequestFailureHandler failureHandler;

	/** Provided to the user via the {@link ElasticsearchSinkFunction} to add {@link ActionRequest ActionRequests}. */
	private transient BulkProcessorIndexer requestIndexer;

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
	 * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown in callbacks and
	 * the user considered it should fail the sink via the
	 * {@link ActionRequestFailureHandler#onFailure(ActionRequest, Throwable, RequestIndexer)} method.
	 *
	 * Errors will be checked and rethrown before processing each input element, and when the sink is closed.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public ElasticsearchSinkBase(
		ElasticsearchApiCallBridge callBridge,
		Map<String, String> userConfig,
		ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
		ActionRequestFailureHandler failureHandler) {

		this.callBridge = checkNotNull(callBridge);
		this.elasticsearchSinkFunction = checkNotNull(elasticsearchSinkFunction);
		this.failureHandler = checkNotNull(failureHandler);

		// we eagerly check if the user-provided sink function and failure handler is serializable;
		// otherwise, if they aren't serializable, users will merely get a non-informative error message
		// "ElasticsearchSinkBase is not serializable"

		try {
			InstantiationUtil.serializeObject(elasticsearchSinkFunction);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"The implementation of the provided ElasticsearchSinkFunction is not serializable. " +
				"The object probably contains or references non serializable fields.");
		}

		try {
			InstantiationUtil.serializeObject(failureHandler);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"The implementation of the provided ActionRequestFailureHandler is not serializable. " +
					"The object probably contains or references non serializable fields.");
		}

		// extract and remove bulk processor related configuration from the user-provided config,
		// so that the resulting user config only contains configuration related to the Elasticsearch client.

		checkNotNull(userConfig);

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

		boolean bulkProcessorFlushBackoffEnable = params.getBoolean(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
		userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

		if (bulkProcessorFlushBackoffEnable) {
			this.bulkProcessorFlushBackoffPolicy = new BulkFlushBackoffPolicy();

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
				bulkProcessorFlushBackoffPolicy.setBackoffType(FlushBackoffType.valueOf(params.get(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
			}

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
				bulkProcessorFlushBackoffPolicy.setMaxRetryCount(params.getInt(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
			}

			if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
				bulkProcessorFlushBackoffPolicy.setDelayMillis(params.getLong(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
				userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
			}

		} else {
			bulkProcessorFlushBackoffPolicy = null;
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
						BulkItemResponse itemResponse;
						Throwable failure;

						for (int i = 0; i < response.getItems().length; i++) {
							itemResponse = response.getItems()[i];
							failure = callBridge.extractFailureCauseFromBulkItemResponse(itemResponse);
							if (failure != null) {
								LOG.error("Failed Elasticsearch item request: {}", itemResponse.getFailureMessage(), failure);
								if (failureHandler.onFailure(request.requests().get(i), failure, requestIndexer)) {
									failureThrowable.compareAndSet(null, failure);
								}
							}
						}
					}
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					LOG.error("Failed Elasticsearch bulk request: {}", failure.getMessage(), failure.getCause());

					// whole bulk request failures are usually just temporary timeouts on
					// the Elasticsearch side; simply retry all action requests in the bulk
					for (ActionRequest action : request.requests()) {
						requestIndexer.add(action);
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

		// if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
		callBridge.configureBulkProcessorBackoff(bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

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

	private void checkErrorAndRethrow() {
		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
		}
	}
}
