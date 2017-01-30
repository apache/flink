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
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;

import javax.annotation.Nullable;
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
 * The version specific behaviours for creating a {@link Client} to connect to a Elasticsearch cluster
 * should be defined by concrete implementations of a {@link ElasticsearchClientFactory}, which is to be provided to the
 * constructor of this class.
 *
 * @param <T> Type of the elements handled by this sink
 */
public abstract class ElasticsearchSinkBase<T> extends RichSinkFunction<T> {

	private static final long serialVersionUID = -1007596293618451942L;

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

	// ------------------------------------------------------------------------
	//  Internals for the Flink Elasticsearch Sink
	// ------------------------------------------------------------------------

	/** Version-specific factory for Elasticsearch clients, provided by concrete subclasses. */
	private final ElasticsearchClientFactory clientFactory;

	/** Elasticsearch client created using the client factory. */
	private transient Client client;

	/** Bulk processor to buffer and send requests to Elasticsearch, created using the client. */
	private transient BulkProcessor bulkProcessor;

	/**
	 * This reference will be passed via {@link ElasticsearchSinkBase#buildBulkProcessor(Client, AtomicReference, Integer, Integer, Integer)}
	 * to allow version-specific implementations of the {@link BulkProcessor.Listener} to set {@link Throwable} thrown during processing.
	 */
	private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

	public ElasticsearchSinkBase(ElasticsearchClientFactory clientFactory,
								Map<String, String> userConfig,
								ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		this.clientFactory = checkNotNull(clientFactory);
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
		client = clientFactory.create(userConfig);

		bulkProcessor = buildBulkProcessor(
			client,
			failureThrowable,
			bulkProcessorFlushMaxActions,
			bulkProcessorFlushMaxSizeMb,
			bulkProcessorFlushIntervalMillis);

		requestIndexer = new BulkProcessorIndexer(bulkProcessor);
	}

	@Override
	public void invoke(T value) throws Exception {
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

		clientFactory.cleanup();

		Throwable cause = failureThrowable.get();
		if (cause != null) {
			throw new RuntimeException("An error occured in ElasticsearchSink.", cause);
		}
	}

	/**
	 * Build a {@link BulkProcessor}, using the specified configuration values.
	 *
	 * @param client the Elasticsearch client to use to build the bulk processor.
	 * @param listenerFailureRef should be used to reference {@link Throwable} thrown in the listener callback.
	 * @param bulkProcessorFlushMaxActions maximum number of actions to buffer before flushing
	 * @param bulkProcessorFlushMaxSizeMb maximum size of actions to buffer before flushing
	 * @param bulkProcessorFlushIntervalMillis flush interval, regardless of the number or size of buffered actions.
	 * @return the built {@link BulkProcessor}
	 */
	protected abstract BulkProcessor buildBulkProcessor(final Client client,
														final AtomicReference<Throwable> listenerFailureRef,
														@Nullable final Integer bulkProcessorFlushMaxActions,
														@Nullable final Integer bulkProcessorFlushMaxSizeMb,
														@Nullable final Integer bulkProcessorFlushIntervalMillis);
}
