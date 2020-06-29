/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all Flink Elasticsearch Sources. This class implements the common behaviour across Elasticsearch versions.
 *
 * <p>The version specific API calls for different Elasticsearch versions should be defined by a concrete implementation of
 * a {@link ElasticsearchApiCallBridge}, which is provided to the constructor of this class. This call bridge is used,
 * for example, to create a Elasticsearch {@link Client}, handle failed item responses, etc.
 *
 * @param <T> Type of the elements handled by this source
 * @param <C> Type of the Elasticsearch client, which implements {@link AutoCloseable}
 */
@Internal
public class ElasticSearchInputFormatBase<T, C extends AutoCloseable> extends RichInputFormat<T, ElasticsearchInputSplit> implements ResultTypeQueryable<T> {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchInputFormatBase.class);
	private static final long serialVersionUID = 1L;

	private final DeserializationSchema<T> deserializationSchema;
	private final String index;
	private final String type;

	private final String[] fieldNames;
	private final QueryBuilder predicate;
	private final long limit;

	private final long scrollTimeout;
	private final int scrollSize;

	private Scroll scroll;
	private String currentScrollWindowId;
	private String[] currentScrollWindowHits;

	private int nextRecordIndex = 0;
	private long currentReadCount = 0L;

	/**
	 * Call bridge for different version-specific.
	 */
	private final ElasticsearchApiCallBridge<C> callBridge;

	private final Map<String, String> userConfig;
	/**
	 * Elasticsearch client created using the call bridge.
	 */
	private transient C client;

	public ElasticSearchInputFormatBase(
		ElasticsearchApiCallBridge<C> callBridge,
		Map<String, String> userConfig,
		DeserializationSchema<T> deserializationSchema,
		String[] fieldNames,
		String index,
		String type,
		long scrollTimeout,
		int scrollSize,
		QueryBuilder predicate,
		long limit) {

		this.callBridge = checkNotNull(callBridge);
		checkNotNull(userConfig);
		// copy config so we can remove entries without side-effects
		this.userConfig = new HashMap<>(userConfig);

		this.deserializationSchema = checkNotNull(deserializationSchema);
		this.index = index;
		this.type = type;

		this.fieldNames = fieldNames;
		this.predicate = predicate;
		this.limit = limit;

		this.scrollTimeout = scrollTimeout;
		this.scrollSize = scrollSize;

	}

	@Override
	public void configure(Configuration parameters) {
		//because of createInputSplits(it needs use client) running before openInputFormat, so we cannot create client in openInputFormat
		try {
			client = callBridge.createClient(userConfig);
			callBridge.verifyClientConnection(client);
		} catch (IOException ioe) {
			LOG.error("Exception while creating connection to Elasticsearch.", ioe);
			throw new RuntimeException("Cannot create connection to Elasticsearcg.", ioe);
		}
	}

	@Override
	public ElasticsearchInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return callBridge.createInputSplitsInternal(client, index, type,  minNumSplits);
	}

	@Override
	public void openInputFormat() throws IOException {
		//do nothing here
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ElasticsearchInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(ElasticsearchInputSplit split) throws IOException {

		SearchRequest searchRequest = new SearchRequest(index);
		if (type == null) {
			searchRequest.types(Strings.EMPTY_ARRAY);
		} else {
			searchRequest.types(type);
		}
		this.scroll = new Scroll(TimeValue.timeValueMinutes(scrollTimeout));
		searchRequest.scroll(scroll);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		int size;
		if (limit > 0) {
			size = (int) Math.min(limit, scrollSize);
		} else {
			size = scrollSize;
		}
		//elasticsearch default value is 10 here.
		searchSourceBuilder.size(size);
		searchSourceBuilder.fetchSource(fieldNames, null);
		if (predicate != null) {
			searchSourceBuilder.query(predicate);
		} else {
			searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		}
		searchRequest.source(searchSourceBuilder);
		searchRequest.preference("_shards:" + split.getShard());
		Tuple2<String, String[]> searchResponse = null;
		try {

			searchResponse = callBridge.search(client, searchRequest);
		} catch (IOException e) {
			LOG.error("Search has error: ", e.getMessage());
		}
		if (searchResponse != null) {
			currentScrollWindowId = searchResponse.f0;
			currentScrollWindowHits = searchResponse.f1;
			nextRecordIndex = 0;
		}
	}

	@Override
	public boolean reachedEnd() {
		if (limit > 0 && currentReadCount >= limit) {
			return true;
		}

		// SearchResponse can be InternalSearchHits.empty(), and the InternalSearchHit[] EMPTY = new InternalSearchHit[0]
		if (currentScrollWindowHits.length != 0 && nextRecordIndex > currentScrollWindowHits.length - 1) {
			fetchNextScrollWindow();
		}

		return currentScrollWindowHits.length == 0;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		if (reachedEnd()) {
			LOG.warn("Already reached the end of the split.");
		}

		String hit = currentScrollWindowHits[nextRecordIndex];
		nextRecordIndex++;
		currentReadCount++;
		LOG.debug("Yielding new record for hit: " + hit);

		return parseSearchHit(hit);
	}

	private void fetchNextScrollWindow() {
		Tuple2<String, String[]> searchResponse = null;
		SearchScrollRequest scrollRequest = new SearchScrollRequest(currentScrollWindowId);
		scrollRequest.scroll(scroll);

		try {
			searchResponse = callBridge.scroll(client, scrollRequest);
		} catch (IOException e) {
			LOG.error("Scroll failed: " + e.getMessage());
		}

		if (searchResponse != null) {
			currentScrollWindowId = searchResponse.f0;
			currentScrollWindowHits = searchResponse.f1;
			nextRecordIndex = 0;
		}
	}

	private T parseSearchHit(String hit) {
		T row = null;
		try {
			row = deserializationSchema.deserialize(hit.getBytes());
		} catch (IOException e) {
			LOG.error("Deserialize search hit failed: " + e.getMessage());
		}

		return row;
	}

	@Override
	public void close() throws IOException {
		//clear split client data
	}

	@Override
	public void closeInputFormat() throws IOException {
		callBridge.close(client);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
