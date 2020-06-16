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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @param <C>
 */
public class ElasticsearchLookupFunction<C extends AutoCloseable> extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchLookupFunction.class);

	private String index;

	private String type;

	private transient C client;

	private BoolQueryBuilder looupCondition;

	private String[] fieldNames;

	private String[] keyNames;

	private final ElasticsearchApiCallBridge<C> callBridge;

	private transient Cache<RowData, List<RowData>> cache;

	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	public ElasticsearchLookupFunction(
		ElasticsearchLookupOptions lookupOptions,
		String index,
		String type,
		String[] fieldNames,
		String[] keyNames,
		ElasticsearchApiCallBridge<C> callBridge) {

		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();

		this.index = index;
		this.type = type;
		this.fieldNames = fieldNames;
		this.keyNames = keyNames;
		this.callBridge = callBridge;
		this.client = callBridge.createClient(null);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
			.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
			.maximumSize(cacheMaxSize)
			.build();
	}

	public void eval(Object... keys) {
		RowData keyRow = GenericRowData.of(keys);
		if (cache != null) {
			List<RowData> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (RowData cachedRow : cachedRows) {
					collect(cachedRow);
				}
				return;
			}
		}

		SearchRequest searchRequest = new SearchRequest(index);
		if (type == null) {
			searchRequest.types(Strings.EMPTY_ARRAY);
		} else {
			searchRequest.types(type);
		}

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(fieldNames, null);

		for (int i = 0; i < fieldNames.length; i++) {
			looupCondition.must(new TermQueryBuilder(keyNames[i], keys[i]));
		}
		searchSourceBuilder.query(looupCondition);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = null;
		try {

			searchResponse = callBridge.search(client, searchRequest);
		} catch (IOException e) {
			LOG.error("Search has error: ", e.getMessage());
		}
		if (searchResponse != null) {
			SearchHit[] result = searchResponse.getHits().getHits();
		}
	}
}
