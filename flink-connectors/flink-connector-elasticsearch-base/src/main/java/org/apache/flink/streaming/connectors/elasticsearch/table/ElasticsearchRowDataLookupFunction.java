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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A lookup function for ElasticsearchSource.
 */
@Internal
public class ElasticsearchRowDataLookupFunction<C extends AutoCloseable> extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private final DeserializationSchema<RowData> deserializationSchema;

	private final String index;
	private final String type;

	private final String[] producedNames;
	private final String[] lookupKeys;
	// converters to convert data from internal to external in order to generate keys for the cache.
	private final DataFormatConverter[] converters;
	private SearchRequest searchRequest;
	private SearchSourceBuilder searchSourceBuilder;

	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final ElasticsearchApiCallBridge<C> callBridge;

	private transient C client;
	private transient Cache<RowData, List<RowData>> cache;

	public ElasticsearchRowDataLookupFunction(
		DeserializationSchema<RowData> deserializationSchema,
		ElasticsearchLookupOptions lookupOptions,
		String index,
		String type,
		String[] producedNames,
		DataType[] producedTypes,
		String[] lookupKeys,
		ElasticsearchApiCallBridge<C> callBridge) {

		checkNotNull(deserializationSchema, "No DeserializationSchema supplied.");
		checkNotNull(lookupOptions, "No ElasticsearchLookupOptions supplied.");
		checkNotNull(producedNames, "No fieldNames supplied.");
		checkNotNull(producedTypes, "No fieldTypes supplied.");
		checkNotNull(lookupKeys, "No keyNames supplied.");
		checkNotNull(callBridge, "No ElasticsearchApiCallBridge supplied.");

		this.deserializationSchema = deserializationSchema;
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();

		this.index = index;
		this.type = type;
		this.producedNames = producedNames;
		this.lookupKeys = lookupKeys;
		this.converters = new DataFormatConverter[lookupKeys.length];
		Map<String, Integer> nameToIndex = IntStream.range(0, producedNames.length).boxed().collect(
			Collectors.toMap(i -> producedNames[i], i -> i));
		for (int i = 0; i < lookupKeys.length; i++) {
			Integer position = nameToIndex.get(lookupKeys[i]);
			Preconditions.checkArgument(position != null, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
			converters[i] = DataFormatConverters.getConverterForDataType(producedTypes[position]);
		}

		this.callBridge = callBridge;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
			.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
			.maximumSize(cacheMaxSize)
			.build();
		this.client = callBridge.createClient(null);

		//Set searchRequest in open method in case of amount of calling in eval method when every record comes.
		this.searchRequest = new SearchRequest(index);
		if (type == null) {
			searchRequest.types(Strings.EMPTY_ARRAY);
		} else {
			searchRequest.types(type);
		}
		searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.fetchSource(producedNames, null);
	}


	/**
	 * This is a lookup method which is called by Flink framework in runtime.
	 *
	 * @param keys lookup keys
	 */
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

		BoolQueryBuilder lookupCondition = new BoolQueryBuilder();
		for (int i = 0; i < lookupKeys.length; i++) {
			lookupCondition.must(new TermQueryBuilder(lookupKeys[i], converters[i].toExternal(keys[i])));
		}
		searchSourceBuilder.query(lookupCondition);
		searchRequest.source(searchSourceBuilder);

		Tuple2<String, String[]> searchResponse = null;

		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			try {
				searchResponse = callBridge.search(client, searchRequest);
				if (searchResponse.f1.length > 0) {
					String[] result = searchResponse.f1;
					// if cache disabled
					if (cache == null) {
						for (int i = 0; i < result.length; i++) {
							RowData row = parseSearchHit(result[i]);
							collect(row);
						}
					} else { // if cache enabled
						ArrayList<RowData> rows = new ArrayList<>();
						for (int i = 0; i < result.length; i++) {
							RowData row = parseSearchHit(result[i]);
							collect(row);
							rows.add(row);
						}
						cache.put(keyRow, rows);
					}
				}
				break;
			} catch (IOException e) {
				LOG.error(String.format("Elasticsearch search error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("Execution of Elasticsearch search failed.", e);
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					LOG.warn("Interrupted while waiting to retry failed elasticsearch search, aborting");
					throw new RuntimeException(e1);
				}
			}
		}
	}

	private RowData parseSearchHit(String hit) {
		RowData row = null;
		try {
			row = deserializationSchema.deserialize(hit.getBytes());
		} catch (IOException e) {
			LOG.error("Deserialize search hit failed: " + e.getMessage());
		}

		return row;
	}
}
