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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import org.elasticsearch.action.index.IndexRequest;

import java.io.Serializable;

/**
 * Function that creates an {@link IndexRequest} from an element in a Stream.
 *
 * <p>This is used by {@link org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink}
 * to prepare elements for sending them to Elasticsearch. See
 * <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/index_.html">Index API</a>
 * for information about how to format data for adding it to an Elasticsearch index.
 *
 * <p>Example:
 *
 * <pre>{@code
 *     private static class MyIndexRequestBuilder implements IndexRequestBuilder<String> {
 *
 *         public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
 *             Map<String, Object> json = new HashMap<>();
 *             json.put("data", element);
 *
 *             return Requests.indexRequest()
 *                 .index("my-index")
 *                 .type("my-type")
 *                 .source(json);
 *         }
 *     }
 * }</pre>
 *
 * @param <T> The type of the element handled by this {@code IndexRequestBuilder}
 *
 * @deprecated Deprecated since version 1.2, to be removed at version 2.0.
 *             Please create a {@link ElasticsearchSink} using a {@link ElasticsearchSinkFunction} instead.
 */
@Deprecated
public interface IndexRequestBuilder<T> extends Function, Serializable {

	/**
	 * Creates an {@link org.elasticsearch.action.index.IndexRequest} from an element.
	 *
	 * @param element The element that needs to be turned in to an {@code IndexRequest}
	 * @param ctx The Flink {@link RuntimeContext} of the {@link ElasticsearchSink}
	 *
	 * @return The constructed {@code IndexRequest}
	 */
	IndexRequest createIndexRequest(T element, RuntimeContext ctx);
}
