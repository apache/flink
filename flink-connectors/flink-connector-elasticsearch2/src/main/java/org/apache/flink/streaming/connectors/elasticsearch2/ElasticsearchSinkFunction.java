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

package org.apache.flink.streaming.connectors.elasticsearch2;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * Method that creates multiple {@link org.elasticsearch.action.ActionRequest}s from an element in a Stream.
 *
 * <p>This is used by {@link ElasticsearchSink} to prepare elements for sending them to Elasticsearch.
 *
 * <p>Example:
 *
 * <pre>{@code
 *					private static class TestElasticSearchSinkFunction implements
 *						ElasticsearchSinkFunction<Tuple2<Integer, String>> {
 *
 *					public IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
 *						Map<String, Object> json = new HashMap<>();
 *						json.put("data", element.f1);
 *
 *						return Requests.indexRequest()
 *							.index("my-index")
 *							.type("my-type")
 *							.id(element.f0.toString())
 *							.source(json);
 *						}
 *
 *				public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
 *					indexer.add(createIndexRequest(element));
 *				}
 *		}
 *
 * }</pre>
 *
 * @param <T> The type of the element handled by this {@code ElasticsearchSinkFunction}
 *
 * @deprecated Deprecated since 1.2, to be removed at 2.0.
 *             This class has been deprecated due to package relocation.
 *             Please use {@link org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction} instead.
 */
@Deprecated
public interface ElasticsearchSinkFunction<T> extends Serializable, Function {
	void process(T element, RuntimeContext ctx, RequestIndexer indexer);
}
