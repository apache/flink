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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * A dummy {@link ElasticsearchSinkFunction} that wraps a {@link IndexRequestBuilder}.
 * This serves as a bridge for the usage deprecation of the {@code IndexRequestBuilder} interface.
 */
@Internal
class IndexRequestBuilderWrapperFunction<T> implements ElasticsearchSinkFunction<T> {

	private static final long serialVersionUID = 289876038414250101L;

	private final IndexRequestBuilder<T> indexRequestBuilder;

	IndexRequestBuilderWrapperFunction(IndexRequestBuilder<T> indexRequestBuilder) {
		this.indexRequestBuilder = indexRequestBuilder;
	}

	@Override
	public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
		indexer.add(indexRequestBuilder.createIndexRequest(element, ctx));
	}
}
