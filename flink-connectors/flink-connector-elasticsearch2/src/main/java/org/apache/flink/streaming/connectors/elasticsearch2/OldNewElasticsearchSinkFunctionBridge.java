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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

/**
 * A dummy {@link org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction} to bridge
 * the migration from the deprecated {@link ElasticsearchSinkFunction}.
 */
class OldNewElasticsearchSinkFunctionBridge<T> implements org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction<T> {

	private static final long serialVersionUID = 2415651895272659448L;

	private final ElasticsearchSinkFunction<T> deprecated;
	private OldNewRequestIndexerBridge reusedRequestIndexerBridge;

	OldNewElasticsearchSinkFunctionBridge(ElasticsearchSinkFunction<T> deprecated) {
		this.deprecated = deprecated;
	}

	@Override
	public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
		if (reusedRequestIndexerBridge == null) {
			reusedRequestIndexerBridge = new OldNewRequestIndexerBridge(indexer);
		}
		deprecated.process(element, ctx, reusedRequestIndexerBridge);
	}
}
