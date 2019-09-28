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

import org.apache.flink.annotation.PublicEvolving;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * Users add multiple delete, index or update requests to a {@link RequestIndexer} to prepare
 * them for sending to an Elasticsearch cluster.
 */
@PublicEvolving
public interface RequestIndexer {

	/**
	 * Add multiple {@link ActionRequest} to the indexer to prepare for sending requests to Elasticsearch.
	 *
	 * @param actionRequests The multiple {@link ActionRequest} to add.
	 * @deprecated use the {@link DeleteRequest}, {@link IndexRequest} or {@link UpdateRequest}
	 */
	@Deprecated
	default void add(ActionRequest... actionRequests) {
		for (ActionRequest actionRequest : actionRequests) {
			if (actionRequest instanceof IndexRequest) {
				add((IndexRequest) actionRequest);
			} else if (actionRequest instanceof DeleteRequest) {
				add((DeleteRequest) actionRequest);
			} else if (actionRequest instanceof UpdateRequest) {
				add((UpdateRequest) actionRequest);
			} else {
				throw new IllegalArgumentException("RequestIndexer only supports Index, Delete and Update requests");
			}
		}
	}

	/**
	 * Add multiple {@link DeleteRequest} to the indexer to prepare for sending requests to Elasticsearch.
	 *
	 * @param deleteRequests The multiple {@link DeleteRequest} to add.
	 */
	void add(DeleteRequest... deleteRequests);

	/**
	 * Add multiple {@link IndexRequest} to the indexer to prepare for sending requests to Elasticsearch.
	 *
	 * @param indexRequests The multiple {@link IndexRequest} to add.
	 */
	void add(IndexRequest... indexRequests);

	/**
	 * Add multiple {@link UpdateRequest} to the indexer to prepare for sending requests to Elasticsearch.
	 *
	 * @param updateRequests The multiple {@link UpdateRequest} to add.
	 */
	void add(UpdateRequest... updateRequests);
}
