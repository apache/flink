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
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;

/**
 * For version-agnostic creating of {@link ActionRequest}s.
 */
@Internal
interface RequestFactory extends Serializable {
	/**
	 * Creates an update request to be added to a {@link RequestIndexer}.
	 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
	 */
	UpdateRequest createUpdateRequest(
		String index,
		String docType,
		String key,
		XContentType contentType,
		byte[] document);

	/**
	 * Creates an index request to be added to a {@link RequestIndexer}.
	 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
	 */
	IndexRequest createIndexRequest(
		String index,
		String docType,
		String key,
		XContentType contentType,
		byte[] document);

	/**
	 * Creates a delete request to be added to a {@link RequestIndexer}.
	 * Note: the type field has been deprecated since Elasticsearch 7.x and it would not take any effort.
	 */
	DeleteRequest createDeleteRequest(
		String index,
		String docType,
		String key);
}
