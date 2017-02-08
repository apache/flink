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

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.client.Client;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map;

/**
 * An {@link ElasticsearchApiCallBridge} is used to bridge incompatible Elasticsearch Java API calls across different versions.
 * This includes calls to create Elasticsearch clients, handle failed item responses, etc. Any incompatible Elasticsearch
 * Java APIs should be bridged using this interface.
 *
 * Implementations are allowed to be stateful. For example, for Elasticsearch 1.x, since connecting via an embedded node
 * is allowed, the call bridge will hold reference to the created embedded node. Each instance of the sink will hold
 * exactly one instance of the call bridge, and state cleanup is performed when the sink is closed.
 */
public interface ElasticsearchApiCallBridge extends Serializable {

	/**
	 * Creates an Elasticsearch {@link Client}.
	 *
	 * @param clientConfig The configuration to use when constructing the client.
	 * @return The created client.
	 */
	Client createClient(Map<String, String> clientConfig);

	/**
	 * Extracts the cause of failure of a bulk item action.
	 *
	 * @param bulkItemResponse the bulk item response to extract cause of failure
	 * @return the extracted {@link Throwable} from the response ({@code null} is the response is successful).
	 */
	@Nullable Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse);

	/**
	 * Perform any necessary state cleanup.
	 */
	void cleanup();

}
