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

import org.elasticsearch.client.Client;

import java.io.Serializable;
import java.util.Map;

/**
 * An {@link ElasticsearchClientFactory} is used to instantiate Elasticsearch clients.
 * The interface is used as a bridge for the incompatible Elasticsearch Java APIs across different versions for creating
 * clients.
 *
 * Version-specific implementations of a {@link ElasticsearchSinkBase} should also implement a
 * {@code ElasticsearchClientFactory}.
 */
public interface ElasticsearchClientFactory extends Serializable {

	/**
	 * Creates an Elasticsearch {@link Client}.
	 *
	 * @param clientConfig The configuration to use when constructing the client.
	 * @return The created client.
	 */
	Client create(Map<String, String> clientConfig);

	/**
	 * Perform any necessary cleanup procedures.
	 */
	void cleanup();

}
