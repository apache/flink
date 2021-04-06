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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.elasticsearch.client.Client;

import java.io.File;

/**
 * The {@link EmbeddedElasticsearchNodeEnvironment} is used in integration tests to manage
 * Elasticsearch embedded nodes.
 *
 * <p>NOTE: In order for {@link ElasticsearchSinkTestBase} to dynamically load version-specific
 * implementations for the tests, concrete implementations must be named {@code
 * EmbeddedElasticsearchNodeEnvironmentImpl}. It must also be located under the same package. The
 * intentional package-private accessibility of this interface enforces that.
 */
public interface EmbeddedElasticsearchNodeEnvironment {

    /**
     * Start an embedded Elasticsearch node instance. Calling this method multiple times
     * consecutively should not restart the embedded node.
     *
     * @param tmpDataFolder The temporary data folder for the embedded node to use.
     * @param clusterName The name of the cluster that the embedded node should be configured with.
     */
    void start(File tmpDataFolder, String clusterName) throws Exception;

    /** Close the embedded node, if previously started. */
    void close() throws Exception;

    /**
     * Returns a client to communicate with the embedded node.
     *
     * @return Client to communicate with the embedded node. Returns {@code null} if the embedded
     *     node wasn't started or is closed.
     */
    Client getClient();
}
