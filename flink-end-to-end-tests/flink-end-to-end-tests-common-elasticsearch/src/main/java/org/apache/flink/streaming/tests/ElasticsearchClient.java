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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.streaming.tests;

import java.util.List;

/** The version-agnostic Elasticsearch client interface. */
public interface ElasticsearchClient {

    /**
     * Delete the index.
     *
     * @param indexName The index name.
     */
    void deleteIndex(String indexName);

    /**
     * Refresh the index.
     *
     * @param indexName The index name.
     */
    void refreshIndex(String indexName);

    /**
     * Create index if it does not exist.
     *
     * @param indexName The index name.
     * @param shards The number of shards.
     * @param replicas The number of replicas.
     */
    void createIndexIfDoesNotExist(String indexName, int shards, int replicas);

    /** Close the client. @throws Exception The exception. */
    void close() throws Exception;

    /**
     * Fetch all results from the index.
     *
     * @param params The parameters of the query.
     * @return All documents from the index.
     */
    List<KeyValue<Integer, String>> fetchAll(QueryParams params);
}
