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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.Internal;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * Users add multiple delete, index or update requests to a {@link RequestIndexer} to prepare them
 * for sending to an Elasticsearch cluster.
 */
@Internal
public interface RequestIndexer {
    /**
     * Add multiple {@link DeleteRequest} to the indexer to prepare for sending requests to
     * Elasticsearch.
     *
     * @param deleteRequests The multiple {@link DeleteRequest} to add.
     */
    void add(DeleteRequest... deleteRequests);

    /**
     * Add multiple {@link IndexRequest} to the indexer to prepare for sending requests to
     * Elasticsearch.
     *
     * @param indexRequests The multiple {@link IndexRequest} to add.
     */
    void add(IndexRequest... indexRequests);

    /**
     * Add multiple {@link UpdateRequest} to the indexer to prepare for sending requests to
     * Elasticsearch.
     *
     * @param updateRequests The multiple {@link UpdateRequest} to add.
     */
    void add(UpdateRequest... updateRequests);
}
