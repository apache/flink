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

import org.elasticsearch.action.update.UpdateRequest;

import java.util.Map;

/** Factory for creating UpdateRequests of Elasticsearch7. */
public class UpdateRequest7Factory implements UpdateRequestFactory {

    private static final long serialVersionUID = 1L;

    private final String indexName;

    /**
     * Instantiates a new update request factory for of Elasticsearch7.
     *
     * @param indexName The index name.
     */
    public UpdateRequest7Factory(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public UpdateRequest createUpdateRequest(KeyValue<Integer, String> element) {
        Map<String, Object> json = UpdateRequestFactory.prepareDoc(element);
        return new UpdateRequest(indexName, String.valueOf(element.key)).doc(json).upsert(json);
    }
}
