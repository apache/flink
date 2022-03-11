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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

class FailureHandlerTestEmitter implements ElasticsearchEmitter<Tuple2<Integer, String>> {
    private final String index;
    private final String documentType;

    FailureHandlerTestEmitter(String index, String documentType) {
        this.index = index;
        this.documentType = documentType;
    }

    @Override
    public void emit(
            Tuple2<Integer, String> element, SinkWriter.Context context, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
        indexer.add(createUpdateRequest(element));
    }

    private IndexRequest createIndexRequest(Tuple2<Integer, String> element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element.f1);

        String idx;
        if (element.f1.startsWith("message #15")) {
            idx = ":intentional invalid index:";
        } else {
            idx = index;
        }

        return Requests.indexRequest().index(idx).id(element.f1).type(documentType).source(json);
    }

    private UpdateRequest createUpdateRequest(Tuple2<Integer, String> element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element.f1);

        return new UpdateRequest(index, documentType, String.valueOf(element.f0))
                .doc(json)
                .upsert(json);
    }
}
