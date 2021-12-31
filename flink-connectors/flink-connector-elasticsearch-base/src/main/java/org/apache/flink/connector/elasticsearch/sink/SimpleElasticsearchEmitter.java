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

import org.elasticsearch.action.update.UpdateRequest;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/** A ElasticsearchEmitter that is currently used Python Flink Connector. */
public class SimpleElasticsearchEmitter implements ElasticsearchEmitter<Map<String, Object>> {

    private static final long serialVersionUID = 1L;
    private Function<Map<String, Object>, UpdateRequest> requestGenerator;

    public SimpleElasticsearchEmitter(
            String index, String documentType, String idFieldName, boolean isDynamicIndex) {
        // If this issue resolve https://issues.apache.org/jira/browse/MSHADE-260
        // we can replace requestGenerator with lambda.
        // Other corresponding issues https://issues.apache.org/jira/browse/FLINK-18857 and
        // https://issues.apache.org/jira/browse/FLINK-18006
        if (isDynamicIndex) {
            this.requestGenerator =
                    new DynamicIndexRequestGenerator(index, documentType, idFieldName);
        } else {
            this.requestGenerator =
                    new StaticIndexRequestGenerator(index, documentType, idFieldName);
        }
    }

    public void emit(
            Map<String, Object> element, SinkWriter.Context context, RequestIndexer indexer) {
        indexer.add(requestGenerator.apply(element));
    }

    private static class StaticIndexRequestGenerator
            implements Function<Map<String, Object>, UpdateRequest>, Serializable {
        private String index;
        private String documentType;
        private String idFieldName;

        public StaticIndexRequestGenerator(String index, String documentType, String idFieldName) {
            this.index = index;
            this.documentType = documentType;
            this.idFieldName = idFieldName;
        }

        public UpdateRequest apply(Map<String, Object> doc) {
            return new UpdateRequest(index, documentType, doc.get(idFieldName).toString())
                    .doc(doc)
                    .upsert(doc);
        }
    }

    private static class DynamicIndexRequestGenerator
            implements Function<Map<String, Object>, UpdateRequest>, Serializable {
        private String index;
        private String documentType;
        private String idFieldName;

        public DynamicIndexRequestGenerator(String index, String documentType, String idFieldName) {
            this.index = index;
            this.documentType = documentType;
            this.idFieldName = idFieldName;
        }

        public UpdateRequest apply(Map<String, Object> doc) {
            return new UpdateRequest(
                            doc.get(index).toString(),
                            documentType,
                            doc.get(idFieldName).toString())
                    .doc(doc)
                    .upsert(doc);
        }
    }
}
