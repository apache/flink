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

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple ElasticsearchEmitter which is currently used in PyFlink ES connector. */
public class SimpleElasticsearchEmitter implements ElasticsearchEmitter<Map<String, Object>> {

    private static final long serialVersionUID = 1L;

    private final String index;
    private @Nullable final String documentType;
    private @Nullable final String idFieldName;
    private final boolean isDynamicIndex;

    private transient BiConsumer<Map<String, Object>, RequestIndexer> requestGenerator;

    public SimpleElasticsearchEmitter(
            String index,
            @Nullable String documentType,
            @Nullable String idFieldName,
            boolean isDynamicIndex) {
        this.index = checkNotNull(index);
        this.documentType = documentType;
        this.idFieldName = idFieldName;
        this.isDynamicIndex = isDynamicIndex;
    }

    @Override
    public void open() throws Exception {
        // If this issue resolve https://issues.apache.org/jira/browse/MSHADE-260
        // we can replace requestGenerator with lambda.
        // Other corresponding issues https://issues.apache.org/jira/browse/FLINK-18857 and
        // https://issues.apache.org/jira/browse/FLINK-18006
        if (isDynamicIndex) {
            requestGenerator = new DynamicIndexRequestGenerator(index, documentType, idFieldName);
        } else {
            requestGenerator = new StaticIndexRequestGenerator(index, documentType, idFieldName);
        }
    }

    @Override
    public void emit(
            Map<String, Object> element, SinkWriter.Context context, RequestIndexer indexer) {
        requestGenerator.accept(element, indexer);
    }

    private static class StaticIndexRequestGenerator
            implements BiConsumer<Map<String, Object>, RequestIndexer> {

        private final String index;
        private final @Nullable String documentType;
        private final @Nullable String idFieldName;

        public StaticIndexRequestGenerator(
                String index, @Nullable String documentType, @Nullable String idFieldName) {
            this.index = checkNotNull(index);
            this.documentType = documentType;
            this.idFieldName = idFieldName;
        }

        public void accept(Map<String, Object> doc, RequestIndexer indexer) {
            if (idFieldName != null) {
                final UpdateRequest updateRequest =
                        new UpdateRequest(index, documentType, doc.get(idFieldName).toString())
                                .doc(doc)
                                .upsert(doc);
                indexer.add(updateRequest);
            } else {
                final IndexRequest indexRequest = new IndexRequest(index, documentType).source(doc);
                indexer.add(indexRequest);
            }
        }
    }

    private static class DynamicIndexRequestGenerator
            implements BiConsumer<Map<String, Object>, RequestIndexer> {

        private final String indexFieldName;
        private final @Nullable String documentType;
        private final @Nullable String idFieldName;

        public DynamicIndexRequestGenerator(
                String indexFieldName,
                @Nullable String documentType,
                @Nullable String idFieldName) {
            this.indexFieldName = checkNotNull(indexFieldName);
            this.documentType = documentType;
            this.idFieldName = idFieldName;
        }

        public void accept(Map<String, Object> doc, RequestIndexer indexer) {
            if (idFieldName != null) {
                final UpdateRequest updateRequest =
                        new UpdateRequest(
                                        doc.get(indexFieldName).toString(),
                                        documentType,
                                        doc.get(idFieldName).toString())
                                .doc(doc)
                                .upsert(doc);
                indexer.add(updateRequest);
            } else {
                final IndexRequest indexRequest =
                        new IndexRequest(doc.get(indexFieldName).toString(), documentType)
                                .source(doc);
                indexer.add(indexRequest);
            }
        }
    }
}
