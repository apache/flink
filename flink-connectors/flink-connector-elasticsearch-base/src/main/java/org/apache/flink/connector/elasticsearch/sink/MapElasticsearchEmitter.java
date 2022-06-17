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
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A simple ElasticsearchEmitter which is currently used in PyFlink ES connector. */
public class MapElasticsearchEmitter implements ElasticsearchEmitter<Map<String, Object>> {

    private static final long serialVersionUID = 1L;

    private final String index;
    private @Nullable final String documentType;
    private @Nullable final String idFieldName;
    private final boolean isDynamicIndex;
    private transient Function<Map<String, Object>, String> indexProvider;

    public MapElasticsearchEmitter(
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
        if (isDynamicIndex) {
            indexProvider = doc -> doc.get(index).toString();
        } else {
            indexProvider = doc -> index;
        }
    }

    @Override
    public void emit(Map<String, Object> doc, SinkWriter.Context context, RequestIndexer indexer) {
        if (idFieldName != null) {
            final UpdateRequest updateRequest =
                    new UpdateRequest(
                                    indexProvider.apply(doc),
                                    documentType,
                                    doc.get(idFieldName).toString())
                            .doc(doc)
                            .upsert(doc);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                    new IndexRequest(indexProvider.apply(doc), documentType).source(doc);
            indexer.add(indexRequest);
        }
    }
}
