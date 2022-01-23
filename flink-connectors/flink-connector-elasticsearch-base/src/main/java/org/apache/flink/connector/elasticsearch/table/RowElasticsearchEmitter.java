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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;

import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Sink function for converting upserts into Elasticsearch {@link ActionRequest}s. */
class RowElasticsearchEmitter implements ElasticsearchEmitter<RowData> {

    private final IndexGenerator indexGenerator;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    @Nullable private final String documentType;
    private final Function<RowData, String> createKey;

    public RowElasticsearchEmitter(
            IndexGenerator indexGenerator,
            SerializationSchema<RowData> serializationSchema,
            XContentType contentType,
            @Nullable String documentType,
            Function<RowData, String> createKey) {
        this.indexGenerator = checkNotNull(indexGenerator);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.contentType = checkNotNull(contentType);
        this.documentType = documentType;
        this.createKey = checkNotNull(createKey);
    }

    @Override
    public void open() {
        indexGenerator.open();
    }

    @Override
    public void emit(RowData element, SinkWriter.Context context, RequestIndexer indexer) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                processUpsert(element, indexer);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private void processUpsert(RowData row, RequestIndexer indexer) {
        final byte[] document = serializationSchema.serialize(row);
        final String key = createKey.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                    new UpdateRequest(indexGenerator.generate(row), documentType, key)
                            .doc(document, contentType)
                            .upsert(document, contentType);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                    new IndexRequest(indexGenerator.generate(row), documentType)
                            .id(key)
                            .source(document, contentType);
            indexer.add(indexRequest);
        }
    }

    private void processDelete(RowData row, RequestIndexer indexer) {
        final String key = createKey.apply(row);
        final DeleteRequest deleteRequest =
                new DeleteRequest(indexGenerator.generate(row), documentType, key);
        indexer.add(deleteRequest);
    }
}
