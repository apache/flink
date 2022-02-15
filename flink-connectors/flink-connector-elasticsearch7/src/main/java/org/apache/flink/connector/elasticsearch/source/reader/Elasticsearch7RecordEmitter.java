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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7SplitState;
import org.apache.flink.util.Collector;

import org.elasticsearch.search.SearchHit;

import java.io.IOException;

/** The {@link RecordEmitter} implementation for {@link Elasticsearch7SourceReader}. */
@Internal
class Elasticsearch7RecordEmitter<T>
        implements RecordEmitter<SearchHit, T, Elasticsearch7SplitState> {

    private final Elasticsearch7SearchHitDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public Elasticsearch7RecordEmitter(
            Elasticsearch7SearchHitDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            SearchHit element, SourceOutput<T> output, Elasticsearch7SplitState splitState)
            throws IOException {
        try {
            sourceOutputWrapper.setSourceOutput(output);
            deserializationSchema.deserialize(element, sourceOutputWrapper);
        } catch (IOException e) {
            throw new IOException("Failed to deserialize SearchHit due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}
    }
}
