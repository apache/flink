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

package org.apache.flink.connector.mongodb.source.reader.emitter;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.mongodb.source.reader.MongoSourceReader;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplitState;
import org.apache.flink.util.Collector;

import org.bson.BsonDocument;

/**
 * The {@link RecordEmitter} implementation for {@link MongoSourceReader} . We would always update
 * the last consumed message id in this emitter.
 */
public class MongoRecordEmitter<T>
        implements RecordEmitter<BsonDocument, T, MongoSourceSplitState> {

    private final MongoDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper;

    public MongoRecordEmitter(MongoDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        this.sourceOutputWrapper = new SourceOutputWrapper<>();
    }

    @Override
    public void emitRecord(
            BsonDocument document, SourceOutput<T> output, MongoSourceSplitState splitState)
            throws Exception {
        // Sink the record to source output.
        sourceOutputWrapper.setSourceOutput(output);
        deserializationSchema.deserialize(document, sourceOutputWrapper);
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
    }
}
