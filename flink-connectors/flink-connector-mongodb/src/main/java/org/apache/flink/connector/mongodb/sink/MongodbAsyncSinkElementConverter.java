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

package org.apache.flink.connector.mongodb.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import org.bson.Document;

/**
 * An implementation of the {@link ElementConverter} that uses the Mongodb. The user only needs to
 * provide a {@link DocumentSerializer} of the {@code InputT} to transform the input element into a
 * Document.
 */
@PublicEvolving
public class MongodbAsyncSinkElementConverter<InputT>
        implements ElementConverter<InputT, Document> {
    /** A serialization schema to specify how the input element should be serialized. */
    private final DocumentSerializer<InputT> serializationSchema;

    private MongodbAsyncSinkElementConverter(DocumentSerializer<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    @Experimental
    @Override
    public Document apply(InputT element, SinkWriter.Context context) {
        return serializationSchema.serialize(element);
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the MongodbAsyncSinkElementConverter. */
    @PublicEvolving
    public static class Builder<InputT> {

        private DocumentSerializer<InputT> serializationSchema;

        public Builder<InputT> setSerializationSchema(
                DocumentSerializer<InputT> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        @Experimental
        public MongodbAsyncSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    serializationSchema,
                    "No SerializationSchema was supplied to the "
                            + "KinesisDataStreamsSinkElementConverter builder.");
            return new MongodbAsyncSinkElementConverter<InputT>(
                    (DocumentSerializer<InputT>) serializationSchema);
        }
    }
}
