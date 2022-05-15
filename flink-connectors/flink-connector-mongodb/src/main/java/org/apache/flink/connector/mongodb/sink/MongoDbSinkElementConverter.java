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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

/**
 * An implementation of the {@link ElementConverter} that uses the MongoDB driver reactive streams
 * 4.6.x. The user only needs to provide a {@link MongoDbWriteOperationConverter} of the {@code
 * InputT} to transform the input element into a {@link MongoDbWriteOperation}.
 */
@Internal
public class MongoDbSinkElementConverter<InputT>
        implements ElementConverter<InputT, MongoDbWriteModel<? extends WriteModel<Document>>> {

    private final MongoDbWriteOperationConverter<InputT> mongoDbWriteOperationConverter;

    private MongoDbSinkElementConverter(
            MongoDbWriteOperationConverter<InputT> mongoDbWriteOperationConverter) {
        this.mongoDbWriteOperationConverter = mongoDbWriteOperationConverter;
    }

    @Override
    public MongoDbWriteModel<? extends WriteModel<Document>> apply(
            InputT element, SinkWriter.Context context) {
        MongoDbWriteOperation writeOperation = mongoDbWriteOperationConverter.apply(element);
        if (writeOperation instanceof MongoDbInsertOneOperation) {
            MongoDbInsertOneOperation insertOneOperation =
                    (MongoDbInsertOneOperation) writeOperation;
            return new MongoDbInsertOneModel(
                    new InsertOneModel<>(new Document(insertOneOperation.getDocument())));
        } else if (writeOperation instanceof MongoDbReplaceOneOperation) {
            MongoDbReplaceOneOperation replaceOneOperation =
                    (MongoDbReplaceOneOperation) writeOperation;
            return new MongoDbReplaceOneModel(
                    new ReplaceOneModel<>(
                            new Document(replaceOneOperation.getFilter()),
                            new Document(replaceOneOperation.getReplacement()),
                            new ReplaceOptions()
                                    .upsert(replaceOneOperation.getReplaceOptions().isUpsert())));
        }
        throw new IllegalArgumentException("Unsupported MongoDbWriteOperation");
    }

    public static <InputT> Builder<InputT> builder() {
        return new Builder<>();
    }

    /** A builder for the KinesisStreamsSinkElementConverter. */
    public static class Builder<InputT> {

        private MongoDbWriteOperationConverter<InputT> mongoDbWriteOperationConverter;

        public Builder<InputT> setMongoDbWriteOperationConverter(
                MongoDbWriteOperationConverter<InputT> mongoDbWriteOperationConverter) {
            this.mongoDbWriteOperationConverter = mongoDbWriteOperationConverter;
            return this;
        }

        public MongoDbSinkElementConverter<InputT> build() {
            Preconditions.checkNotNull(
                    mongoDbWriteOperationConverter,
                    "No MongoDbWriteOperationConverter was supplied to the "
                            + "MongoDbSinkElementConverter builder.");
            return new MongoDbSinkElementConverter<>(mongoDbWriteOperationConverter);
        }
    }
}
