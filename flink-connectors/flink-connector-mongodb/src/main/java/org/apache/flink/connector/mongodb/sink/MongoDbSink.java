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
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * A MongoDB Sink that performs async requests against a destination collection using the buffering
 * protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link com.mongodb.reactivestreams.client.MongoClient} to
 * communicate with the AWS endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to MongoDB
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the maximum
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to
 *       MongoDB measured in bytes
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink
 *   <li>{@code failOnError}: when an exception is encountered while persisting to MongoDB the job
 *       will fail immediately if failOnError is set
 * </ul>
 *
 * <p>Please see the writer implementation in {@link MongoDbSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class MongoDbSink<InputT>
        extends AsyncSinkBase<InputT, MongoDbWriteModel<? extends WriteModel<Document>>> {

    private final boolean failOnError;
    private final String databaseName;
    private final String collectionName;
    private final Properties mongoProperties;

    MongoDbSink(
            ElementConverter<InputT, MongoDbWriteModel<? extends WriteModel<Document>>>
                    elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            boolean failOnError,
            String databaseName,
            String collectionName,
            Properties mongoProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.databaseName =
                Preconditions.checkNotNull(
                        databaseName,
                        "The database name must not be null when initializing the MongoDB Sink.");
        Preconditions.checkArgument(
                !this.databaseName.isEmpty(),
                "The database name must be set when initializing the MongoDB Sink.");
        this.collectionName =
                Preconditions.checkNotNull(
                        collectionName,
                        "The collection name must not be null when initializing the MongoDB Sink.");
        Preconditions.checkArgument(
                !this.collectionName.isEmpty(),
                "The collection name must be set when initializing the MongoDB Sink.");
        this.failOnError = failOnError;
        this.mongoProperties = mongoProperties;
    }

    /**
     * Create a {@link MongoDbSinkBuilder} to allow the fluent construction of a new {@code
     * MongoDbSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link MongoDbSinkBuilder}
     */
    public static <InputT> MongoDbSinkBuilder<InputT> builder() {
        return new MongoDbSinkBuilder<>();
    }

    @Internal
    @Override
    public StatefulSinkWriter<
                    InputT, BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>>>
            createWriter(InitContext context) throws IOException {
        return new MongoDbSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                Collections.emptyList(),
                failOnError,
                databaseName,
                collectionName,
                mongoProperties);
    }

    @Internal
    @Override
    public SimpleVersionedSerializer<
                    BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>>>
            getWriterStateSerializer() {
        return new MongoDbStateSerializer();
    }

    @Internal
    @Override
    public StatefulSinkWriter<
                    InputT, BufferedRequestState<MongoDbWriteModel<? extends WriteModel<Document>>>>
            restoreWriter(
                    InitContext context,
                    Collection<
                                    BufferedRequestState<
                                            MongoDbWriteModel<? extends WriteModel<Document>>>>
                            recoveredState)
                    throws IOException {
        return new MongoDbSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                recoveredState,
                failOnError,
                databaseName,
                collectionName,
                mongoProperties);
    }
}
